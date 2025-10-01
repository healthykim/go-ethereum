package fetcher

import (
	"math/rand"
	"slices"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
)

type random interface {
	Intn(n int) int
}

// todo blob count should be delivered in announce.
const (
	availabilityThreshold     = 2
	maxPayloadRetrievals      = 128
	maxPayloadAnnounces       = 4096
	maxCellsPerPartialRequest = 8
	blobAvailabilityTimeout   = 500 * time.Millisecond
	blobFetchTimeout          = 5 * time.Second
)

// BlobFetcher fetches blobs of new type-3 transactions with probability p,
// and for the remaining (1-p) transactions, it performs availability checks.
// For availability checks, it fetches cells from each blob in the transaction
// according to the custody cell indices provided by the consensus client
// connected to this execution client.
//
// BlobFetcher manages three buffers:
//   - Transactions not to be fetched are moved to "waitlist"
//     if a payload(blob) seems to be possessed by D(threshold) other peers, request custody cells for that.
//     Accept it when the cells are received. Otherwise, it is dropped.
//   - Transactions queued to be fetched are moved to "announces"
//     if a payload is received, it is added to the blob pool. Otherwise, the transaction is dropped.
//   - Transactions to be fetched are moved to "fetching"
//     if a payload/cell announcement is received during fetch, the peer is recorded as an alternate source.
type BlobFetcher struct {
	notify  chan *blobTxAnnounce
	cleanup chan *payloadDelivery
	drop    chan *txDrop
	quit    chan struct{}
	custody *types.CustodyBitmap

	txSeq uint64 // To make transactions fetched in arrival order

	full    map[common.Hash]struct{}
	partial map[common.Hash]struct{}

	// Buffer 1: Set of blob txs whose blob data is waiting for availability confirmation (not pull decision)
	wait *waitBuffer

	// Buffer 2: Transactions queued for fetching (pull decision + not pull decision)
	// "announces" is shared with stage 3, for DoS protection
	announcetime map[common.Hash]mclock.AbsTime          // Timestamp when added to announces, for metrics
	announces    map[string]map[common.Hash]*cellWithSeq // Set of announced transactions, grouped by origin peer

	// Buffer 2
	// Stage 3: Transactions whose payloads/cells are currently being fetched (pull decision + not pull decision)
	fetches  map[common.Hash]*fetchStatus // Hash -> Bitmap, in-flight transaction cells
	requests map[string][]*cellRequest    // In-flight transaction retrievals
	// todo simplify / remove alterantes
	alternates map[common.Hash]map[string]*types.CustodyBitmap // In-flight transaction alternate origins (in case the peer is dropped)

	// Callbacks
	validateCells func([]common.Hash, [][]kzg4844.Cell, *types.CustodyBitmap) []error
	addPayload    func([]common.Hash, [][]kzg4844.Cell, *types.CustodyBitmap) []error
	fetchPayloads func(string, []common.Hash, *types.CustodyBitmap) error
	dropPeer      func(string)

	step     chan struct{}    // Notification channel when the fetcher loop iterates
	clock    mclock.Clock     // Monotonic clock or simulated clock for tests
	realTime func() time.Time // Real system time or simulated time for tests
	rand     random           // Randomizer
}

func NewBlobFetcher(
	validateCells func([]common.Hash, [][]kzg4844.Cell, *types.CustodyBitmap) []error, //
	addPayload func([]common.Hash, [][]kzg4844.Cell, *types.CustodyBitmap) []error, //
	fetchPayloads func(string, []common.Hash, *types.CustodyBitmap) error,
	dropPeer func(string),
	custody *types.CustodyBitmap, rand random) *BlobFetcher {
	return &BlobFetcher{
		notify:        make(chan *blobTxAnnounce),
		cleanup:       make(chan *payloadDelivery),
		drop:          make(chan *txDrop),
		quit:          make(chan struct{}),
		full:          make(map[common.Hash]struct{}),
		partial:       make(map[common.Hash]struct{}),
		wait:          NewWaitBuffer(),
		announcetime:  make(map[common.Hash]mclock.AbsTime),
		announces:     make(map[string]map[common.Hash]*cellWithSeq),
		fetches:       make(map[common.Hash]*fetchStatus),
		requests:      make(map[string][]*cellRequest),
		alternates:    make(map[common.Hash]map[string]*types.CustodyBitmap),
		validateCells: validateCells,
		addPayload:    addPayload,
		fetchPayloads: fetchPayloads,
		dropPeer:      dropPeer,
		custody:       custody,
		clock:         mclock.System{},
		realTime:      time.Now,
		rand:          rand,
	}
}

// todo
func (f *BlobFetcher) setClock(clock mclock.Clock) {
	f.wait.clock = clock
	f.clock = clock
}

// Notify is called when a Type 3 transaction is observed on the network. (TransactionPacket / NewPooledTransactionHashesPacket)
func (f *BlobFetcher) Notify(peer string, txs []common.Hash, cells types.CustodyBitmap) error {
	blobAnnounceInMeter.Mark(int64(len(txs)))

	// Validation regarding tx (e.g. hasTx etc) will be performed in txFetcher
	blobAnnounce := &blobTxAnnounce{origin: peer, txs: txs, cells: cells}
	select {
	case f.notify <- blobAnnounce:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// Enqueue inserts a batch of received blob payloads into the blob pool.
// This is triggered by ethHandler upon receiving direct request responses.
func (f *BlobFetcher) Enqueue(peer string, hashes []common.Hash, cells [][]kzg4844.Cell, cellBitmap types.CustodyBitmap) error {
	var (
		validatedTxs   = make([]common.Hash, 0)
		validatedCells = make([][]kzg4844.Cell, 0)
	)

	blobReplyInMeter.Mark(int64(len(hashes)))

	reject := 0
	for i := 0; i < len(hashes); i += addTxsBatchSize {
		end := i + addTxsBatchSize
		if end > len(hashes) {
			end = len(hashes)
		}
		hashBatch := hashes[i:end]
		cellBatch := cells[i:end]
		for j, err := range f.validateCells(hashBatch, cellBatch, &cellBitmap) {
			if err != nil {
				reject++
			} else {
				validatedTxs = append(validatedTxs, hashBatch[j])
				validatedCells = append(validatedCells, cellBatch[j])
			}
			// Currently we silently drop invalid items and continue processing -> should we disconnect?
		}
	}
	blobReplyRejectMeter.Mark(int64(reject))

	// Process valid data if any exists
	if len(validatedTxs) > 0 {
		select {
		case f.cleanup <- &payloadDelivery{origin: peer, txs: validatedTxs, cells: validatedCells, cellBitmap: &cellBitmap}:
		case <-f.quit:
			return errTerminated
		}
	}

	return nil
}

func (f *BlobFetcher) UpdateCustody(cells *types.CustodyBitmap) {
	f.custody = cells
}

func (f *BlobFetcher) Drop(peer string) error {
	select {
	case f.drop <- &txDrop{peer: peer}:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

func (f *BlobFetcher) Start() {
	go f.loop()
}

func (f *BlobFetcher) Stop() {
	close(f.quit)
}

func (f *BlobFetcher) loop() {
	var (
		waitTimer      = new(mclock.Timer) // Timer for waitlist (availability)
		waitTrigger    = make(chan struct{}, 1)
		timeoutTimer   = new(mclock.Timer) // Timer for payload fetch request
		timeoutTrigger = make(chan struct{}, 1)
	)
	for {
		select {
		case ann := <-f.notify:
			// Drop part of the announcements if too many have accumulated from that peer
			// This prevents a peer from dominating the queue with txs without responding to the request
			used := f.wait.countEntry(ann.origin) + len(f.announces[ann.origin])
			if used >= maxPayloadAnnounces {
				blobAnnounceDOSMeter.Mark(int64(len(ann.txs)))
				// Already full
				break
			}

			want := used + len(ann.txs)
			if want >= maxPayloadAnnounces {
				// drop part of announcements
				ann.txs = ann.txs[:maxPayloadAnnounces-used]
				blobAnnounceDOSMeter.Mark(int64(want - maxPayloadAnnounces))
			}

			var (
				idleWait   = f.wait.idle()
				_, oldPeer = f.announces[ann.origin]
				nextSeq    = func() uint64 {
					seq := f.txSeq
					f.txSeq++
					return seq
				}
				reschedule = make(map[string]struct{})
			)
			for _, hash := range ann.txs {
				if oldPeer && f.announces[ann.origin][hash] != nil {
					// Ignore already announced information
					// We also have to prevent reannouncement by changing cells field.
					// Considering cell custody transition is notified in advance of its finalization by consensus client,
					// there is no reason to reannounce cells, and it has to be prevented.
					continue
				}
				if ok, new := f.isPartial(hash); ok {
					// Decided to send partial request of the tx
					if new || f.wait.isWaiting(hash) {
						if !ann.cells.AllSet() {
							delete(f.partial, hash) // clean up
							continue
						}
						// Transaction is at the stage of availability check
						// Add the peer to the peer list with full availability (waitlist)
						f.wait.add(ann.origin, hash)
						if f.wait.removable(hash) {
							for peer := range f.wait.waitlist[hash] { // todo
								if f.announces[peer] == nil {
									f.announces[peer] = make(map[common.Hash]*cellWithSeq)
								}
								f.announces[peer][hash] = &cellWithSeq{
									cells: f.custody,
									seq:   nextSeq(),
								}
								f.announcetime[hash] = f.clock.Now()
								reschedule[peer] = struct{}{}
							}
							f.wait.removeTx(hash)
							blobFetcherWaitTime.Update(f.clock.Now().Sub(f.wait.waittime[hash]).Milliseconds()) // todo
						}
						continue
					}
					if ann.cells.Intersection(f.custody).OneCount() == 0 {
						// No custody overlapping
						continue
					}

					if f.announces[ann.origin] == nil {
						f.announces[ann.origin] = make(map[common.Hash]*cellWithSeq)
					}
					f.announces[ann.origin][hash] = &cellWithSeq{
						cells: ann.cells.Intersection(f.custody),
						seq:   nextSeq(),
					}
					reschedule[ann.origin] = struct{}{}
				} else {
					// 1) Decided to send full request of the tx
					if !ann.cells.AllSet() {
						continue
					}
					if f.announces[ann.origin] == nil {
						f.announces[ann.origin] = make(map[common.Hash]*cellWithSeq)
					}
					f.announces[ann.origin][hash] = &cellWithSeq{
						cells: types.CustodyBitmapData,
						seq:   nextSeq(),
					}
					f.announcetime[hash] = f.clock.Now()
					reschedule[ann.origin] = struct{}{}
					continue
				}
			}

			// If a new item was added to the waitlist, schedule its timeout
			if idleWait && !f.wait.idle() {
				f.rescheduleWait(waitTimer, waitTrigger)
			}

			// If this is a new peer and that peer sent transaction with payload flag,
			// schedule transaction fetches from it
			//todo i forgot why i added this reschedule list;;;
			if !oldPeer && len(f.announces[ann.origin]) > 0 {
				f.scheduleFetches(timeoutTimer, timeoutTrigger, reschedule)
			}

		case <-waitTrigger:
			// At least one transaction's waiting time ran out, pop all expired ones
			// and update the blobpool according to availability
			// Availability failure case
			f.wait.timeout()
			// If transactions are still waiting for availability, reschedule the wait timer
			if !f.wait.idle() {
				f.rescheduleWait(waitTimer, waitTrigger)
			}

		case <-timeoutTrigger:
			// Clean up any expired retrievals and avoid re-requesting them from the
			// same peer (either overloaded or malicious, useless in both cases).
			// Update blobpool according to availability result.
			for peer, requests := range f.requests {
				newRequests := make([]*cellRequest, 0)
				for _, req := range requests {
					if time.Duration(f.clock.Now()-req.time)+txGatherSlack > blobFetchTimeout {
						blobRequestTimeoutMeter.Mark(int64(len(req.txs)))
						// Reschedule all timeout cells to alternate peers
						for _, hash := range req.txs {
							// Do not request the same tx from this peer
							delete(f.announces[peer], hash)
							delete(f.alternates[hash], peer)
							// Allow other candidates to be requested these cells
							f.fetches[hash].fetching = f.fetches[hash].fetching.Difference(req.cells)

							// Drop cells if there is no alternate source to fetch cells from
							if len(f.alternates[hash]) == 0 {
								delete(f.alternates, hash)
								delete(f.fetches, hash)
								delete(f.announcetime, hash)
							}
						}
						if len(f.announces[peer]) == 0 {
							delete(f.announces, peer)
						}
					} else {
						newRequests = append(newRequests, req)
					}
				}
				// remove request
				f.requests[peer] = newRequests
				if len(f.requests[peer]) == 0 {
					delete(f.requests, peer)
				}
			}

			// Schedule a new transaction retrieval
			f.scheduleFetches(timeoutTimer, timeoutTrigger, nil)

			// Trigger timeout for new schedule
			f.rescheduleTimeout(timeoutTimer, timeoutTrigger)
		case delivery := <-f.cleanup:
			// Remove from announce
			addedHashes := make([]common.Hash, 0)
			addedCells := make([][]kzg4844.Cell, 0)

			var requestId int
			request := new(cellRequest)
			for _, hash := range delivery.txs {
				// Find the request
				for i, req := range f.requests[delivery.origin] {
					if slices.Contains(req.txs, hash) && req.cells.Same(delivery.cellBitmap) {
						request = req
						requestId = i
						break
					}
				}
				if request != nil {
					break
				}
			}
			if request == nil {
				// peer sent cells not requested. ignore
				break
			}
			blobRequestDoneMeter.Mark(int64(len(delivery.txs)))
			for i, hash := range delivery.txs {
				if !slices.Contains(request.txs, hash) {
					// Unexpected hash, ignore
					continue
				}
				// Update fetch status
				f.fetches[hash].fetched = append(f.fetches[hash].fetched, delivery.cellBitmap.Indices()...)
				f.fetches[hash].cells = append(f.fetches[hash].cells, delivery.cells[i]...)

				// Update announces of this peer
				delete(f.announces[delivery.origin], hash)
				if len(f.announces[delivery.origin]) == 0 {
					delete(f.announces, delivery.origin)
				}
				delete(f.alternates[hash], delivery.origin)
				if len(f.alternates[hash]) == 0 {
					delete(f.alternates, hash)
				}

				// Check whether the all required cells are fetched
				completed := false
				if _, ok := f.full[hash]; ok && len(f.fetches[hash].fetched) >= kzg4844.DataPerBlob {
					completed = true
				} else if _, ok := f.partial[hash]; ok {
					fetched := make([]uint64, len(f.fetches[hash].fetched))
					copy(fetched, f.fetches[hash].fetched)
					slices.Sort(fetched)

					custodyIndices := f.custody.Indices()

					completed = slices.Equal(fetched, custodyIndices)
				}

				if completed {
					addedHashes = append(addedHashes, hash)
					fetchStatus := f.fetches[hash]
					sort.Slice(fetchStatus.cells, func(i, j int) bool {
						return fetchStatus.fetched[i] < fetchStatus.fetched[j]
					})
					addedCells = append(addedCells, fetchStatus.cells)

					// remove announces from other peers
					for peer, txset := range f.announces {
						delete(txset, hash)
						if len(txset) == 0 {
							delete(f.announces, peer)
						}
					}
					blobFetcherFetchTime.Update(f.clock.Now().Sub(f.announcetime[hash]).Milliseconds())
					delete(f.alternates, hash)
					delete(f.fetches, hash)
					delete(f.announcetime, hash)
				}
			}
			// Update mempool status for arrived hashes
			// Record errors if needed
			if len(addedHashes) > 0 {
				f.addPayload(addedHashes, addedCells, delivery.cellBitmap)
			}

			// Remove the request
			f.requests[delivery.origin][requestId] = f.requests[delivery.origin][len(f.requests[delivery.origin])-1]
			f.requests[delivery.origin] = f.requests[delivery.origin][:len(f.requests[delivery.origin])-1]
			if len(f.requests[delivery.origin]) == 0 {
				delete(f.requests, delivery.origin)
			}

			// Reschedule missing transactions in the request
			// Anything not delivered should be re-scheduled (with or without
			// this peer, depending on the response cutoff)
			delivered := make(map[common.Hash]struct{})
			for _, hash := range delivery.txs {
				delivered[hash] = struct{}{}
			}
			cutoff := len(request.txs)
			for i, hash := range request.txs {
				if _, ok := delivered[hash]; ok {
					cutoff = i
					continue
				}
			}
			// Reschedule missing hashes from alternates, not-fulfilled from alt+self
			for i, hash := range request.txs {
				if _, ok := delivered[hash]; !ok {
					// Not delivered
					if i < cutoff {
						// Remove origin from candidate sources for partial responses
						delete(f.alternates[hash], delivery.origin)
						delete(f.announces[delivery.origin], hash)
						if len(f.announces[delivery.origin]) == 0 {
							delete(f.announces, delivery.origin)
						}
					}
					// Mark cells deliverable by other peers
					if f.fetches[hash] != nil {
						f.fetches[hash].fetching = f.fetches[hash].fetching.Difference(delivery.cellBitmap)
					}
				}
			}
			// Something was delivered, try to reschedule requests
			f.scheduleFetches(timeoutTimer, timeoutTrigger, nil) // Partial delivery may enable others to deliver too
		case drop := <-f.drop:
			// A peer was dropped, remove all traces of it
			f.wait.dropPeer(drop.peer)
			if len(f.wait.waitlist) > 0 {
				f.rescheduleWait(waitTimer, waitTrigger)
			}
			// Clean up general announcement tracking
			if _, ok := f.announces[drop.peer]; ok {
				for hash := range f.announces[drop.peer] {
					delete(f.alternates[hash], drop.peer)
					if len(f.alternates[hash]) == 0 {
						delete(f.alternates, hash)
					}
				}
				delete(f.announces, drop.peer)
			}
			delete(f.announces, drop.peer)

			// Clean up any active requests
			if request, ok := f.requests[drop.peer]; ok && len(request) != 0 {
				for _, req := range request {
					for _, hash := range req.txs {
						// Undelivered hash, reschedule if there's an alternative origin available
						f.fetches[hash].fetching = f.fetches[hash].fetching.Difference(req.cells)
						delete(f.alternates[hash], drop.peer)
						if len(f.alternates[hash]) == 0 {
							delete(f.alternates, hash)
							delete(f.fetches, hash)
							delete(f.announcetime, hash)
						}
					}
				}
				delete(f.requests, drop.peer)
				// If a request was cancelled, check if anything needs to be rescheduled
				f.scheduleFetches(timeoutTimer, timeoutTrigger, nil)
				f.rescheduleTimeout(timeoutTimer, timeoutTrigger)
			}

		case <-f.quit:
			return
		}

		blobFetcherWaitingPeers.Update(int64(len(f.wait.waitslots))) // todo
		blobFetcherWaitingHashes.Update(int64(len(f.wait.waitlist)))
		blobFetcherQueueingPeers.Update(int64(len(f.announces) - len(f.requests)))
		announced := make(map[common.Hash]struct{})
		for _, hashes := range f.announces {
			for hash := range hashes {
				if _, ok := announced[hash]; !ok {
					announced[hash] = struct{}{}
				}
			}
		}
		blobFetcherQueueingHashes.Update(int64(len(announced)))
		blobFetcherFetchingPeers.Update(int64(len(f.requests)))
		blobFetcherFetchingHashes.Update(int64(len(f.fetches)))

		// Loop did something, ping the step notifier if needed (tests)
		if f.step != nil {
			f.step <- struct{}{}
		}
	}
}

// todo move to buffer?
func (f *BlobFetcher) rescheduleWait(timer *mclock.Timer, trigger chan struct{}) {
	if *timer != nil {
		(*timer).Stop()
	}
	now := f.clock.Now()

	earliest := f.wait.getEarliest()

	*timer = f.clock.AfterFunc(txArriveTimeout-time.Duration(now-earliest), func() {
		trigger <- struct{}{}
	})
}

// Exactly same as the one in TxFetcher
func (f *BlobFetcher) rescheduleTimeout(timer *mclock.Timer, trigger chan struct{}) {
	if *timer != nil {
		(*timer).Stop()
	}
	now := f.clock.Now()

	earliest := now
	for _, requests := range f.requests {
		for _, req := range requests {
			// If this request already timed out, skip it altogether
			if req.txs == nil {
				continue
			}
			if earliest > req.time {
				earliest = req.time
				if blobFetchTimeout-time.Duration(now-earliest) < txGatherSlack {
					break
				}
			}
		}
	}
	*timer = f.clock.AfterFunc(blobFetchTimeout-time.Duration(now-earliest), func() {
		trigger <- struct{}{}
	})
}
func (f *BlobFetcher) scheduleFetches(timer *mclock.Timer, timeout chan struct{}, whitelist map[string]struct{}) {
	// Gather the set of peers we want to retrieve from (default to all)
	actives := whitelist
	if actives == nil {
		actives = make(map[string]struct{})
		for peer := range f.announces {
			actives[peer] = struct{}{}
		}
	}
	if len(actives) == 0 {
		return
	}
	// For each active peer, try to schedule some payload fetches
	idle := len(f.requests) == 0

	f.forEachPeer(actives, func(peer string) {
		if len(f.announces[peer]) == 0 || len(f.requests[peer]) != 0 {
			return // continue
		}
		var (
			hashes    = make([]common.Hash, 0, maxTxRetrievals)
			custodies = make([]*types.CustodyBitmap, 0, maxTxRetrievals)
		)
		f.forEachAnnounce(f.announces[peer], func(hash common.Hash, cells *types.CustodyBitmap) bool {
			var difference *types.CustodyBitmap

			if f.fetches[hash] == nil {
				// tx is not being fetched
				difference = cells
			} else {
				difference = cells.Difference(f.fetches[hash].fetching)
			}
			if _, ok := f.partial[hash]; ok {
				difference = difference.Truncate(maxCellsPerPartialRequest)
			}

			// Mark fetching for differences
			if difference.OneCount() != 0 {
				if f.fetches[hash] == nil {
					f.fetches[hash] = &fetchStatus{
						fetching: difference,
						fetched:  make([]uint64, 0),
						cells:    make([]kzg4844.Cell, 0),
					}
				} else {
					f.fetches[hash].fetching = f.fetches[hash].fetching.Union(difference)
				}
				// Accumulate the hash and stop if the limit was reached
				hashes = append(hashes, hash)
				custodies = append(custodies, difference)
			}

			// Mark alternatives
			if f.alternates[hash] == nil {
				f.alternates[hash] = map[string]*types.CustodyBitmap{
					peer: cells,
				}
			} else {
				f.alternates[hash][peer] = cells
			}

			// todo need different threshold
			return len(hashes) < maxPayloadRetrievals
		})
		// If any hashes were allocated, request them from the peer
		if len(hashes) > 0 {
			blobRequestOutMeter.Mark(int64(len(hashes)))
			// Group hashes by custody bitmap
			requestByCustody := make(map[string]*cellRequest)

			for i, hash := range hashes {
				custody := custodies[i]

				key := string(custody[:])

				if _, ok := requestByCustody[key]; !ok {
					requestByCustody[key] = &cellRequest{
						txs:   []common.Hash{},
						cells: custody,
						time:  f.clock.Now(),
					}
				}
				requestByCustody[key].txs = append(requestByCustody[key].txs, hash)
			}
			// construct request
			var request []*cellRequest
			for _, cr := range requestByCustody {
				request = append(request, cr)
			}
			f.requests[peer] = request
			go func(peer string, request []*cellRequest) {
				for _, req := range request {
					if err := f.fetchPayloads(peer, req.txs, req.cells); err != nil {
						blobRequestFailMeter.Mark(int64(len(hashes)))
						f.Drop(peer)
						break
					}
				}
			}(peer, request)
		}
	})
	// If a new request was fired, schedule a timeout timer
	if idle && len(f.requests) > 0 {
		f.rescheduleTimeout(timer, timeout)
	}
}

// forEachAnnounce loops over the given announcements in arrival order, invoking
// the do function for each until it returns false. We enforce an arrival
// ordering to minimize the chances of transaction nonce-gaps, which result in
// transactions being rejected by the txpool.
func (f *BlobFetcher) forEachAnnounce(announces map[common.Hash]*cellWithSeq, do func(hash common.Hash, cells *types.CustodyBitmap) bool) {
	type announcement struct {
		hash  common.Hash
		cells *types.CustodyBitmap
		seq   uint64
	}
	// Process announcements by their arrival order
	list := make([]announcement, 0, len(announces))
	for hash, entry := range announces {
		list = append(list, announcement{hash: hash, cells: entry.cells, seq: entry.seq})
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].seq < list[j].seq
	})
	for i := range list {
		if !do(list[i].hash, list[i].cells) {
			return
		}
	}
}

// forEachPeer does a range loop over a map of peers in production, but during
// testing it does a deterministic sorted random to allow reproducing issues.
func (f *BlobFetcher) forEachPeer(peers map[string]struct{}, do func(peer string)) {
	// If we're running production(step == nil), use whatever Go's map gives us
	if f.step == nil {
		for peer := range peers {
			do(peer)
		}
		return
	}
	// We're running the test suite, make iteration sorted by peer id
	list := make([]string, 0, len(peers))
	for peer := range peers {
		list = append(list, peer)
	}
	sort.Strings(list)
	for _, peer := range list {
		do(peer)
	}
}

func (f *BlobFetcher) isPartial(tx common.Hash) (bool, bool) {
	new := false
	if _, ok := f.full[tx]; !ok {
		if _, ok := f.partial[tx]; !ok {
			// Not decided yet
			new = true
			var randomValue int
			if f.rand == nil {
				randomValue = rand.Intn(100)
			} else {
				randomValue = f.rand.Intn(100)
			}
			if randomValue < 15 {
				f.full[tx] = struct{}{}
			} else {
				f.partial[tx] = struct{}{}
			}
		}
	}

	_, ok := f.partial[tx]
	return ok, new
}
