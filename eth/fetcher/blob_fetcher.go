package fetcher

import (
	"math/rand"
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

	full    map[common.Hash]struct{}
	partial map[common.Hash]struct{}

	// Buffer 1: Set of blob txs whose blob data is waiting for availability confirmation (not pull decision)
	wait *waitBuffer

	// Buffer 2: Transactions being fetched
	fetch *fetchBuffer

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
		fetch:         NewFetchBuffer(),
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
	f.fetch.clock = clock
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
			used := f.wait.countTxs(ann.origin) + f.fetch.countTxs(ann.origin)
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
				reschedule = make(map[string]struct{})
			)
			for _, hash := range ann.txs {
				if ok, new := f.isPartial(hash); ok {
					// Decided to send partial request of the tx
					if new || f.wait.has(hash) {
						if !ann.cells.AllSet() {
							delete(f.partial, hash) // clean up
							continue
						}
						// Transaction is at the stage of availability check
						// Add the peer to the peer list with full availability (waitlist)
						if peers := f.wait.update(ann.origin, hash); len(peers) != 0 {
							for peer := range peers {
								f.fetch.addSource(peer, hash, f.custody)
								reschedule[peer] = struct{}{}
							}
						}
						continue
					}
					if ann.cells.Intersection(f.custody).OneCount() != 0 {
						// Custody overlapping
						f.fetch.addSource(ann.origin, hash, ann.cells.Intersection(f.custody))
						reschedule[ann.origin] = struct{}{}
					}
				} else {
					// 1) Decided to send full request of the tx
					if ann.cells.AllSet() {
						f.fetch.addSource(ann.origin, hash, types.CustodyBitmapData)
						reschedule[ann.origin] = struct{}{}
					}
				}
			}

			// If a new item was added to the waitlist, schedule its timeout
			if idleWait && !f.wait.idle() {
				f.rescheduleWait(waitTimer, waitTrigger)
			}

			// If this is a new peer and that peer sent transaction with payload flag,
			// schedule transaction fetches from it
			//todo use all available peers as data sources
			if len(reschedule) > 0 {
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
			f.fetch.timeout()

			// Schedule a new transaction retrieval
			f.scheduleFetches(timeoutTimer, timeoutTrigger, nil)

			// Trigger timeout for new schedule
			f.rescheduleTimeout(timeoutTimer, timeoutTrigger)
		case delivery := <-f.cleanup:
			addedHashes := make([]common.Hash, 0)
			addedCells := make([][]kzg4844.Cell, 0)

			f.fetch.markDelivery(delivery)
			blobRequestDoneMeter.Mark(int64(len(delivery.txs)))

			// Check completed cells
			for _, tx := range delivery.txs {
				partial, _ := f.isPartial(tx)
				var need *types.CustodyBitmap
				if partial {
					need = f.custody
				}
				hash, cells := f.fetch.removeComplete(tx, need)
				if len(cells) != 0 {
					// complete
					addedHashes = append(addedHashes, hash)
					addedCells = append(addedCells, cells)
				}
			}
			// Update mempool status for arrived hashes
			// Record errors if needed
			if len(addedHashes) > 0 {
				f.addPayload(addedHashes, addedCells, delivery.cellBitmap)
			}
			// Something was delivered, try to reschedule requests
			f.scheduleFetches(timeoutTimer, timeoutTrigger, nil) // Partial delivery may enable others to deliver too
		case drop := <-f.drop:
			// A peer was dropped, remove all traces of it
			f.wait.removePeer(drop.peer)
			if !f.wait.idle() {
				f.rescheduleWait(waitTimer, waitTrigger)
			}
			f.fetch.dropPeer(drop.peer)
			// If a request was cancelled, check if anything needs to be rescheduled
			f.scheduleFetches(timeoutTimer, timeoutTrigger, nil)
			f.rescheduleTimeout(timeoutTimer, timeoutTrigger)

		case <-f.quit:
			return
		}

		f.wait.updateMetrics()
		f.fetch.updateMetrics()

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
	*timer = f.clock.AfterFunc(f.wait.getTimeout(), func() {
		trigger <- struct{}{}
	})
}

// Exactly same as the one in TxFetcher
func (f *BlobFetcher) rescheduleTimeout(timer *mclock.Timer, trigger chan struct{}) {
	if *timer != nil {
		(*timer).Stop()
	}
	*timer = f.clock.AfterFunc(f.fetch.getTimeout(), func() {
		trigger <- struct{}{}
	})
}

func (f *BlobFetcher) scheduleFetches(timer *mclock.Timer, timeout chan struct{}, whitelist map[string]struct{}) {
	// Gather the set of peers we want to retrieve from (default to all)
	actives := whitelist
	if actives == nil {
		actives = f.fetch.getIdlePeers()
	}
	if len(actives) == 0 {
		return
	}
	// For each active peer, try to schedule some payload fetches
	idle := f.fetch.idle()

	f.forEachPeer(actives, func(peer string) {
		var (
			hashes    = make([]common.Hash, 0, maxTxRetrievals)
			custodies = make([]*types.CustodyBitmap, 0, maxTxRetrievals)
		)
		for _, hash := range f.fetch.getTxs(peer) {
			var cells *types.CustodyBitmap
			if _, ok := f.partial[hash]; ok {
				cells = f.fetch.markFetching(peer, hash, maxCellsPerPartialRequest)
			} else {
				cells = f.fetch.markFetching(peer, hash, kzg4844.DataPerBlob)
			}

			// Accumulate the hash and stop if the limit was reached
			if cells != nil && cells.OneCount() != 0 {
				hashes = append(hashes, hash)
				custodies = append(custodies, cells)
			}

			// todo need more fine grained threshold?
			if len(hashes) > maxPayloadRetrievals {
				break
			}
		}
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
			f.fetch.addRequest(peer, request)
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
	if idle && !f.fetch.idle() {
		f.rescheduleTimeout(timer, timeout)
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
