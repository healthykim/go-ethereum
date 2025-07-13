package fetcher

import (
	"fmt"
	"math/rand"
	mrand "math/rand"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// BlobFetcher fetches payloads for 15% of new Type 3 transactions and performs availability checks for the remaining 85%.

// TODO(healthykim): make timeout configurable
var blobFetchTimeout = 5 * time.Second

type status int

const (
	AvailabilityThreshold = 4
	// TODO(healthykim): should we limit to one blob transaction per fetch?
	maxPayloadRetrievalSize = 128 * 1024
	maxPayloadRetrievals    = 1
	// todo(heatlhykim)
	maxPayloadAnnounces = 4096
)

type blobTxAnnounce struct {
	origin         string        // Identifier of the peer that sent the announcement
	withPayload    []common.Hash // Hashes of transactions announced with payloads
	withoutPayload []common.Hash // Hashes of transactions announced without payloads
}

type payloadRequest struct {
	hashes []common.Hash  // Transactions that have been requested
	time   mclock.AbsTime // Timestamp when the request was made
}

type payloadDelivery struct {
	origin string        // Peer from which the payloads were delivered
	hashes []common.Hash // Hashes of transactions that were delivered
}

// todo(healthykim): add logic for metering
// BlobFetcher is responsible for managing type 3 transactions based on peer announcements.
//
// BlobFetcher manages three buffers:
//   - Transactions not to be fetched are moved to "waitlist"
//     if a paylaoad seems to be possessed by 4(threshold) other peers, accept it. Otherwise, it is dropped
//   - Transactions queued to be fetched are moved to "announces"
//     if a payload is received, it is added to the blob pool. Otherwise, the transaction is dropped.
//   - Transactions to be fetched are moved to "fetching"
//     if a payload announcement is received during fetch, the peer is recorded as an alternate source.
//
// todo buffer renaming
type BlobFetcher struct {
	notify  chan *blobTxAnnounce
	cleanup chan *payloadDelivery
	drop    chan *txDrop
	quit    chan struct{}

	txSeq uint64 // To make transactions fetched in arrival order

	// Buffer 1: Set of blob txs whose blob data is waiting for availability confirmation (not pull decision)
	waitlist  map[common.Hash]map[string]struct{} // Peer set who announced blob availability
	waittime  map[common.Hash]mclock.AbsTime      // Timestamp when added to waitlist
	waitslots map[string]map[common.Hash]struct{} // Waiting announcements grouped by peer (DoS protection)
	// waitSlots should also include the announcements without hasPayload, because such announcements
	// also have to be considered at DoS protection

	// Buffer 2
	// Stage 1: Transactions that have been observed but are still waiting for candidate peer to fetch it from
	seenby   map[string]map[common.Hash]struct{}
	peerwait map[common.Hash]struct{}
	seentime map[common.Hash]mclock.AbsTime // Timestamps when first seen

	// Buffer 2
	// Stage 2: Transactions queued for fetching (pull decision)
	// "announces" is shared with stage 3, for DoS protection
	announces map[string]map[common.Hash]uint64   // Set of announced transactions, grouped by origin peer
	announced map[common.Hash]map[string]struct{} // Peer set who announced blob availability (key: txHash, value: peerId)

	// Buffer 2
	// Stage 3: Transactions currently being fetched (pull decision)
	fetching   map[common.Hash]string              // Transaction set currently being retrieved
	requests   map[string]*payloadRequest          // In-flight transaction retrievals
	alternates map[common.Hash]map[string]struct{} // In-flight transaction alternate origins (in case of the peer dropped)

	// Callbacks
	hasTx         func(common.Hash) bool
	hasPayload    func(common.Hash) bool
	dropTxs       func([]common.Hash) []error
	makeAvailable func([]common.Hash) bool
	addPayloads   func([]common.Hash, []*types.BlobTxSidecar) []error
	fetchPayloads func(string, []common.Hash) error
	dropPeer      func(string)

	// todo(healthykim) add tests
	step     chan struct{}    // Notification channel when the fetcher loop iterates
	clock    mclock.Clock     // Monotonic clock or simulated clock for tests
	realTime func() time.Time // Real system time or simulated time for tests
	rand     *mrand.Rand      // Randomizer to use in tests instead of map range loops (soft-random)
}

func NewBlobFetcher(
	hasTx func(common.Hash) bool, hasPayload func(common.Hash) bool,
	makeAvailable func([]common.Hash) bool, addPayloads func([]common.Hash, []*types.BlobTxSidecar) []error,
	fetchPayloads func(string, []common.Hash) error, dropPeer func(string), clock mclock.Clock, realTime func() time.Time,
	rand *mrand.Rand) *BlobFetcher {
	return &BlobFetcher{
		notify:        make(chan *blobTxAnnounce),
		cleanup:       make(chan *payloadDelivery),
		drop:          make(chan *txDrop),
		quit:          make(chan struct{}),
		waitlist:      make(map[common.Hash]map[string]struct{}),
		waittime:      make(map[common.Hash]mclock.AbsTime),
		waitslots:     make(map[string]map[common.Hash]struct{}),
		seenby:        make(map[string]map[common.Hash]struct{}),
		peerwait:      make(map[common.Hash]struct{}),
		seentime:      make(map[common.Hash]mclock.AbsTime),
		announces:     make(map[string]map[common.Hash]uint64),
		announced:     make(map[common.Hash]map[string]struct{}),
		fetching:      make(map[common.Hash]string),
		requests:      make(map[string]*payloadRequest),
		alternates:    make(map[common.Hash]map[string]struct{}),
		hasTx:         hasTx,
		hasPayload:    hasPayload,
		makeAvailable: makeAvailable,
		addPayloads:   addPayloads,
		fetchPayloads: fetchPayloads,
		dropPeer:      dropPeer,
		clock:         clock,
		realTime:      realTime,
		rand:          rand,
	}
}

// Notify is called when a Type 3 transaction is observed on the network.
func (f *BlobFetcher) Notify(peer string, txs []*types.Transaction) error {
	// Cheap, discarded and duplication check are not required here,
	// as these are handled by tx_fetcher.
	var (
		withoutPayload = make([]common.Hash, 0, len(txs))
		withPayload    = make([]common.Hash, 0, len(txs))
	)
	for _, tx := range txs {
		if tx.Type() != types.BlobTxType || f.hasPayload(tx.Hash()) {
			// Skip non-blob transactions or those already processed
			continue
		}

		if !tx.HasPayload() {
			withoutPayload = append(withoutPayload, tx.Hash())
		} else {
			withPayload = append(withPayload, tx.Hash())
		}
	}

	// If anything's left to announce, push it into the internal loop
	if len(withoutPayload)+len(withPayload) == 0 {
		return nil
	}
	blobAnnounce := &blobTxAnnounce{origin: peer, withPayload: withPayload, withoutPayload: withoutPayload}
	select {
	case f.notify <- blobAnnounce:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// Enqueue inserts a batch of received blob payloads into the blob pool.
// This is triggered by ethHandler upon receiving direct request responses.
// Note: blob payloads are never received via broadcast.
func (f *BlobFetcher) Enqueue(peer string, hashes []common.Hash, payloads []*types.BlobTxSidecar) error {
	//TODO-BS Packet for hashes + payloads

	// We don't need metadata for cleanup because:
	// 1. Size check: Each blob sidecar element has a fixed size.
	//    Misssized data from a peer would be rejected during deserialization.
	// 2. Type check: Blob payloads don't have type.
	// TODO(healthykim): should we validate the sidecar version?
	var added = make([]common.Hash, 0, len(payloads))

	for i := 0; i < len(payloads); i += addTxsBatchSize {
		end := i + addTxsBatchSize
		if end > len(payloads) {
			end = len(payloads)
		}
		hashBatch := hashes[i:end]
		payloadBatch := payloads[i:end]
		// TODO(healthykim): track validation errors if needed
		for j, _ := range f.addPayloads(hashBatch, payloadBatch) {
			added = append(added, hashBatch[j])
		}
	}
	select {
	case f.cleanup <- &payloadDelivery{origin: peer, hashes: added}:
		return nil
	case <-f.quit:
		return errTerminated
	}
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
		waitTimer       = new(mclock.Timer) // Timer for waitlist (availability)
		waitTrigger     = make(chan struct{}, 1)
		peerWaitTimer   = new(mclock.Timer)
		peerWaitTrigger = make(chan struct{}, 1)
		timeoutTimer    = new(mclock.Timer) // Timer for payload fetch request
		timeoutTrigger  = make(chan struct{}, 1)
	)
	for {
		select {
		case ann := <-f.notify:
			//TODO-BS DOS protection
			//TODO-BS announced timeout
			// Drop part of the announcements if too many have accumulated from that peer
			// This prevents a peer from dominating the queue with txs without responding to the request
			// This is why announces is designed to include the transactions that are currerntly being fetched
			used := len(f.waitslots[ann.origin]) + len(f.seenby[ann.origin]) + len(f.announces[ann.origin])
			if used >= maxPayloadAnnounces {
				// Already full
				break
			}

			wantWithPayload := used + len(ann.withPayload)
			wantWithoutPayload := wantWithPayload + len(ann.withoutPayload)
			if wantWithPayload >= maxPayloadAnnounces {
				// drop part of withPayload, drop all of withoutpaylaod
				ann.withPayload = ann.withPayload[:maxPayloadAnnounces-used] // fill the left amount by withPayload
				ann.withoutPayload = ann.withoutPayload[:0]
			} else if wantWithoutPayload >= maxPayloadAnnounces {
				// Enough space for withPayload, drop part of without Payload
				ann.withoutPayload = ann.withoutPayload[:maxPayloadAnnounces-wantWithoutPayload]
			}

			var (
				idleWait   = len(f.waittime) == 0
				_, oldPeer = f.announces[ann.origin] // todo

				// nextSeq returns the next available sequence number for tagging
				nextSeq = func() uint64 {
					seq := f.txSeq
					f.txSeq++
					return seq
				}
			)

			for _, hash := range ann.withPayload {
				if f.waitlist[hash] != nil {
					// Transaction already added to waitlist (decided as not pull)
					// Add the peer to the waitlist
					if _, ok := f.waitlist[hash][ann.origin]; ok {
						continue
					}
					f.waitlist[hash][ann.origin] = struct{}{}
					if waitslots := f.waitslots[ann.origin]; waitslots != nil {
						waitslots[hash] = struct{}{}
					} else {
						f.waitslots[ann.origin] = map[common.Hash]struct{}{
							hash: {},
						}
					}
					continue
				}

				if _, ok := f.peerwait[hash]; ok {
					// Transaction waiting for peer to fetch tx from (decided as pull)
					// Move hash to stage 2
					if f.announced[hash] != nil {
						panic("announce tracker already contains peerwait item")
					}
					f.announced[hash] = map[string]struct{}{ann.origin: {}}
					if f.announces[ann.origin] == nil {
						f.announces[ann.origin] = make(map[common.Hash]uint64)
					}
					f.announces[ann.origin][hash] = nextSeq()

					delete(f.seenby[ann.origin], hash)
					if len(f.seenby[ann.origin]) == 0 {
						delete(f.seenby, ann.origin)
					}
					delete(f.peerwait, hash)
					delete(f.seentime, hash)

					continue
				}

				if f.announced[hash] != nil {
					// Transaction queued for fetching (decided as pull)
					// Add the peer as a candidate source
					// Skip if this peer has already announced this transaction
					if _, ok := f.announced[hash][ann.origin]; ok {
						continue
					}

					f.announced[hash][ann.origin] = struct{}{}
					if announces := f.announces[ann.origin]; announces != nil {
						announces[hash] = nextSeq()
					} else {
						f.announces[ann.origin] = make(map[common.Hash]uint64)
						f.announces[ann.origin][hash] = nextSeq()
					}
					continue
				}

				if f.alternates[hash] != nil {
					// Transaction is being fetched (decided as pull)
					// add the peer as an alternate source and update announces
					f.alternates[hash][ann.origin] = struct{}{}

					if f.announces[ann.origin] == nil {
						f.announces[ann.origin] = make(map[common.Hash]uint64)
					}
					f.announces[ann.origin][hash] = nextSeq()
					// Do not update announced since it's only used to store candidate sources
					// for transactions that are NOT being fetched

					continue
				}

				// For transactions unknown to the blobFetcher:
				// Randomly decide whether to pull (15%) or not pull (85%) the payload
				// TODO-BS Use f.rand
				if rand.New(rand.NewSource(time.Now().UnixNano())).Intn(100) < 15 {
					// New transaction, decided as not pull
					// Add to waitlist with the peer
					f.waitlist[hash] = map[string]struct{}{ann.origin: {}}
					f.waittime[hash] = f.clock.Now()
				} else {
					// New transaction, decided as pull
					// Add to announces queue with the peer
					f.announced[hash] = map[string]struct{}{ann.origin: {}}
					if f.announces[ann.origin] == nil {
						f.announces[ann.origin] = make(map[common.Hash]uint64)
					}
					f.announces[ann.origin][hash] = nextSeq()
				}
			}

			var newPeerWait = false
			// For transactions announced without payload, apply the same random decision process for pull/not-pull
			// and set timer, only if the trasnaction status is not decided yet
			for _, hash := range ann.withoutPayload {
				_, fetching := f.fetching[hash]
				_, waitingpeer := f.peerwait[hash]
				if !fetching || f.waitlist[hash] != nil || !waitingpeer || f.announced[hash] != nil {
					// Skip if transaction status is already decided
					continue
				}
				// For transactions unknown to the blobFetcher,
				// randomly decide whether to pull (15%) or not pull (85%)
				if rand.New(rand.NewSource(time.Now().UnixNano())).Intn(100) < 15 {
					// New transaction & decided as pull -> add to peerwait queue
					f.peerwait[hash] = struct{}{}
					if f.seenby[ann.origin] == nil {
						f.seenby[ann.origin] = make(map[common.Hash]struct{})
					}
					f.seenby[ann.origin][hash] = struct{}{}
					f.seentime[hash] = f.clock.Now()
					newPeerWait = true
				} else {
					// New transaction & decided as not pull -> add to waitlist without any peer (since payload cannot be pulled from the peer)
					f.waitlist[hash] = make(map[string]struct{})
					f.waittime[hash] = f.clock.Now()
				}
			}

			// If a new item was added to the waitlist, schedule its timeout
			if idleWait && len(f.waittime) > 0 {
				f.rescheduleWait(waitTimer, waitTrigger)
			}

			// If a new item was added to the peerwait, schedule its timeout
			if newPeerWait {
				f.reschedulePeerWait(peerWaitTimer, peerWaitTrigger)
			}

			// If this is a new peer and that peer sent transaction with payload flag,
			// schedule transaction fetches from it
			if !oldPeer && len(f.announces[ann.origin]) > 0 {
				f.scheduleFetches(timeoutTimer, timeoutTrigger, map[string]struct{}{ann.origin: {}})
			}

		case <-waitTrigger:
			// At least one transaction's waiting time ran out, pop all expired ones
			// and update the blobpool according to availability
			availableHashes := make([]common.Hash, 0, len(f.waittime))
			dropHashes := make([]common.Hash, 0, len(f.waittime))
			for hash, instance := range f.waittime {
				if time.Duration(f.clock.Now()-instance)+txGatherSlack > txArriveTimeout {
					// Check if enough peers have announced availability
					if len(f.waitlist[hash]) >= AvailabilityThreshold {
						availableHashes = append(availableHashes, hash)
					} else {
						dropHashes = append(dropHashes, hash)
					}
					for peer := range f.waitlist[hash] {
						delete(f.waitslots[peer], hash)
						if len(f.waitslots[peer]) == 0 {
							delete(f.waitslots, peer)
						}
					}
					delete(f.waittime, hash)
					delete(f.waitlist, hash)
				}
			}
			f.makeAvailable(availableHashes)
			f.dropTxs(dropHashes)

			// If transactions are still waiting for availability, reschedule the wait timer
			if len(f.waittime) > 0 {
				f.rescheduleWait(waitTimer, waitTrigger)
			}
		case <-peerWaitTrigger:
			// At least one transaction's peer waiting time ran out, pop all expired ones
			// and update the blobpool
			dropHashes := make([]common.Hash, 0, len(f.seentime))
			for hash, instance := range f.seentime {
				if time.Duration(f.clock.Now()-instance)+txGatherSlack > txArriveTimeout {
					dropHashes = append(dropHashes, hash)
					// clear the maps

					var emptyPeers []string
					for peer, hashes := range f.seenby {
						delete(hashes, hash)
						if len(f.seenby[peer]) == 0 {
							emptyPeers = append(emptyPeers, peer)
						}
					}
					for _, peer := range emptyPeers {
						delete(f.seenby, peer)
					}
					delete(f.seentime, hash)
					delete(f.peerwait, hash)
				}
			}
			f.dropTxs(dropHashes)

			// If transactions are still waiting for availability, reschedule the wait timer
			if len(f.waittime) > 0 {
				f.rescheduleWait(waitTimer, waitTrigger)
			}
		case <-timeoutTrigger:
			// Clean up any expired retrievals and avoid re-requesting them from the
			// same peer (either overloaded or malicious, useless in both cases).
			// Update blobpool according to availability result.
			dropHashes := make([]common.Hash, 0, len(f.requests))
			for peer, req := range f.requests {
				if time.Duration(f.clock.Now()-req.time)+txGatherSlack > txFetchTimeout {
					// Reschedule all the not-yet-delivered fetches to alternate peers
					for _, hash := range req.hashes {
						// Move the delivery back from fetching to queued
						if _, ok := f.announced[hash]; ok {
							panic("announced tracker already contains alternate item")
						}
						// No checking logic for nil (only nil in broadcast)
						f.announced[hash] = f.alternates[hash]

						delete(f.announced[hash], peer)
						if len(f.announced[hash]) == 0 {
							delete(f.announced, hash)
							dropHashes = append(dropHashes, hash)
						}
						delete(f.announces[peer], hash)
						delete(f.alternates, hash)
						delete(f.fetching, hash)
					}
					if len(f.announces[peer]) == 0 {
						delete(f.announces, peer)
					}
					// Keep track of the request as dangling, but never expire
					f.requests[peer].hashes = nil
				}
			}
			f.dropTxs(dropHashes)

			// Schedule a new transaction retrieval
			f.scheduleFetches(timeoutTimer, timeoutTrigger, nil)

			// Trigger timeout for new schedule
			f.rescheduleTimeout(timeoutTimer, timeoutTrigger)
		case delivery := <-f.cleanup:
			// Remove from announce
			for _, hash := range delivery.hashes {
				// Remove from announces, as it is shared by fetching stage
				for peer, txset := range f.announces {
					delete(txset, hash)
					if len(txset) == 0 {
						delete(f.announces, peer)
					}
				}
				// delete(f.announced, hash) // todo(healthykim) Is this possible?
				delete(f.alternates, hash)
				delete(f.fetching, hash)
			}
			// Update mempool status for arrived hashes
			f.makeAvailable(delivery.hashes)

			// Remove the request
			req := f.requests[delivery.origin]
			if req == nil {
				log.Warn("Unexpected transaction delivery", "peer", delivery.origin)
				break
			}

			delete(f.requests, delivery.origin)

			// Reschedule missing transactions in the request
			// Anything not delivered should be re-scheduled (with or without
			// this peer, depending on the response cutoff)
			delivered := make(map[common.Hash]struct{})
			for _, hash := range delivery.hashes {
				delivered[hash] = struct{}{}
			}
			cutoff := len(req.hashes) // If nothing is delivered, assume everything is missing, don't retry!!!
			for i, hash := range req.hashes {
				if _, ok := delivered[hash]; ok {
					cutoff = i
				}
			}
			// Reschedule missing hashes from alternates, not-fulfilled from alt+self
			dropHashes := make([]common.Hash, 0, len(req.hashes))
			for i, hash := range req.hashes {
				if _, ok := delivered[hash]; !ok {
					if i < cutoff {
						// Remove origin from candidate sources for partial responses
						delete(f.alternates[hash], delivery.origin)
						delete(f.announces[delivery.origin], hash)
						if len(f.announces[delivery.origin]) == 0 {
							delete(f.announces, delivery.origin)
						}
					}
					if len(f.alternates[hash]) > 0 {
						// Move back to announces queue if alternates exist
						if _, ok := f.announced[hash]; ok {
							panic(fmt.Sprintf("announced tracker already contains alternate item: %v", f.announced[hash]))
						}
						f.announced[hash] = f.alternates[hash]
					} else {
						// Drop if no alternates are available
						dropHashes = append(dropHashes, hash)
					}
				}
				delete(f.alternates, hash)
				delete(f.fetching, hash)
			}
			f.dropTxs(dropHashes)
			// Something was delivered, try to reschedule requests
			f.scheduleFetches(timeoutTimer, timeoutTrigger, nil) // Partial delivery may enable others to deliver too
		case drop := <-f.drop:
			// A peer was dropped, remove all traces of it
			if _, ok := f.waitslots[drop.peer]; ok {
				var dropHashes = make([]common.Hash, 0, len(f.waitslots[drop.peer]))
				for hash := range f.waitslots[drop.peer] {
					delete(f.waitlist[hash], drop.peer)
					if len(f.waitlist[hash]) == 0 {
						delete(f.waitlist, hash)
						delete(f.waittime, hash)
					}
					dropHashes = append(dropHashes, hash)
				}
				delete(f.waitslots, drop.peer)
				f.dropTxs(dropHashes)
				if len(f.waitlist) > 0 {
					f.rescheduleWait(waitTimer, waitTrigger)
				}
			}
			// Remove hash that put to the peerwait stage by that peer
			if hashes, ok := f.seenby[drop.peer]; ok {
				dropHashes := make([]common.Hash, 0, len(hashes))
				for hash := range hashes {
					delete(f.peerwait, hash)
					delete(f.waittime, hash)
					dropHashes = append(dropHashes, hash)
				}
				delete(f.waitslots, drop.peer)
				f.dropTxs(dropHashes)
			}
			// Clean up general announcement tracking
			if _, ok := f.announces[drop.peer]; ok {
				dropHashes := make([]common.Hash, 0, len(f.announces[drop.peer]))
				for hash := range f.announces[drop.peer] {
					delete(f.announced[hash], drop.peer)
					if len(f.announced[hash]) == 0 {
						delete(f.announced, hash)
						dropHashes = append(dropHashes, hash)
					}
				}
				delete(f.announces, drop.peer)
				f.dropTxs(dropHashes)
			}
			// Clean up any active requests
			var request *payloadRequest
			if request = f.requests[drop.peer]; request != nil {
				for _, hash := range request.hashes {
					// Undelivered hash, reschedule if there's an alternative origin available
					delete(f.alternates[hash], drop.peer)
					if len(f.alternates[hash]) == 0 {
						delete(f.alternates, hash)
					} else {
						f.announced[hash] = f.alternates[hash]
						delete(f.alternates, hash)
					}
					delete(f.fetching, hash)
				}
				delete(f.requests, drop.peer)
				f.dropTxs(request.hashes)
			}
			// If a request was cancelled, check if anything needs to be rescheduled
			if request != nil {
				f.scheduleFetches(timeoutTimer, timeoutTrigger, nil)
				f.rescheduleTimeout(timeoutTimer, timeoutTrigger)
			}
		case <-f.quit:
			return
		}
		// Loop did something, ping the step notifier if needed (tests)
		if f.step != nil {
			f.step <- struct{}{}
		}
	}
}

// For peer waiting pull txs
// todo(healthykim) integrate with reschduleWait?
func (f *BlobFetcher) reschedulePeerWait(timer *mclock.Timer, trigger chan struct{}) {
	if *timer != nil {
		(*timer).Stop()
	}
	now := f.clock.Now()

	earliest := now
	for _, instance := range f.seentime {
		if earliest > instance {
			earliest = instance
			if txArriveTimeout-time.Duration(now-earliest) < txGatherSlack {
				break
			}
		}
	}
	*timer = f.clock.AfterFunc(txArriveTimeout-time.Duration(now-earliest), func() {
		trigger <- struct{}{}
	})
}

func (f *BlobFetcher) rescheduleWait(timer *mclock.Timer, trigger chan struct{}) {
	if *timer != nil {
		(*timer).Stop()
	}
	now := f.clock.Now()

	earliest := now
	for _, instance := range f.waittime {
		if earliest > instance {
			earliest = instance
			if txArriveTimeout-time.Duration(now-earliest) < txGatherSlack {
				break
			}
		}
	}
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
	for _, req := range f.requests {
		// If this request already timed out, skip it altogether
		if req.hashes == nil {
			continue
		}
		if earliest > req.time {
			earliest = req.time
			if txFetchTimeout-time.Duration(now-earliest) < txGatherSlack {
				break
			}
		}
	}
	*timer = f.clock.AfterFunc(txFetchTimeout-time.Duration(now-earliest), func() {
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
		if f.requests[peer] != nil {
			return // continue in the for-each
		}
		if len(f.announces[peer]) == 0 {
			return // continue in the for-each
		}
		var (
			hashes = make([]common.Hash, 0, maxTxRetrievals)
		)
		f.forEachAnnounce(f.announces[peer], func(hash common.Hash) bool {
			// If the transaction is already fetching, skip to the next one
			if _, ok := f.fetching[hash]; ok {
				return true
			}
			// Mark the hash as fetching and stash away possible alternates
			f.fetching[hash] = peer

			if _, ok := f.alternates[hash]; ok {
				panic(fmt.Sprintf("alternate tracker already contains fetching item: %v", f.alternates[hash]))
			}
			f.alternates[hash] = f.announced[hash]
			delete(f.announced, hash)

			// Accumulate the hash and stop if the limit was reached
			hashes = append(hashes, hash)
			return len(hashes) >= maxPayloadRetrievals
		})
		// If any hashes were allocated, request them from the peer
		if len(hashes) > 0 {
			f.requests[peer] = &payloadRequest{hashes: hashes, time: f.clock.Now()}

			go func(peer string, hashes []common.Hash) {
				if err := f.fetchPayloads(peer, hashes); err != nil {
					f.Drop(peer)
				}
			}(peer, hashes)
		}
	})
	// If a new request was fired, schedule a timeout timer
	if idle && len(f.requests) > 0 {
		f.rescheduleTimeout(timer, timeout)
	}
}

// forEachPeer does a range loop over a map of peers in production, but during
// testing it does a deterministic sorted random to allow reproducing issues.
func (f *BlobFetcher) forEachPeer(peers map[string]struct{}, do func(peer string)) {
	// If we're running production, use whatever Go's map gives us
	if f.rand == nil {
		for peer := range peers {
			do(peer)
		}
		return
	}
	// We're running the test suite, make iteration deterministic
	list := make([]string, 0, len(peers))
	for peer := range peers {
		list = append(list, peer)
	}
	sort.Strings(list)
	rotateStrings(list, f.rand.Intn(len(list)))
	for _, peer := range list {
		do(peer)
	}
}

// forEachAnnounce loops over the given announcements in arrival order, invoking
// the do function for each until it returns false. We enforce an arrival
// ordering to minimize the chances of transaction nonce-gaps, which result in
// transactions being rejected by the txpool.
func (f *BlobFetcher) forEachAnnounce(announces map[common.Hash]uint64, do func(hash common.Hash) bool) {
	type announcement struct {
		hash common.Hash
		seq  uint64
	}
	// Process announcements by their arrival order
	list := make([]announcement, 0, len(announces))
	for hash, entry := range announces {
		list = append(list, announcement{hash: hash, seq: entry})
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].seq < list[j].seq
	})
	for i := range list {
		if !do(list[i].hash) {
			return
		}
	}
}
