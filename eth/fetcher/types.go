package fetcher

import (
	"slices"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
)

type blobTxAnnounce struct {
	origin string              // Identifier of the peer that sent the announcement
	txs    []common.Hash       // Hashes of transactions announced
	cells  types.CustodyBitmap // Custody information of transactions being announced
}

type cellRequest struct {
	txs   []common.Hash        // Transactions that have been requested for their cells
	cells *types.CustodyBitmap // Requested cell indices
	time  mclock.AbsTime       // Timestamp when the request was made
}

type payloadDelivery struct {
	origin     string        // Peer from which the payloads were delivered
	txs        []common.Hash // Hashes of transactions that were delivered
	cells      [][]kzg4844.Cell
	cellBitmap *types.CustodyBitmap
}

type cellWithSeq struct {
	seq   uint64
	cells *types.CustodyBitmap
}

type fetchStatus struct {
	fetching *types.CustodyBitmap // To avoid fetching cells which had already been fetched / currently being fetched
	fetched  []uint64             // To sort cells
	cells    []kzg4844.Cell
}

// waitBuffer stores set of blob tx waiting for availability confirmation
type waitBuffer struct {
	waitlist  map[common.Hash]map[string]struct{} // Peer set that announced blob availability
	waittime  map[common.Hash]mclock.AbsTime      // Timestamp when added to waitlist
	waitslots map[string]map[common.Hash]struct{} // Waiting announcements grouped by peer (DoS protection)

	clock mclock.Clock
}

func NewWaitBuffer() *waitBuffer {
	return &waitBuffer{
		waitlist:  make(map[common.Hash]map[string]struct{}),
		waittime:  make(map[common.Hash]mclock.AbsTime),
		waitslots: make(map[string]map[common.Hash]struct{}),
		clock:     mclock.System{},
	}
}

func (b *waitBuffer) countTxs(peer string) int {
	return len(b.waitslots[peer])
}

func (b *waitBuffer) idle() bool {
	return len(b.waittime) == 0
}

func (b *waitBuffer) update(peer string, tx common.Hash) map[string]struct{} {
	if len(b.waitlist[tx]) == 0 {
		// Register for availability check
		b.waitlist[tx] = make(map[string]struct{})
		b.waittime[tx] = b.clock.Now()
	}
	if len(b.waitslots[peer]) == 0 {
		b.waitslots[peer] = make(map[common.Hash]struct{})
	}
	b.waitlist[tx][peer] = struct{}{}
	b.waitslots[peer][tx] = struct{}{}

	if len(b.waitlist[tx]) >= availabilityThreshold {
		blobFetcherWaitTime.Update(b.clock.Now().Sub(b.waittime[tx]).Milliseconds())
		return b.removeTx(tx)
	} else {
		return nil
	}
}

func (b *waitBuffer) has(tx common.Hash) bool {
	return b.waitlist[tx] != nil
}

// combine with removable
func (b *waitBuffer) removeTx(tx common.Hash) map[string]struct{} {
	removedPeers := b.waitlist[tx]
	for peer := range b.waitlist[tx] {
		delete(b.waitslots[peer], tx)
		if len(b.waitslots[peer]) == 0 {
			delete(b.waitslots, peer)
		}
	}
	delete(b.waitlist, tx)
	delete(b.waittime, tx)
	return removedPeers
}

func (b *waitBuffer) timeout() {
	for hash, instance := range b.waittime {
		if time.Duration(b.clock.Now()-instance)+txGatherSlack > blobAvailabilityTimeout {
			b.removeTx(hash)
		}
	}
}

func (b *waitBuffer) removePeer(peer string) {
	if _, ok := b.waitslots[peer]; ok {
		for hash := range b.waitslots[peer] {
			delete(b.waitlist[hash], peer)
			if len(b.waitlist[hash]) == 0 {
				delete(b.waitlist, hash)
				delete(b.waittime, hash)
			}
		}
		delete(b.waitslots, peer)
	}
}

func (b *waitBuffer) getTimeout() time.Duration {
	now := b.clock.Now()
	earliest := b.clock.Now()
	for _, instance := range b.waittime {
		if earliest > instance {
			earliest = instance
			if txArriveTimeout-time.Duration(now-earliest) < txGatherSlack {
				break
			}
		}
	}

	return txArriveTimeout - time.Duration(now-earliest)
}

func (b *waitBuffer) updateMetrics() {
	blobFetcherWaitingPeers.Update(int64(len(b.waitslots)))
	blobFetcherWaitingHashes.Update(int64(len(b.waitlist)))
}

type fetchBuffer struct {
	// Buffer 2: Transactions queued for fetching (pull decision + not pull decision)
	announcetime map[common.Hash]mclock.AbsTime          // Timestamp when added to announces, for metrics
	announces    map[string]map[common.Hash]*cellWithSeq // Set of announced transactions, grouped by origin peer

	// Transactions whose payloads/cells are currently being fetched (pull decision + not pull decision)
	fetches  map[common.Hash]*fetchStatus // Hash -> Bitmap, in-flight transaction cells
	requests map[string][]*cellRequest    // In-flight transaction retrievals
	// todo simplify / remove alterantes
	alternates map[common.Hash]map[string]*types.CustodyBitmap // In-flight transaction alternate origins (in case the peer is dropped)

	clock mclock.Clock
	txSeq uint64 // To make transactions fetched in arrival orde
}

func NewFetchBuffer() *fetchBuffer {
	return &fetchBuffer{
		announcetime: make(map[common.Hash]mclock.AbsTime),
		announces:    make(map[string]map[common.Hash]*cellWithSeq),
		fetches:      make(map[common.Hash]*fetchStatus),
		requests:     make(map[string][]*cellRequest),
		alternates:   make(map[common.Hash]map[string]*types.CustodyBitmap),

		clock: mclock.System{},
	}
}

func (b *fetchBuffer) countTxs(peer string) int {
	return len(b.announces[peer])
}

func (b *fetchBuffer) idle() bool {
	return len(b.requests) == 0
}

func (b *fetchBuffer) addSource(peer string, tx common.Hash, custody *types.CustodyBitmap) {
	if b.announces[peer] != nil && b.announces[peer][tx] != nil {
		return
	}
	if b.announces[peer] == nil {
		b.announces[peer] = make(map[common.Hash]*cellWithSeq)
	}
	b.txSeq++
	b.announces[peer][tx] = &cellWithSeq{
		cells: custody,
		seq:   b.txSeq,
	}
	if _, ok := b.announcetime[tx]; !ok {
		b.announcetime[tx] = b.clock.Now()
	}
}

func (b *fetchBuffer) removeSource(peer string, tx common.Hash) {
	delete(b.announces[peer], tx)
	if len(b.announces[peer]) == 0 {
		delete(b.announces, peer)
	}

	// todo requests and fetches, announcetime etc
	delete(b.alternates[tx], peer)
	if len(b.alternates[tx]) == 0 {
		delete(b.alternates, tx)
		delete(b.announcetime, tx)
		delete(b.fetches, tx)
	}
}

func (b *fetchBuffer) timeout() {
	for peer, requests := range b.requests {
		newRequests := make([]*cellRequest, 0)
		for _, req := range requests {
			if time.Duration(b.clock.Now()-req.time)+txGatherSlack > blobFetchTimeout {
				blobRequestTimeoutMeter.Mark(int64(len(req.txs)))
				// Reschedule all timeout cells to alternate peers
				for _, hash := range req.txs {
					// Do not request the same tx from this peer
					b.removeSource(peer, hash)
					// Allow other candidates to be requested these cells
					if b.fetches[hash] != nil {
						b.fetches[hash].fetching = b.fetches[hash].fetching.Difference(req.cells)
					}
				}
			} else {
				newRequests = append(newRequests, req)
			}
		}
		// remove request
		b.requests[peer] = newRequests
		if len(b.requests[peer]) == 0 {
			delete(b.requests, peer)
		}
	}
}

func (b *fetchBuffer) markDelivery(delivery *payloadDelivery) {
	request := new(cellRequest)
	requestId := 0
	for _, hash := range delivery.txs {
		// Find the request
		for i, req := range b.requests[delivery.origin] {
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
		// cannot find corresponding request (e.g. timeout), ignore
		return
	}

	// update fetch status and announces
	for i, hash := range delivery.txs {
		if !slices.Contains(request.txs, hash) {
			// Unexpected hash, ignore
			continue
		}
		// Update fetch status
		b.fetches[hash].fetched = append(b.fetches[hash].fetched, delivery.cellBitmap.Indices()...)
		b.fetches[hash].cells = append(b.fetches[hash].cells, delivery.cells[i]...)

		// Update announces of this peer
		b.removeSource(delivery.origin, hash)
	}
	// Remove the request
	b.requests[delivery.origin][requestId] = b.requests[delivery.origin][len(b.requests[delivery.origin])-1]
	b.requests[delivery.origin] = b.requests[delivery.origin][:len(b.requests[delivery.origin])-1]
	if len(b.requests[delivery.origin]) == 0 {
		delete(b.requests, delivery.origin)
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
				b.removeSource(delivery.origin, hash)
			}
			// Mark cells deliverable by other peers
			if b.fetches[hash] != nil {
				b.fetches[hash].fetching = b.fetches[hash].fetching.Difference(delivery.cellBitmap)
			}
		}
	}
}

// If all indices are needed, `need` should be set to nil
func (b *fetchBuffer) removeComplete(tx common.Hash, need *types.CustodyBitmap) (common.Hash, []kzg4844.Cell) {
	if b.fetches[tx] == nil {
		// dropped
		return common.Hash{}, nil
	}
	var completed bool
	if need == nil {
		// full fetch
		completed = len(b.fetches[tx].fetched) >= kzg4844.DataPerBlob
	} else {
		fetched := make([]uint64, len(b.fetches[tx].fetched))
		copy(fetched, b.fetches[tx].fetched)
		slices.Sort(fetched)

		completed = slices.Equal(fetched, need.Indices())
	}
	if completed {
		cells := b.fetches[tx].cells
		fetchStatus := b.fetches[tx]
		sort.Slice(fetchStatus.cells, func(i, j int) bool {
			return fetchStatus.fetched[i] < fetchStatus.fetched[j]
		})

		// remove announces from other peers
		for peer, txset := range b.announces {
			delete(txset, tx)
			if len(txset) == 0 {
				delete(b.announces, peer)
			}
		}
		blobFetcherFetchTime.Update(b.clock.Now().Sub(b.announcetime[tx]).Milliseconds())
		delete(b.alternates, tx)
		delete(b.fetches, tx)
		delete(b.announcetime, tx)

		return tx, cells
	}
	return common.Hash{}, nil
}

func (b *fetchBuffer) dropPeer(peer string) {
	// Clean up general announcement tracking
	if _, ok := b.announces[peer]; ok {
		for hash := range b.announces[peer] {
			b.removeSource(peer, hash)
		}
	}

	// Clean up any active requests
	if request, ok := b.requests[peer]; ok && len(request) != 0 {
		for _, req := range request {
			for _, hash := range req.txs {
				// Undelivered hash, reschedule if there's an alternative origin available
				if b.fetches[hash] != nil {
					b.fetches[hash].fetching = b.fetches[hash].fetching.Difference(req.cells)
				}
			}
		}
		delete(b.requests, peer)
	}
}

func (b *fetchBuffer) getIdlePeers() map[string]struct{} {
	actives := make(map[string]struct{})
	for peer := range b.announces {
		if len(b.announces[peer]) == 0 || len(b.requests[peer]) != 0 {
			continue
		}
		actives[peer] = struct{}{}
	}

	return actives
}

func (b *fetchBuffer) getTxs(peer string) []common.Hash {
	txs := make([]common.Hash, 0)
	// Process announcements by their arrival order
	for hash, _ := range b.announces[peer] {
		txs = append(txs, hash)
	}
	sort.Slice(txs, func(i, j int) bool {
		return b.announces[peer][txs[i]].seq < b.announces[peer][txs[j]].seq
	})
	return txs
}

func (b *fetchBuffer) markFetching(peer string, tx common.Hash, max int) *types.CustodyBitmap {
	if len(b.announces[peer]) == 0 || len(b.requests[peer]) != 0 {
		return nil // continue
	}
	var difference *types.CustodyBitmap

	if b.fetches[tx] == nil {
		// tx is not being fetched
		difference = b.announces[peer][tx].cells
	} else {
		difference = b.announces[peer][tx].cells.Difference(b.fetches[tx].fetching)
	}
	difference = difference.Truncate(uint(max))

	if difference.OneCount() != 0 {
		// mark fetch
		if b.fetches[tx] == nil {
			b.fetches[tx] = &fetchStatus{
				fetching: difference,
				fetched:  make([]uint64, 0),
				cells:    make([]kzg4844.Cell, 0),
			}
		} else {
			b.fetches[tx].fetching = b.fetches[tx].fetching.Union(difference)
		}
	}

	// Mark alternatives
	if b.alternates[tx] == nil {
		b.alternates[tx] = map[string]*types.CustodyBitmap{
			peer: b.announces[peer][tx].cells,
		}
	} else {
		b.alternates[tx][peer] = b.announces[peer][tx].cells
	}

	return difference
}

func (b *fetchBuffer) addRequest(peer string, request []*cellRequest) {
	b.requests[peer] = request
}

func (b *fetchBuffer) getTimeout() time.Duration {
	now := b.clock.Now()

	earliest := now
	for _, requests := range b.requests {
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

	return blobFetchTimeout - time.Duration(now-earliest)
}

func (b *fetchBuffer) updateMetrics() {
	blobFetcherQueueingPeers.Update(int64(len(b.announces) - len(b.requests)))
	announced := make(map[common.Hash]struct{})
	for _, hashes := range b.announces {
		for hash := range hashes {
			if _, ok := announced[hash]; !ok {
				announced[hash] = struct{}{}
			}
		}
	}
	blobFetcherQueueingHashes.Update(int64(len(announced)))
	blobFetcherFetchingPeers.Update(int64(len(b.requests)))
	blobFetcherFetchingHashes.Update(int64(len(b.fetches)))
}
