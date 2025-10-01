package fetcher

import (
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

func (b *waitBuffer) countEntry(peer string) int {
	return len(b.waitslots[peer])
}

func (b *waitBuffer) idle() bool {
	return len(b.waittime) == 0
}

func (b *waitBuffer) add(peer string, tx common.Hash) {
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
}

func (b *waitBuffer) removable(tx common.Hash) bool {
	return len(b.waitlist[tx]) >= availabilityThreshold
}

func (b *waitBuffer) isWaiting(tx common.Hash) bool {
	return b.waitlist[tx] != nil
}

func (b *waitBuffer) removeTx(tx common.Hash) {
	for peer := range b.waitlist[tx] {
		delete(b.waitslots[peer], tx)
		if len(b.waitslots[peer]) == 0 {
			delete(b.waitslots, peer)
		}
	}
	delete(b.waitlist, tx)
	delete(b.waittime, tx)
}

func (b *waitBuffer) timeout() {
	for hash, instance := range b.waittime {
		if time.Duration(b.clock.Now()-instance)+txGatherSlack > blobAvailabilityTimeout {
			for peer := range b.waitlist[hash] {
				delete(b.waitslots[peer], hash)
				if len(b.waitslots[peer]) == 0 {
					delete(b.waitslots, peer)
				}
			}
			delete(b.waittime, hash)
			delete(b.waitlist, hash)
		}
	}
}

func (b *waitBuffer) dropPeer(peer string) {
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

func (b *waitBuffer) getEarliest() mclock.AbsTime {
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

	return earliest
}
