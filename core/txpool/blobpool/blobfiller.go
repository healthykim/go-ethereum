// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package blobpool

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
)

const (
	fillInterval = time.Second
	fillTimeout  = 30 * time.Second
)

// BlobFiller watches the blob pool and, while the blocked suffix exceeds its
// cap, asks the network layer to fetch the missing cells of the head partial
// transaction of each blocked account so it can become includable. Requests
// that fail to relieve the pressure within fillTimeout are dropped.
type BlobFiller struct {
	pool        *BlobPool
	clock       mclock.Clock
	requestFill func(common.Hash)

	requested map[common.Hash]mclock.AbsTime // Request time of each tx hashes

	quit chan struct{}
	wg   sync.WaitGroup
	step func()
}

// NewBlobFiller creates a filler backed by the given blob pool. requestFill is
// called to fetch the missing cells of a blocking partial transaction.
func NewBlobFiller(pool *BlobPool, requestFill func(common.Hash)) *BlobFiller {
	return newBlobFiller(pool, requestFill, mclock.System{}, nil)
}

func newBlobFiller(pool *BlobPool, requestFill func(common.Hash), clock mclock.Clock, step func()) *BlobFiller {
	if requestFill == nil {
		requestFill = func(common.Hash) {}
	}
	f := &BlobFiller{
		pool:        pool,
		clock:       clock,
		requestFill: requestFill,
		requested:   make(map[common.Hash]mclock.AbsTime),
		quit:        make(chan struct{}),
		step:        step,
	}
	f.wg.Add(1)
	go f.loop()
	return f
}

func (f *BlobFiller) Stop() {
	close(f.quit)
	f.wg.Wait()
}

func (f *BlobFiller) loop() {
	defer f.wg.Done()

	timer := f.clock.NewTimer(fillInterval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C():
			f.tick()
			timer.Reset(fillInterval)
		case <-f.quit:
			return
		}
		if f.step != nil {
			f.step()
		}
	}
}

// tick reconciles the outstanding fill requests against the pool's current
// blocked heads: it forgets requests that no longer block, drops the ones that
// timed out, and issues fresh requests for the rest.
func (f *BlobFiller) tick() {
	heads := f.pool.blocking()
	if len(heads) == 0 {
		clear(f.requested)
		return
	}
	// Forget requests whose transaction no longer blocks: it was either filled
	// and unblocked, or dropped elsewhere.
	for hash := range f.requested {
		if _, ok := heads[hash]; !ok {
			delete(f.requested, hash)
		}
	}
	now := f.clock.Now()
	for hash, addr := range heads {
		switch at, ok := f.requested[hash]; {
		case !ok:
			f.requested[hash] = now
			f.requestFill(hash)
		case time.Duration(now-at) > fillTimeout:
			f.pool.dropBlocked(hash, addr)
			delete(f.requested, hash)
		}
	}
}
