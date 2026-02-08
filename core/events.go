// Copyright 2014 The go-ethereum Authors
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

package core

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// NewTxsEvent is posted when a batch of transactions enter the transaction pool.
type NewTxsEvent struct {
	txs    []*types.Transaction
	hashes []common.Hash
}

func NewTxsEventFromTxs(txs []*types.Transaction) NewTxsEvent {
	return NewTxsEvent{txs: txs}
}

func NewTxsEventFromMetas(hashes []common.Hash) NewTxsEvent {
	return NewTxsEvent{hashes: hashes}
}

// Txs returns the transactions. If transactions are not set, resolve function
// will be used to get transaction. If resolve function is nil and txs field is not set,
// nil will be returned.
func (e NewTxsEvent) Txs(resolve func(common.Hash) *types.Transaction) []*types.Transaction {
	if len(e.txs) > 0 {
		return e.txs
	}
	if resolve == nil {
		return nil
	}
	txs := make([]*types.Transaction, 0, len(e.hashes))
	for _, h := range e.hashes {
		if tx := resolve(h); tx != nil {
			txs = append(txs, tx)
		}
	}
	return txs
}

// Hashes returns the transaction hashes.
func (e NewTxsEvent) Hashes() []common.Hash {
	if len(e.hashes) > 0 {
		return e.hashes
	}
	hashes := make([]common.Hash, len(e.txs))
	for i, tx := range e.txs {
		hashes[i] = tx.Hash()
	}
	return hashes
}

// RemovedLogsEvent is posted when a reorg happens
type RemovedLogsEvent struct{ Logs []*types.Log }

type ChainEvent struct {
	Header       *types.Header
	Receipts     []*types.Receipt
	Transactions []*types.Transaction
}

type ChainHeadEvent struct {
	Header *types.Header
}

// NewPayloadEvent is posted when engine_newPayloadVX processes a block.
type NewPayloadEvent struct {
	Hash           common.Hash
	Number         uint64
	ProcessingTime time.Duration
}
