// Copyright 2020 The go-ethereum Authors
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

package eth

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// ethHandler implements the eth.Backend interface to handle the various network
// packets that are sent as replies or broadcasts.
type ethHandler handler

func (h *ethHandler) Chain() *core.BlockChain { return h.chain }
func (h *ethHandler) TxPool() eth.TxPool      { return h.txpool }

// RunPeer is invoked when a peer joins on the `eth` protocol.
func (h *ethHandler) RunPeer(peer *eth.Peer, hand eth.Handler) error {
	return (*handler)(h).runEthPeer(peer, hand)
}

// PeerInfo retrieves all known `eth` information about a peer.
func (h *ethHandler) PeerInfo(id enode.ID) interface{} {
	if p := h.peers.peer(id.String()); p != nil {
		return p.info()
	}
	return nil
}

// AcceptTxs retrieves whether transaction processing is enabled on the node
// or if inbound transactions should simply be dropped.
func (h *ethHandler) AcceptTxs() bool {
	return h.synced.Load()
}

// Handle is invoked from a peer's message handler when it receives a new remote
// message that the handler couldn't consume and serve itself.
func (h *ethHandler) Handle(peer *eth.Peer, packet eth.Packet) error {
	// Consume any broadcasts and announces, forwarding the rest to the downloader
	switch packet := packet.(type) {
	case *eth.NewPooledTransactionHashesPacket:
		err := h.txFetcher.Notify(peer.ID(), packet.Types, packet.Sizes, packet.Hashes)
		if err != nil {
			return err
		}

		// Notify blobfetcher arrival of new type3 transactions
		var hashes []common.Hash
		var hasPayload []bool
		for i, hash := range packet.Hashes {
			if packet.Types[i] == types.BlobTxType {
				hashes = append(hashes, hash)
				hasPayload = append(hasPayload, packet.HasPayloads[i])
			}
		}
		if len(packet.Hashes) == 0 {
			return nil
		}
		return h.blobFetcher.Notify(peer.ID(), packet.Hashes, packet.HasPayloads)

	case *eth.TransactionsPacket:
		var hashes []common.Hash
		var hasPayload []bool
		for i, tx := range packet.Txs {
			if tx.Type() == types.BlobTxType {
				if tx.BlobTxSidecar() != nil {
					return errors.New("disallowed broadcast full-blob transaction")
				}
				hashes = append(hashes, tx.Hash())
				hasPayload = append(hasPayload, packet.HasPayloads[i])
			}
		}

		// Check if the transaction is acceptable to the blobpool (txpool)
		err := h.txFetcher.Enqueue(peer.ID(), packet.Txs, false)
		if err != nil {
			return err
		}

		if len(hashes) == 0 {
			return nil
		}
		// Notify blobfetcher arrival of new type3 transactions
		return h.blobFetcher.Notify(peer.ID(), hashes, hasPayload)

	case *eth.PooledTransactionsResponse:
		// If we receive any blob transactions missing sidecars, or with
		// sidecars that don't correspond to the versioned hashes reported
		// in the header, disconnect from the sending peer.
		for _, tx := range *packet {
			if tx.Type() == types.BlobTxType {
				if tx.BlobTxSidecar() == nil {
					return errors.New("received sidecar-less blob transaction")
				}
				if err := tx.BlobTxSidecar().ValidateBlobCommitmentHashes(tx.BlobHashes()); err != nil {
					return err
				}
			}
		}
		return h.txFetcher.Enqueue(peer.ID(), *packet, true)

	case *eth.Type3PayloadResponse:
		return h.blobFetcher.Enqueue(peer.ID(), packet.Hashes, packet.Sidecars)

	default:
		return fmt.Errorf("unexpected eth packet type: %T", packet)
	}
}
