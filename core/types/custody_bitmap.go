package types

import (
	"errors"
	"math/bits"
	"math/rand"

	"github.com/ethereum/go-ethereum/crypto/kzg4844"
)

// `CustodyBitmap` is a bitmap to represent which custody index to store (little endian)
type CustodyBitmap [16]byte

var (
	CustodyBitmapAll = func() *CustodyBitmap {
		var result CustodyBitmap
		for i := 0; i < len(result); i++ {
			result[i] = 0xFF
		}
		return &result
	}()

	CustodyBitmapData = func() *CustodyBitmap {
		var result CustodyBitmap
		for i := 0; i < kzg4844.DataPerBlob/8; i++ {
			result[i] = 0xFF
		}
		return &result
	}()
)

func NewCustodyBitmap(custody []uint64) CustodyBitmap {
	result := CustodyBitmap{}
	err := (&result).SetIndices(custody)
	if err != nil {
		panic("CustodyBitmap: bit index out of range")
	}
	return result
}

// NewRandomCustodyBitmap creates a CustodyBitmap with n randomly selected indices
// This should be used only for test
func NewRandomCustodyBitmap(n int) CustodyBitmap {
	if n <= 0 || n > kzg4844.CellsPerBlob {
		panic("CustodyBitmap: invalid number of indices")
	}

	// Generate random indices without duplicates
	indices := make([]uint64, 0, n)
	used := make(map[uint64]bool)

	for len(indices) < n {
		idx := uint64(rand.Intn(kzg4844.CellsPerBlob))
		if !used[idx] {
			used[idx] = true
			indices = append(indices, idx)
		}
	}

	return NewCustodyBitmap(indices)
}

func (b CustodyBitmap) IsSet(i uint64) bool {
	if i >= uint64(kzg4844.CellsPerBlob) {
		return false
	}
	byteIdx := i / 8
	bitOff := i % 8
	return ((b[byteIdx] >> bitOff) & 1) == 1
}

// Set ith bit
func (b *CustodyBitmap) Set(i uint64) error {
	if i >= uint64(kzg4844.CellsPerBlob) {
		return errors.New("bit index out of range")
	}
	byteIdx := i / 8
	bitOff := i % 8
	b[byteIdx] |= 1 << bitOff
	return nil
}

// Clear ith bit
func (b *CustodyBitmap) Clear(i uint64) error {
	if i >= uint64(kzg4844.CellsPerBlob) {
		return errors.New("bit index out of range")
	}
	byteIdx := i / 8
	bitOff := i % 8
	b[byteIdx] &^= 1 << bitOff
	return nil
}

// Number of bits set to 1
func (b CustodyBitmap) OneCount() int {
	total := 0
	for _, data := range b {
		total += bits.OnesCount8(data)
	}
	return total
}

// Return bit indices set to 1, ascending order
func (b CustodyBitmap) Indices() []uint64 {
	out := make([]uint64, 0, b.OneCount())
	for byteIdx, val := range b {
		v := val
		for v != 0 {
			tz := bits.TrailingZeros8(v) // 0..7
			idx := uint64(byteIdx*8 + tz)
			out = append(out, idx)
			v &^= 1 << tz
		}
	}
	return out
}

func (b *CustodyBitmap) SetIndices(indices []uint64) error {
	for _, i := range indices {
		if i >= uint64(kzg4844.CellsPerBlob) {
			return errors.New("bit index out of range")
		}
		byteIdx := i / 8
		bitOff := i % 8
		b[byteIdx] |= 1 << bitOff
	}
	return nil
}

func (b CustodyBitmap) Same(set *CustodyBitmap) bool {
	for i := 0; i < len(b); i++ {
		if b[i] != set[i] {
			return false
		}
	}
	return true
}

// Difference returns a new CustodyBitmap which is b \ set
func (b *CustodyBitmap) Difference(set *CustodyBitmap) *CustodyBitmap {
	var out CustodyBitmap
	for i := 0; i < len(b); i++ {
		out[i] = b[i] &^ set[i]
	}
	return &out
}

func (b *CustodyBitmap) Intersection(set *CustodyBitmap) *CustodyBitmap {
	var out CustodyBitmap
	for i := 0; i < len(b); i++ {
		out[i] = b[i] & set[i]
	}
	return &out
}

func (b *CustodyBitmap) Union(set *CustodyBitmap) *CustodyBitmap {
	var out CustodyBitmap
	for i := 0; i < len(b); i++ {
		out[i] = b[i] | set[i]
	}
	return &out
}

func (b CustodyBitmap) AllSet() bool {
	totalBits := kzg4844.CellsPerBlob
	fullBytes := totalBits / 8
	remainBits := totalBits % 8

	for i := 0; i < fullBytes; i++ {
		if b[i] != 0xFF {
			return false
		}
	}

	if remainBits > 0 {
		mask := byte((1 << remainBits) - 1)
		if b[fullBytes]&mask != mask {
			return false
		}
	}

	return true
}

func (b *CustodyBitmap) Truncate(len uint) *CustodyBitmap {
	totalBytes := kzg4844.CellsPerBlob / 8

	var out CustodyBitmap
	count := 0
	for byteIdx := 0; byteIdx < totalBytes; byteIdx++ {
		for bitIdx := 0; bitIdx < 8; bitIdx++ {
			if (b[byteIdx] & (1 << bitIdx)) != 0 {
				if count < int(len) {
					out[byteIdx] |= (1 << bitIdx)
					count++
				}
			}
		}
	}
	return &out
}
