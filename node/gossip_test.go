package node

import (
	"circular/graph"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// channelUpdateRecord returns a gossip_store record (header and body) holding a
// channel_update for channel <block>x1x0, direction 0, with an inbound fee TLV.
func channelUpdateRecord(block uint32, flags uint16, baseFee, feeRate int32) []byte {
	body := make([]byte, 138+12)
	binary.BigEndian.PutUint16(body[0:2], 258) // channel_update
	// scid at body[98:106]: 3 bytes block, 3 bytes tx (1), 2 bytes output (0)
	body[98] = byte(block >> 16)
	body[99] = byte(block >> 8)
	body[100] = byte(block)
	body[103] = 1
	body[110] = 1 // message_flags: htlc_maximum_msat present
	body[111] = 0 // channel_flags: direction 0
	// TLV type 55555 (0xfdd903), length 8
	body[138] = 0xfd
	body[139] = 0xd9
	body[140] = 0x03
	body[141] = 8
	binary.BigEndian.PutUint32(body[142:146], uint32(baseFee))
	binary.BigEndian.PutUint32(body[146:150], uint32(feeRate))

	header := make([]byte, 12)
	binary.BigEndian.PutUint16(header[0:2], flags)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(body)))
	return append(header, body...)
}

// newGossipStore creates a gossip_store holding the version byte and records.
func newGossipStore(t *testing.T, version byte, records ...[]byte) (dir string, file *os.File) {
	dir = t.TempDir()
	file, err := os.Create(filepath.Join(dir, "gossip_store"))
	assert.NoError(t, err)
	_, err = file.Write([]byte{version})
	assert.NoError(t, err)
	for _, record := range records {
		_, err = file.Write(record)
		assert.NoError(t, err)
	}
	return dir, file
}

func startParser(t *testing.T, dir string) *Node {
	n := &Node{Graph: graph.NewGraph()}
	go n.StartGossipParser(dir, "")
	t.Cleanup(n.StopGossipParser)
	return n
}

// inboundFee waits up to 3 seconds for the parser to record the inbound fee
// of channel <block>x1x0 (the fee applies to direction 1).
func inboundFee(n *Node, key string) *graph.InboundFee {
	for i := 0; i < 30; i++ {
		n.Graph.Lock()
		fee, ok := n.Graph.InboundFees[key]
		n.Graph.Unlock()
		if ok {
			return fee
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func TestGossipParser(t *testing.T) {
	dir, file := newGossipStore(t, 0x0b, channelUpdateRecord(964, 0, -200, -100))
	file.Close()

	n := startParser(t, dir)

	// The inbound fee applies to payments in the opposite direction of the update.
	fee := inboundFee(n, "964x1x0/1")
	if assert.NotNil(t, fee, "Inbound fee was not parsed in time") {
		assert.Equal(t, int32(-200), fee.BaseFee)
		assert.Equal(t, int32(-100), fee.FeeRate)
	}
}

// A record that gossipd is still writing used to leave the parser misaligned
// for good: a partial header was not rewound, and a partial body was rewound
// by the header length only.
func TestGossipParserResumesAfterPartialRecords(t *testing.T) {
	first := channelUpdateRecord(100, 0, -1, -1)
	second := channelUpdateRecord(200, 0, -2, -2)
	third := channelUpdateRecord(300, 0, -3, -3)

	// the store ends in the middle of the first record's body
	dir, file := newGossipStore(t, 0x0b, first[:40])
	defer file.Close()
	n := startParser(t, dir)
	time.Sleep(700 * time.Millisecond)

	// the rest of it, then half of the second record's header
	_, err := file.Write(append(first[40:], second[:5]...))
	assert.NoError(t, err)
	assert.NotNil(t, inboundFee(n, "100x1x0/1"))
	time.Sleep(700 * time.Millisecond)

	_, err = file.Write(append(second[5:], third...))
	assert.NoError(t, err)
	assert.NotNil(t, inboundFee(n, "200x1x0/1"))
	if fee := inboundFee(n, "300x1x0/1"); assert.NotNil(t, fee) {
		assert.Equal(t, int32(-3), fee.BaseFee)
	}
}

// From store version 15, gossipd sets the COMPLETED flag once a record is
// fully written; until then the parser must not read it.
func TestGossipParserWaitsForCompletedFlag(t *testing.T) {
	record := channelUpdateRecord(500, 0, -5, -5)
	dir, file := newGossipStore(t, 0x0f, record)
	defer file.Close()
	n := startParser(t, dir)

	time.Sleep(1200 * time.Millisecond)
	n.Graph.Lock()
	_, parsed := n.Graph.InboundFees["500x1x0/1"]
	n.Graph.Unlock()
	assert.False(t, parsed, "parsed a record without the COMPLETED flag")

	// gossipd marks it complete with a one-byte write of the flags
	_, err := file.WriteAt([]byte{gossipStoreCompletedBit >> 8}, 1)
	assert.NoError(t, err)
	assert.NotNil(t, inboundFee(n, "500x1x0/1"))
}

// circular-stop pauses rebalancing; it used to stop the parser for good.
func TestGossipParserKeepsRunningWhenRebalancingIsStopped(t *testing.T) {
	dir, file := newGossipStore(t, 0x0b, channelUpdateRecord(700, 0, -7, -7))
	defer file.Close()
	n := startParser(t, dir)
	assert.NotNil(t, inboundFee(n, "700x1x0/1"))

	n.Stopped.Store(true) // circular-stop
	time.Sleep(700 * time.Millisecond)
	_, err := file.Write(channelUpdateRecord(800, 0, -8, -8))
	assert.NoError(t, err)
	assert.NotNil(t, inboundFee(n, "800x1x0/1"))
}
