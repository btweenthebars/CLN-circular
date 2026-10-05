package node

import (
	"circular/graph"
	"encoding/binary"
	"github.com/elementsproject/glightning/glightning"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// update is a channel_update for channel <block>x1x0.
type update struct {
	block     uint32
	direction byte
	disabled  bool
	timestamp uint32
	cltv      uint16
	htlcMin   uint64
	htlcMax   uint64
	baseFee   uint32
	feeRate   uint32
	inbound   *graph.InboundFee // the TLV 55555, if any
}

// record returns the update as a gossip_store record: header and body.
func (u update) record(flags uint16) []byte {
	body := make([]byte, 138)
	binary.BigEndian.PutUint16(body[0:2], 258) // channel_update
	// scid at body[98:106]: 3 bytes block, 3 bytes tx (1), 2 bytes output (0)
	body[98] = byte(u.block >> 16)
	body[99] = byte(u.block >> 8)
	body[100] = byte(u.block)
	body[103] = 1
	binary.BigEndian.PutUint32(body[106:110], u.timestamp)
	body[110] = 1 // message_flags: htlc_maximum_msat present
	body[111] = u.direction
	if u.disabled {
		body[111] |= 2
	}
	binary.BigEndian.PutUint16(body[112:114], u.cltv)
	binary.BigEndian.PutUint64(body[114:122], u.htlcMin)
	binary.BigEndian.PutUint32(body[122:126], u.baseFee)
	binary.BigEndian.PutUint32(body[126:130], u.feeRate)
	binary.BigEndian.PutUint64(body[130:138], u.htlcMax)
	if u.inbound != nil {
		// TLV type 55555 (0xfdd903), length 8
		tlv := []byte{0xfd, 0xd9, 0x03, 8, 0, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(tlv[4:8], uint32(u.inbound.BaseFee))
		binary.BigEndian.PutUint32(tlv[8:12], uint32(u.inbound.FeeRate))
		body = append(body, tlv...)
	}

	header := make([]byte, 12)
	binary.BigEndian.PutUint16(header[0:2], flags)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(body)))
	return append(header, body...)
}

// channelUpdateRecord returns a gossip_store record holding a channel_update
// for channel <block>x1x0, direction 0, with an inbound fee TLV.
func channelUpdateRecord(block uint32, flags uint16, baseFee, feeRate int32) []byte {
	return update{block: block, inbound: &graph.InboundFee{BaseFee: baseFee, FeeRate: feeRate}}.record(flags)
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

// eventually waits up to 3 seconds for condition to hold.
func eventually(n *Node, condition func() bool) bool {
	for i := 0; i < 30; i++ {
		n.Graph.Lock()
		ok := condition()
		n.Graph.Unlock()
		if ok {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// A node that drops its inbound discount used to keep it in circular, whose
// routes then underpaid it and failed with WIRE_FEE_INSUFFICIENT.
func TestGossipParserRemovesInboundFee(t *testing.T) {
	dir, file := newGossipStore(t, 0x0b, channelUpdateRecord(900, 0, -9, -9))
	defer file.Close()
	n := startParser(t, dir)
	assert.NotNil(t, inboundFee(n, "900x1x0/1"))

	// the other direction's update says nothing about this inbound fee
	_, err := file.Write(update{block: 900, direction: 1, timestamp: 2}.record(0))
	assert.NoError(t, err)
	_, err = file.Write(channelUpdateRecord(901, 0, -1, -1))
	assert.NoError(t, err)
	assert.NotNil(t, inboundFee(n, "901x1x0/1"))
	assert.NotNil(t, inboundFee(n, "900x1x0/1"))

	_, err = file.Write(update{block: 900, timestamp: 3}.record(0))
	assert.NoError(t, err)
	assert.True(t, eventually(n, func() bool {
		_, ok := n.Graph.InboundFees["900x1x0/1"]
		return !ok
	}), "the inbound fee was not removed")
}

// Fee changes and disables used to reach the graph only at the next
// listchannels refresh, up to 10 minutes later.
func TestGossipParserAppliesChannelPolicy(t *testing.T) {
	dir, file := newGossipStore(t, 0x0b)
	defer file.Close()
	n := &Node{Graph: graph.NewGraph()}
	old := graph.NewChannel(&glightning.Channel{
		Source:                   "02a",
		Destination:              "02b",
		ShortChannelId:           "950x1x0",
		IsActive:                 true,
		LastUpdate:               100,
		AmountMsat:               glightning.AmountFromMSat(1000000000),
		BaseFeeMillisatoshi:      1000,
		FeePerMillionth:          100,
		Delay:                    40,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(1),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(990000000),
	}, 123456, 7)
	n.Graph.AddChannel(old)
	n.Graph.Channels["950x1x0/0"] = old
	go n.StartGossipParser(dir, "")
	t.Cleanup(n.StopGossipParser)

	newer := update{block: 950, timestamp: 200, disabled: true, cltv: 144,
		htlcMin: 1000, htlcMax: 500000000, baseFee: 0, feeRate: 2500}
	_, err := file.Write(append(update{block: 950, timestamp: 50, feeRate: 1}.record(0), newer.record(0)...))
	assert.NoError(t, err)

	assert.True(t, eventually(n, func() bool { return n.Graph.Channels["950x1x0/0"].LastUpdate == 200 }))
	c, err := n.Graph.GetChannel("950x1x0/0")
	assert.NoError(t, err)
	assert.False(t, c.IsActive)
	assert.Equal(t, uint64(0), c.BaseFeeMillisatoshi)
	assert.Equal(t, uint64(2500), c.FeePerMillionth)
	assert.Equal(t, uint(144), c.Delay)
	assert.Equal(t, uint64(1000), c.HtlcMinimumMilliSatoshis.MSat())
	assert.Equal(t, uint64(500000000), c.HtlcMaximumMilliSatoshis.MSat())
	assert.False(t, c.CanForward(1000), "a disabled channel cannot forward")
	assert.Equal(t, uint64(123456), c.Liquidity, "the liquidity belief is kept")
	assert.Equal(t, uint64(1000000000), c.AmountMsat.MSat())

	// a route priced before the update keeps the policy it was priced with
	assert.Equal(t, uint64(100), old.FeePerMillionth)
	assert.True(t, old.IsActive)

	// listchannels output taken before the update does not undo it
	stale := *old.Channel
	n.Graph.RefreshChannels([]*glightning.Channel{&stale})
	c, _ = n.Graph.GetChannel("950x1x0/0")
	assert.Equal(t, uint64(2500), c.FeePerMillionth)
}
