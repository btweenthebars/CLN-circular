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

func TestGossipParser(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gossip_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	gossipFilePath := filepath.Join(tmpDir, "gossip_store")

	// 1. Create file and write version header (1 byte)
	f, err := os.Create(gossipFilePath)
	assert.NoError(t, err)

	// Version 11 (0x0b)
	_, err = f.Write([]byte{0x0b})
	assert.NoError(t, err)

	// 2. Construct channel_update record
	// Body bytes
	body := make([]byte, 138+12)
	// Type 258 (channel_update)
	binary.BigEndian.PutUint16(body[0:2], 258)
	// scid bytes at body[98:106]. Let's construct 964x1x0.
	// block=964 (0x0003c4), tx=1 (0x000001), out=0 (0x0000)
	body[98] = 0x00
	body[99] = 0x03
	body[100] = 0xc4
	body[101] = 0x00
	body[102] = 0x00
	body[103] = 0x01
	body[104] = 0x00
	body[105] = 0x00

	// messageFlags = 1 at body[110]
	body[110] = 1
	// channelFlags = 0 at body[111]
	body[111] = 0

	// TLV data at body[138:]
	// Type 55555 (0xfdd903)
	body[138] = 0xfd
	body[139] = 0xd9
	body[140] = 0x03
	// Length 8
	body[141] = 8
	// BaseFee = -200 (0xffffff38)
	binary.BigEndian.PutUint32(body[142:146], 0xffffff38)
	// FeeRate = -100 (0xffffff9c)
	binary.BigEndian.PutUint32(body[146:150], 0xffffff9c)

	// Write record header (12 bytes)
	// flags: 0, length: 150, crc: 0, timestamp: 0
	header := make([]byte, 12)
	binary.BigEndian.PutUint16(header[0:2], 0)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(body)))

	_, err = f.Write(header)
	assert.NoError(t, err)
	_, err = f.Write(body)
	assert.NoError(t, err)
	f.Close()

	// 3. Initialize Node
	n := &Node{
		Graph:   graph.NewGraph(),
		Stopped: false,
	}

	// 4. Start parser in background
	go n.StartGossipParser(tmpDir, "")

	// 5. Wait for it to parse and assert the inbound fee was set correctly
	// Key: scidStr + "/" + (1 - channelFlags & 1)
	// scidStr: 964x1x0. channelFlags: 0. So key: 964x1x0/1
	expectedKey := "964x1x0/1"

	var inboundFee *graph.InboundFee
	for i := 0; i < 20; i++ {
		n.Graph.Lock()
		fee, ok := n.Graph.InboundFees[expectedKey]
		n.Graph.Unlock()
		if ok {
			inboundFee = fee
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.NotNil(t, inboundFee, "Inbound fee was not parsed in time")
	if inboundFee != nil {
		assert.Equal(t, int32(-200), inboundFee.BaseFee)
		assert.Equal(t, int32(-100), inboundFee.FeeRate)
	}

	// 6. Stop the parser
	n.Stopped = true
}
