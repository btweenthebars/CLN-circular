package node

import (
	"encoding/binary"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

func parseBigSize(data []byte, offset *int) (uint64, bool) {
	if *offset >= len(data) {
		return 0, false
	}
	b := data[*offset]
	*offset++
	if b < 0xfd {
		return uint64(b), true
	}
	if b == 0xfd {
		if *offset+2 > len(data) {
			return 0, false
		}
		val := binary.BigEndian.Uint16(data[*offset : *offset+2])
		*offset += 2
		return uint64(val), true
	}
	if b == 0xfe {
		if *offset+4 > len(data) {
			return 0, false
		}
		val := binary.BigEndian.Uint32(data[*offset : *offset+4])
		*offset += 4
		return uint64(val), true
	}
	// b == 0xff
	if *offset+8 > len(data) {
		return 0, false
	}
	val := binary.BigEndian.Uint64(data[*offset : *offset+8])
	*offset += 8
	return val, true
}

func (n *Node) StartGossipParser(lightningDir string, network string) {
	path := filepath.Join(lightningDir, network, "gossip_store")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		path = filepath.Join(lightningDir, "gossip_store")
	}

	n.Logf(glightning.Info, "Starting gossip store parser for: %s", path)

	var file *os.File
	var err error

	for {
		if n.Stopped {
			if file != nil {
				file.Close()
			}
			return
		}

		if file == nil {
			file, err = os.Open(path)
			if err != nil {
				n.Logf(glightning.Unusual, "Failed to open gossip_store: %v. Retrying...", err)
				time.Sleep(2 * time.Second)
				continue
			}

			// Read and check version header (1 byte)
			var version [1]byte
			if _, err := io.ReadFull(file, version[:]); err != nil {
				n.Logf(glightning.Unusual, "Failed to read gossip_store version: %v. Retrying...", err)
				file.Close()
				file = nil
				time.Sleep(2 * time.Second)
				continue
			}

			n.Logf(glightning.Info, "Opened gossip_store: major=%d, minor=%d", version[0]>>5, version[0]&0x1f)
		}

		// Get current inode
		fi, err := file.Stat()
		var currentIno uint64
		if err == nil {
			if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
				currentIno = stat.Ino
			}
		}

		// Check if file has been replaced/compacted
		newFi, err := os.Stat(path)
		if err == nil {
			if stat, ok := newFi.Sys().(*syscall.Stat_t); ok {
				if stat.Ino != currentIno {
					n.Logf(glightning.Info, "gossip_store was replaced (compaction detected). Reopening...")
					file.Close()
					file = nil
					continue
				}
			}
		}

		// Try to read header (12 bytes)
		var header [12]byte
		_, err = io.ReadFull(file, header[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Reached end of file, wait for new updates
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err != nil {
			n.Logf(glightning.Unusual, "Error reading gossip_store header: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		flags := binary.BigEndian.Uint16(header[0:2])
		length := binary.BigEndian.Uint16(header[2:4])

		// Try to read body
		body := make([]byte, length)
		_, err = io.ReadFull(file, body)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Partial record at end of file. Rewind header and wait.
			_, _ = file.Seek(-12, io.SeekCurrent)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err != nil {
			n.Logf(glightning.Unusual, "Error reading gossip_store body: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Skip deleted records
		if flags&0x8000 != 0 {
			continue
		}

		if len(body) < 2 {
			continue
		}

		msgType := binary.BigEndian.Uint16(body[0:2])
		if msgType == 258 { // channel_update
			n.parseChannelUpdate(body)
		}
	}
}

func (n *Node) parseChannelUpdate(body []byte) {
	if len(body) < 130 {
		return
	}

	scidBytes := body[98:106]
	block := binary.BigEndian.Uint32(append([]byte{0}, scidBytes[0:3]...))
	tx := binary.BigEndian.Uint32(append([]byte{0}, scidBytes[3:6]...))
	out := binary.BigEndian.Uint16(scidBytes[6:8])
	scidStr := fmt.Sprintf("%dx%dx%d", block, tx, out)

	messageFlags := body[110]
	channelFlags := body[111]

	offset := 130
	if messageFlags&1 != 0 {
		if len(body) < 138 {
			return
		}
		offset = 138
	}

	if len(body) <= offset {
		return // No TLVs
	}

	n.parseTLVs(scidStr, channelFlags, body[offset:])
}

func (n *Node) parseTLVs(scidStr string, channelFlags byte, tlvBytes []byte) {
	offset := 0
	for offset < len(tlvBytes) {
		tType, ok := parseBigSize(tlvBytes, &offset)
		if !ok {
			break
		}
		tLen, ok := parseBigSize(tlvBytes, &offset)
		if !ok {
			break
		}
		if offset+int(tLen) > len(tlvBytes) {
			break
		}
		tVal := tlvBytes[offset : offset+int(tLen)]
		offset += int(tLen)

		if tType == 55555 {
			if len(tVal) == 8 {
				baseFee := int32(binary.BigEndian.Uint32(tVal[0:4]))
				feeRate := int32(binary.BigEndian.Uint32(tVal[4:8]))

				// channelFlags & 1 is the direction of the channel_update.
				direction := channelFlags & 1
				// The inbound fee applies to payments traversing the channel in the opposite direction.
				targetDirection := 1 - direction

				key := fmt.Sprintf("%s/%d", scidStr, targetDirection)
				n.Graph.SetInboundFee(key, baseFee, feeRate)
				n.Logf(glightning.Debug, "Parsed inbound fee for %s: base=%d msat, rate=%d ppm", key, baseFee, feeRate)
			} else {
				n.Logf(glightning.Unusual, "Inbound fee TLV value length invalid: %d bytes", len(tVal))
			}
		}
	}
}
