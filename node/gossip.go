package node

import (
	"bufio"
	"encoding/binary"
	"errors"
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

const (
	gossipStoreHeaderLen      = 12
	gossipStoreDeletedBit     = 0x8000
	gossipStoreCompletedBit   = 0x2000 // set once a record is fully written...
	gossipStoreCompletedSince = 15     // ...from store version 15 (CLN 25.12); 0x2000 meant something else before
	gossipStorePollInterval   = 500 * time.Millisecond
	gossipStoreRetryInterval  = 2 * time.Second
)

// errIncompleteRecord means the reader caught up with gossipd: the record is
// not, or not yet fully, written.
var errIncompleteRecord = errors.New("incomplete gossip_store record")

// StartGossipParser follows lightningd's gossip_store and records the inbound
// fees announced in channel_updates. It keeps running while rebalancing is
// paused with circular-stop, until StopGossipParser is called.
func (n *Node) StartGossipParser(lightningDir string, network string) {
	path := filepath.Join(lightningDir, network, "gossip_store")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		path = filepath.Join(lightningDir, "gossip_store")
	}

	n.Logf(glightning.Info, "Starting gossip store parser for: %s", path)

	for !n.gossipStopped.Load() {
		if err := n.followGossipStore(path); err != nil {
			n.Logf(glightning.Unusual, "gossip_store: %v. Retrying...", err)
			time.Sleep(gossipStoreRetryInterval)
		}
	}
}

// StopGossipParser makes StartGossipParser return.
func (n *Node) StopGossipParser() {
	n.gossipStopped.Store(true)
}

// followGossipStore reads the store from its start, then waits for gossipd to
// append more. It returns nil when the store has been replaced (compaction),
// so the caller reopens it.
func (n *Node) followGossipStore(path string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("parser panic: %v", r)
		}
	}()

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	var version [1]byte
	if _, err := io.ReadFull(file, version[:]); err != nil {
		return fmt.Errorf("reading version: %w", err)
	}
	major, minor := version[0]>>5, version[0]&0x1f
	n.Logf(glightning.Info, "Opened gossip_store: major=%d, minor=%d", major, minor)
	waitForCompleted := major == 0 && minor >= gossipStoreCompletedSince
	inode := fileInode(file)

	reader := bufio.NewReaderSize(file, 64*1024)
	offset := int64(len(version)) // where the next record starts
	header := make([]byte, gossipStoreHeaderLen)
	var body []byte

	for !n.gossipStopped.Load() {
		flags, record, err := readGossipRecord(reader, header, body, waitForCompleted)
		body = record
		if err == errIncompleteRecord {
			// Caught up with gossipd. Wait, then read this record again from its
			// start, discarding the part of it already consumed.
			if storeReplaced(path, inode) {
				n.Logf(glightning.Info, "gossip_store was replaced (compaction detected). Reopening...")
				return nil
			}
			time.Sleep(gossipStorePollInterval)
			if _, err := file.Seek(offset, io.SeekStart); err != nil {
				return err
			}
			reader.Reset(file)
			continue
		}
		if err != nil {
			return err
		}

		offset += int64(gossipStoreHeaderLen + len(record))
		if flags&gossipStoreDeletedBit == 0 && len(record) >= 2 && binary.BigEndian.Uint16(record[0:2]) == 258 {
			n.parseChannelUpdate(record) // channel_update
		}
	}
	return nil
}

// readGossipRecord reads the next record, reusing body's storage. It returns
// errIncompleteRecord when the record is not fully written yet.
func readGossipRecord(reader *bufio.Reader, header, body []byte, waitForCompleted bool) (uint16, []byte, error) {
	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, body, incompleteRecord(err)
	}
	flags := binary.BigEndian.Uint16(header[0:2])
	if waitForCompleted && flags&gossipStoreCompletedBit == 0 {
		return 0, body, errIncompleteRecord
	}

	length := int(binary.BigEndian.Uint16(header[2:4]))
	if cap(body) < length {
		body = make([]byte, length)
	}
	body = body[:length]
	if _, err := io.ReadFull(reader, body); err != nil {
		return 0, body, incompleteRecord(err)
	}
	return flags, body, nil
}

func incompleteRecord(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return errIncompleteRecord
	}
	return err
}

func fileInode(file *os.File) uint64 {
	info, err := file.Stat()
	if err != nil {
		return 0
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(stat.Ino)
	}
	return 0
}

// storeReplaced reports whether path now names a different file than inode.
// While gossipd is renaming the new store into place, path may be missing:
// that is checked again at the next wait.
func storeReplaced(path string, inode uint64) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && uint64(stat.Ino) != inode
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
