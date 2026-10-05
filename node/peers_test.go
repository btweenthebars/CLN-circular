package node

import (
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

// circular-node's metric looks channels up with functions that take PeersLock.
// It used to run with the read lock held, which deadlocks when a writer
// (refreshPeers every 30 s, connect, disconnect) queues in between. A metric
// that takes the write lock itself deadlocks at once under the old code.
func TestGetBestPeerChannelRunsMetricWithoutLock(t *testing.T) {
	low := &glightning.PeerChannel{ShortChannelId: "1x1x1", ToUsMsat: glightning.AmountFromMSat(1000)}
	high := &glightning.PeerChannel{ShortChannelId: "2x1x1", ToUsMsat: glightning.AmountFromMSat(5000)}
	n := &Node{
		PeersLock: &sync.RWMutex{},
		Peers: map[string]*glightning.Peer{
			"02peer": {Id: "02peer", Channels: []*glightning.PeerChannel{low, high}},
		},
	}

	done := make(chan *glightning.PeerChannel)
	go func() {
		done <- n.GetBestPeerChannel("02peer", func(c *glightning.PeerChannel) uint64 {
			n.PeersLock.Lock() // what a waiting writer needs
			n.PeersLock.Unlock()
			return c.ToUsMsat.MSat()
		})
	}()

	select {
	case best := <-done:
		assert.Equal(t, high, best)
	case <-time.After(5 * time.Second):
		t.Fatal("GetBestPeerChannel deadlocked")
	}

	assert.Nil(t, n.GetBestPeerChannel("02unknown", func(*glightning.PeerChannel) uint64 { return 1 }))
}
