package node

import (
	"circular/graph"
	"circular/util"
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

// Our channels built from listpeerchannels had last_update 0, so each graph
// refresh pruned the ones listchannels does not return, such as unannounced
// channels.
func TestLocalChannelsSurviveGraphRefresh(t *testing.T) {
	private := &glightning.PeerChannel{
		PeerId:         "02peer",
		ShortChannelId: "5x5x5",
		State:          "CHANNELD_NORMAL",
		Private:        true,
		PeerConnected:  true,
		TotalMsat:      glightning.AmountFromMSat(2000000000),
		ToUsMsat:       glightning.AmountFromMSat(500000000),
	}
	n := &Node{
		Id:        "02self",
		Graph:     graph.NewGraph(),
		PeersLock: &sync.RWMutex{},
		Peers: map[string]*glightning.Peer{
			"02peer": {Id: "02peer", Channels: []*glightning.PeerChannel{private}},
		},
	}
	for _, outgoing := range []bool{true, false} {
		c := n.ConvertPeerChannelToGraphChannel(private, outgoing)
		assert.InDelta(t, time.Now().Unix(), int64(c.LastUpdate), 5)
		n.Graph.AddChannel(c)
		n.Graph.Channels["5x5x5/"+util.GetDirection(c.Source, c.Destination)] = c
	}

	public := &glightning.Channel{Source: "02x", Destination: "02y", ShortChannelId: "6x6x6",
		LastUpdate: uint(time.Now().Unix()), AmountMsat: glightning.AmountFromMSat(1000)}
	gossip := []*glightning.Channel{public}

	assert.Equal(t, 0, n.Graph.SyncChannels(gossip, n.localChannelIds()))
	n.Graph.PruneChannels()
	assert.Len(t, n.Graph.Channels, 3)

	// once the channel is gone from listpeerchannels, it goes from the graph too
	n.Peers = map[string]*glightning.Peer{}
	assert.Equal(t, 2, n.Graph.SyncChannels(gossip, n.localChannelIds()))
	assert.Len(t, n.Graph.Channels, 1)
}

// After a split, the rebalanced channels are read again rather than adjusted
// in the cache: an adjustment counted the payment twice when refreshPeers had
// already seen it, and could wrap the balance around to about 1.8e19 msat.
func TestSetPeerChannelsReplacesThePeer(t *testing.T) {
	before := &glightning.PeerChannel{PeerId: "02peer", ShortChannelId: "7x7x7", State: "CHANNELD_NORMAL",
		TotalMsat: glightning.AmountFromMSat(1000000), ToUsMsat: glightning.AmountFromMSat(1000000)}
	old := &glightning.Peer{Id: "02peer", Connected: true, Channels: []*glightning.PeerChannel{before}}
	n := &Node{
		Id:         "02self",
		Graph:      graph.NewGraph(),
		PeersLock:  &sync.RWMutex{},
		Peers:      map[string]*glightning.Peer{"02peer": old},
		scidToPeer: map[string]*glightning.Peer{"7x7x7": old},
	}

	after := *before
	after.ToUsMsat = glightning.AmountFromMSat(400000)
	closing := &glightning.PeerChannel{PeerId: "02peer", ShortChannelId: "8x8x8", State: "CHANNELD_SHUTTING_DOWN"}
	n.setPeerChannels("02peer", []*glightning.PeerChannel{&after, closing})

	peer := n.Peers["02peer"]
	assert.NotSame(t, old, peer)
	assert.Equal(t, []*glightning.PeerChannel{&after}, peer.Channels)
	assert.True(t, peer.Connected)
	assert.Equal(t, []*glightning.PeerChannel{before}, old.Channels, "the peer read earlier is unchanged")
	assert.Same(t, peer, n.scidToPeer["7x7x7"])
	_, ok := n.scidToPeer["8x8x8"]
	assert.False(t, ok)

	c, err := n.Graph.GetChannel("7x7x7/" + util.GetDirection("02self", "02peer"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(400000), c.Liquidity)

	n.setPeerChannels("02unknown", []*glightning.PeerChannel{&after})
	assert.Len(t, n.Peers, 1)
}
