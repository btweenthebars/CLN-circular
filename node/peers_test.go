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
	n.setPeerChannels("02peer", []*glightning.PeerChannel{&after, closing}, time.Now())

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

	n.setPeerChannels("02unknown", []*glightning.PeerChannel{&after}, time.Now())
	assert.Len(t, n.Peers, 1)
}

// A peer that left listpeers used to stay in n.Peers with its old channels,
// and could still be picked as a rebalance candidate.
func TestSetPeersForgetsPeersThatAreGone(t *testing.T) {
	gone := &glightning.PeerChannel{PeerId: "02gone", ShortChannelId: "1x1x1", State: "CHANNELD_NORMAL"}
	n := &Node{PeersLock: &sync.RWMutex{}, Peers: map[string]*glightning.Peer{}, scidToPeer: map[string]*glightning.Peer{}}
	n.setPeers([]*glightning.Peer{{Id: "02gone"}}, []*glightning.PeerChannel{gone}, time.Now())
	assert.True(t, n.HasPeer("02gone"))

	open := &glightning.PeerChannel{PeerId: "02stays", ShortChannelId: "2x2x2", State: "CHANNELD_NORMAL", PeerConnected: true}
	closed := &glightning.PeerChannel{PeerId: "02stays", ShortChannelId: "3x3x3", State: "ONCHAIN"}
	n.setPeers([]*glightning.Peer{{Id: "02connected", Connected: true}}, []*glightning.PeerChannel{open, closed}, time.Now())

	assert.False(t, n.HasPeer("02gone"))
	_, err := n.GetChannelPeerFromScid("1x1x1")
	assert.Equal(t, util.ErrNoPeerChannel, err)
	assert.Equal(t, 2, n.PeerCount())
	assert.True(t, n.HasPeer("02connected"), "listpeers peers without channels are kept")
	assert.Equal(t, []*glightning.PeerChannel{open}, n.Peers["02stays"].Channels)
	assert.True(t, n.Peers["02stays"].Connected)
	peer, err := n.GetChannelPeerFromScid("2x2x2")
	assert.NoError(t, err)
	assert.Equal(t, "02stays", peer.Id)
}

// A rebalance reads its channels again after a split. A peer refresh whose
// listpeerchannels ran before that read used to overwrite it with the
// balances from before the split, and the reverse.
func TestPeerReadsApplyInTheOrderTheyWereMade(t *testing.T) {
	balance := func(toUs uint64) []*glightning.PeerChannel {
		return []*glightning.PeerChannel{{PeerId: "02peer", ShortChannelId: "9x9x9", State: "CHANNELD_NORMAL",
			TotalMsat: glightning.AmountFromMSat(1000000), ToUsMsat: glightning.AmountFromMSat(toUs)}}
	}
	toUs := func(n *Node) uint64 {
		n.PeersLock.RLock()
		defer n.PeersLock.RUnlock()
		return n.Peers["02peer"].Channels[0].ToUsMsat.MSat()
	}
	n := &Node{Id: "02self", Graph: graph.NewGraph(), PeersLock: &sync.RWMutex{},
		Peers: map[string]*glightning.Peer{}, scidToPeer: map[string]*glightning.Peer{}}
	t0 := time.Now()
	n.setPeers([]*glightning.Peer{{Id: "02peer"}}, balance(1000000), t0)

	// the peer refresh read its channels at t1, the rebalance at t2 > t1, but
	// the rebalance's read is applied first
	t1, t2 := t0.Add(time.Second), t0.Add(2*time.Second)
	n.setPeerChannels("02peer", balance(600000), t2)
	open := n.setPeers([]*glightning.Peer{{Id: "02peer"}}, balance(1000000), t1)
	assert.Equal(t, uint64(600000), toUs(n), "the newer read is kept")
	assert.Equal(t, uint64(600000), open[0].ToUsMsat.MSat(), "and goes to the graph")
	peer, err := n.GetChannelPeerFromScid("9x9x9")
	assert.NoError(t, err)
	assert.Same(t, n.Peers["02peer"], peer)

	// a read older than the last peer refresh is dropped
	t3 := t0.Add(3 * time.Second)
	n.setPeers([]*glightning.Peer{{Id: "02peer"}}, balance(500000), t3)
	n.setPeerChannels("02peer", balance(700000), t2)
	assert.Equal(t, uint64(500000), toUs(n))
	assert.Empty(t, n.channelsReadAt, "reads older than the refresh are forgotten")
}

// Connect and disconnect events changed the shared peer in place, while
// IsPeerConnected read it without the lock.
func TestConnectEventsReplaceThePeer(t *testing.T) {
	old := &glightning.Peer{Id: "02peer", Channels: []*glightning.PeerChannel{{ShortChannelId: "1x1x1"}}}
	n := &Node{PeersLock: &sync.RWMutex{}, Peers: map[string]*glightning.Peer{"02peer": old},
		scidToPeer: map[string]*glightning.Peer{"1x1x1": old}}

	n.OnConnect(&glightning.ConnectEvent{PeerId: "02peer"})
	assert.False(t, old.Connected)
	assert.True(t, n.Peers["02peer"].Connected)
	assert.Same(t, n.Peers["02peer"], n.scidToPeer["1x1x1"])
	assert.True(t, n.IsPeerConnected(&glightning.PeerChannel{ShortChannelId: "1x1x1"}))

	n.OnDisconnect(&glightning.DisconnectEvent{PeerId: "02peer"})
	assert.False(t, n.IsPeerConnected(&glightning.PeerChannel{ShortChannelId: "1x1x1"}))
	n.OnConnect(&glightning.ConnectEvent{PeerId: "02unknown"})
	assert.Len(t, n.Peers, 1)
}
