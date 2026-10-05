package node

import (
	"circular/graph"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"sync"
	"testing"
	"time"
)

// Peer refreshes, per-peer reads after splits and connection events run
// while rebalances read the peers. Run with -race: peers and their channels
// used to be changed in place while other goroutines read them without the
// lock.
func TestConcurrentPeerUse(t *testing.T) {
	n := &Node{Id: "02self", Graph: graph.NewGraph(), PeersLock: &sync.RWMutex{},
		Peers: map[string]*glightning.Peer{}, scidToPeer: map[string]*glightning.Peer{}}
	channels := func(round int) []*glightning.PeerChannel {
		var result []*glightning.PeerChannel
		for p := 0; p < 5; p++ {
			result = append(result, &glightning.PeerChannel{
				PeerId:         fmt.Sprintf("02peer%d", p),
				ShortChannelId: fmt.Sprintf("%dx1x1", p+1),
				State:          "CHANNELD_NORMAL",
				TotalMsat:      glightning.AmountFromMSat(1000000000),
				ToUsMsat:       glightning.AmountFromMSat(uint64(round) * 1000),
				SpendableMsat:  glightning.AmountFromMSat(uint64(round) * 1000),
			})
		}
		return result
	}
	peers := func() []*glightning.Peer {
		var result []*glightning.Peer
		for p := 0; p < 5; p++ {
			result = append(result, &glightning.Peer{Id: fmt.Sprintf("02peer%d", p), Connected: true})
		}
		return result
	}
	n.addLocalChannelsToGraph(n.setPeers(peers(), channels(0), time.Now()))

	const rounds = 200
	var wg sync.WaitGroup
	run := func(f func(i int)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				f(i)
			}
		}()
	}

	run(func(i int) { n.addLocalChannelsToGraph(n.setPeers(peers(), channels(i), time.Now())) })
	run(func(i int) {
		c := channels(i)
		n.setPeerChannels(c[i%5].PeerId, c[i%5:i%5+1], time.Now())
	})
	run(func(i int) {
		e := &glightning.ConnectEvent{PeerId: fmt.Sprintf("02peer%d", i%5)}
		n.OnConnect(e)
		n.OnDisconnect(&glightning.DisconnectEvent{PeerId: e.PeerId})
	})
	run(func(i int) {
		id := fmt.Sprintf("02peer%d", i%5)
		best := n.GetBestPeerChannel(id, func(c *glightning.PeerChannel) uint64 { return c.SpendableMsat.MSat() })
		if best != nil {
			_ = n.IsPeerConnected(best)
			_, _ = n.GetOutgoingChannelFromScid(best.ShortChannelId)
			_, _ = n.GetIncomingChannelFromScid(best.ShortChannelId)
			_, _ = n.GetChannelPeerFromScid(best.ShortChannelId)
		}
		_ = n.HasPeer(id)
		_ = n.PeerCount()
		_ = n.localChannelIds()
	})
	wg.Wait()
}
