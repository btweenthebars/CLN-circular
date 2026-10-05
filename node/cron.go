package node

import (
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/robfig/cron/v3"
	"log"
	"strconv"
	"strings"
	"time"
)

const (
	LIQUIDITY_REFRESH_INTERVAL = 10 // minutes
)

func (n *Node) setupCronJobs(options map[string]glightning.Option) {
	c := cron.New()

	// every 10 minutes by default, refresh the information gathered via gossip
	addCronJob(c, strconv.Itoa(options["circular-graph-refresh"].GetValue().(int))+"m", func() {
		n.refreshGraph()
	})

	// every 30 seconds by default, refresh peers
	addCronJob(c, strconv.Itoa(options["circular-peer-refresh"].GetValue().(int))+"s", func() {
		n.refreshPeers()
	})

	// every 10 minutes by default, check if there are channels that need to be reset
	addCronJob(c, strconv.Itoa(LIQUIDITY_REFRESH_INTERVAL)+"m", func() {
		n.refreshLiquidity()
	})

	c.Start()
}

func addCronJob(c *cron.Cron, interval string, f func()) {
	_, err := c.AddFunc("@every "+interval, f)
	if err != nil {
		log.Fatalln("error adding cron job", err)
	}
}

func (n *Node) refreshGraph() error {
	defer util.TimeTrack(time.Now(), "node.refreshGraph", n.Logf)
	n.Logln(glightning.Info, "refreshing graph")

	asOf := uint(time.Now().Unix())
	channelList, err := n.lightning.ListChannels()
	if err != nil {
		// glightning returns an error ("No channel found for short channel id ") when 0 channels exist in gossip
		if strings.Contains(err.Error(), "No channel found") {
			channelList = []*glightning.Channel{}
		} else {
			n.Logf(glightning.Unusual, "error listing channels: %+v", err)
			return err
		}
	}

	n.Logln(glightning.Debug, "refreshing channels")
	if removed := n.Graph.SyncChannels(channelList, n.localChannelIds(), asOf); removed < 0 {
		n.Logf(glightning.Unusual, "listchannels returned only %d channels: not removing the channels it lacks", len(channelList))
	} else if removed > 0 {
		n.Logf(glightning.Info, "removed %d channels that are no longer in gossip", removed)
	}

	n.Logln(glightning.Debug, "pruning channels")
	n.Graph.PruneChannels()

	n.Logln(glightning.Debug, "refreshing aliases")
	nodes, err := n.lightning.ListNodes()
	if err != nil {
		n.Logf(glightning.Unusual, "error listing nodes: %+v", err)
	} else {
		n.Graph.RefreshAliases(nodes)
	}

	n.Logln(glightning.Debug, "saving graph to file")
	if err = n.SaveGraphToFile(CIRCULAR_DIR, "graph.json"); err != nil {
		n.Logf(glightning.Unusual, "error saving graph to file: %+v", err)
	}

	n.Logln(glightning.Info, "graph has been refreshed")
	return nil
}

// localChannelIds returns the ids of both directions of our open channels.
func (n *Node) localChannelIds() map[string]bool {
	n.PeersLock.RLock()
	defer n.PeersLock.RUnlock()

	ids := make(map[string]bool)
	for _, peer := range n.Peers {
		for _, channel := range peer.Channels {
			if channel.ShortChannelId == "" {
				continue
			}
			ids[channel.ShortChannelId+"/"+util.GetDirection(n.Id, peer.Id)] = true
			ids[channel.ShortChannelId+"/"+util.GetDirection(peer.Id, n.Id)] = true
		}
	}
	return ids
}

func (n *Node) refreshPeers() error {
	defer util.TimeTrack(time.Now(), "node.refreshPeers", n.Logf)
	n.Logln(glightning.Debug, "refreshing peers")

	peers, err := n.lightning.ListPeers()
	if err != nil {
		n.Logln(glightning.Unusual, err)
		return err
	}

	readAt := time.Now()
	channelsResp, err := n.lightning.ListPeerChannels("")
	if err != nil {
		n.Logln(glightning.Unusual, err)
		return err
	}

	n.addLocalChannelsToGraph(n.setPeers(peers, channelsResp.Channels, readAt))
	return nil
}

// setPeers replaces the peers with those in listpeers and the peers of the
// open channels in listpeerchannels, read at readAt. The map is built afresh:
// a peer gone from both used to stay, with its old channels, and could still
// be picked as a rebalance candidate. A peer whose channels were read again
// after readAt, after a rebalance, keeps those. It returns the open channels.
func (n *Node) setPeers(peers []*glightning.Peer, channels []*glightning.PeerChannel, readAt time.Time) []*glightning.PeerChannel {
	fresh := make(map[string]*glightning.Peer, len(peers))
	for _, peer := range peers {
		peer.Channels = make([]*glightning.PeerChannel, 0)
		fresh[peer.Id] = peer
	}

	for _, channel := range channels {
		if channel.State != "CHANNELD_NORMAL" {
			continue
		}
		peer, ok := fresh[channel.PeerId]
		if !ok {
			peer = &glightning.Peer{
				Id:        channel.PeerId,
				Connected: channel.PeerConnected,
				Channels:  make([]*glightning.PeerChannel, 0),
			}
			fresh[channel.PeerId] = peer
		}
		peer.Channels = append(peer.Channels, channel)
	}

	n.PeersLock.Lock()
	defer n.PeersLock.Unlock()

	for id, at := range n.channelsReadAt {
		if !at.After(readAt) {
			delete(n.channelsReadAt, id)
			continue
		}
		old, ok := n.Peers[id]
		if !ok || old == nil {
			continue
		}
		if peer, ok := fresh[id]; ok {
			peer.Channels = old.Channels
		} else {
			fresh[id] = old
		}
	}

	scidToPeer := make(map[string]*glightning.Peer, len(channels))
	open := make([]*glightning.PeerChannel, 0, len(channels))
	for _, peer := range fresh {
		for _, channel := range peer.Channels {
			open = append(open, channel)
			if channel.ShortChannelId != "" {
				scidToPeer[channel.ShortChannelId] = peer
			}
		}
	}
	n.Peers = fresh
	n.scidToPeer = scidToPeer
	n.peersReadAt = readAt
	return open
}

// addLocalChannelsToGraph puts both directions of our open channels in the
// graph, with the exact fees and balances lightningd reports.
func (n *Node) addLocalChannelsToGraph(channels []*glightning.PeerChannel) {
	n.Graph.Lock()
	defer n.Graph.Unlock()

	for _, channel := range channels {
		if channel.State != "CHANNELD_NORMAL" || channel.ShortChannelId == "" {
			continue
		}
		for _, outgoing := range []bool{true, false} {
			c := n.ConvertPeerChannelToGraphChannel(channel, outgoing)
			n.Graph.AddChannel(c)
			n.Graph.Channels[c.ShortChannelId+"/"+util.GetDirection(c.Source, c.Destination)] = c
		}
	}
}

// RefreshPeerChannels reads our channels with the given peers again, so that
// the rebalances that follow one that just moved liquidity see the new
// balances without waiting for the next peer refresh.
func (n *Node) RefreshPeerChannels(peerIds ...string) error {
	for _, id := range peerIds {
		readAt := time.Now()
		resp, err := n.lightning.ListPeerChannels(id)
		if err != nil {
			return err
		}
		n.setPeerChannels(id, resp.Channels, readAt)
	}
	return nil
}

// setPeerChannels gives a known peer the open channels among channels, read
// at readAt, unless its channels were read since. The peer is replaced, not
// changed in place, as refreshPeers does: callers read the peers they got
// earlier without the lock.
func (n *Node) setPeerChannels(peerId string, channels []*glightning.PeerChannel, readAt time.Time) {
	n.PeersLock.Lock()
	old, ok := n.Peers[peerId]
	if !ok || old == nil || !readAt.After(n.peersReadAt) || !readAt.After(n.channelsReadAt[peerId]) {
		n.PeersLock.Unlock()
		return
	}
	peer := *old
	peer.Channels = make([]*glightning.PeerChannel, 0, len(channels))
	for _, channel := range channels {
		if channel.State == "CHANNELD_NORMAL" {
			peer.Channels = append(peer.Channels, channel)
		}
	}
	n.replacePeer(old, &peer)
	if n.channelsReadAt == nil {
		n.channelsReadAt = make(map[string]time.Time)
	}
	n.channelsReadAt[peerId] = readAt
	n.PeersLock.Unlock()

	n.addLocalChannelsToGraph(peer.Channels)
}

// replacePeer puts peer in place of old, in Peers and in the scid index.
// PeersLock must be held.
func (n *Node) replacePeer(old, peer *glightning.Peer) {
	for scid, p := range n.scidToPeer {
		if p == old {
			delete(n.scidToPeer, scid)
		}
	}
	for _, channel := range peer.Channels {
		if channel.ShortChannelId != "" {
			n.scidToPeer[channel.ShortChannelId] = peer
		}
	}
	n.Peers[peer.Id] = peer
}

func (n *Node) refreshLiquidity() {
	defer util.TimeTrack(time.Now(), "node.refreshLiquidity", n.Logf)
	n.Logln(glightning.Debug, "refreshing liquidity")

	hits := n.Graph.RefreshLiquidity(n.liquidityRefresh)
	n.Logf(glightning.Info, "liquidity has been reset on %d channels", hits)
}
