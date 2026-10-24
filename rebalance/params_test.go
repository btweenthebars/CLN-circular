package rebalance

import (
	"circular/node"
	"circular/util"
	"errors"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

// peerChannel is a channel with a balance of 6M sats out of 10M, but channel
// reserves and HTLCs in flight leave less to send and receive.
func peerChannel(peer, scid string, spendable, receivable uint64) *glightning.PeerChannel {
	return &glightning.PeerChannel{
		PeerId:         peer,
		ShortChannelId: scid,
		State:          NORMAL,
		PeerConnected:  true,
		TotalMsat:      glightning.AmountFromMSat(10000000000),
		ToUsMsat:       glightning.AmountFromMSat(6000000000),
		SpendableMsat:  glightning.AmountFromMSat(spendable),
		ReceivableMsat: glightning.AmountFromMSat(receivable),
	}
}

// The checks used to-us and total minus to-us, which ignore reserves,
// commitment fees and HTLCs in flight, so payments failed at our own channels.
func TestCheckLiquidityUsesSpendableAndReceivable(t *testing.T) {
	r := &Rebalance{Amount: 3000000000}
	enough := peerChannel(outP, "1x1x1", 3000000000, 3000000000)
	assert.NoError(t, r.checkLiquidity(enough, enough))

	// 4M sats remote balance, but 1.5M of it cannot be received
	in := peerChannel(inP, "5x1x1", 0, 2500000000)
	assert.Equal(t, util.ErrIncomingChannelDepleted, r.checkLiquidity(in, enough))

	// 6M sats local balance, but only 2.9M can be sent
	out := peerChannel(outP, "1x1x1", 2900000000, 0)
	assert.Equal(t, util.ErrOutgoingChannelDepleted, r.checkLiquidity(enough, out))
}

// The out channel carries the amount plus the fees of the other hops.
func TestCheckFirstHopCountsTheFees(t *testing.T) {
	r, route := testRoute(t) // 100,000 sats, 4,000 msat of fees
	assert.Equal(t, uint64(100004000), route.Hops[0].MilliSatoshi)

	out := peerChannel(outP, "1x1x1", 100003999, 0)
	r.Node.PeersLock = &sync.RWMutex{}
	r.Node.Peers = map[string]*glightning.Peer{outP: {Id: outP, Channels: []*glightning.PeerChannel{out}}}
	assert.NoError(t, r.checkLiquidity(peerChannel(inP, "5x1x1", 0, 100000000), out))
	err := r.checkFirstHop(route)
	assert.True(t, errors.Is(err, util.ErrOutgoingChannelDepleted), "got %v", err)

	out.SpendableMsat = glightning.AmountFromMSat(100004000)
	assert.NoError(t, r.checkFirstHop(route))
}

// The node's startup options fill what a call leaves out; what a call gives
// wins; with neither, the built-in defaults.
func TestNodeDefaults(t *testing.T) {
	n := &node.Node{Defaults: node.Defaults{Attempts: 7, MaxHops: 5, MaxNodeFails: 3}}
	r := &Rebalance{Node: n}
	r.setDefaults()
	assert.Equal(t, 7, r.Attempts)
	assert.Equal(t, 5, r.MaxHops)
	assert.Equal(t, 3, r.maxNodeFails())

	r = &Rebalance{Node: n, Attempts: 2, MaxHops: 9, MaxNodeFails: 4}
	r.setDefaults()
	assert.Equal(t, 2, r.Attempts)
	assert.Equal(t, 9, r.MaxHops)
	assert.Equal(t, 4, r.maxNodeFails())

	r = &Rebalance{Node: &node.Node{}}
	r.setDefaults()
	assert.Equal(t, DEFAULT_ATTEMPTS, r.Attempts)
	assert.Equal(t, DEFAULT_MAXHOPS, r.MaxHops)
	assert.Equal(t, DEFAULT_MAX_NODE_FAILS, r.maxNodeFails())
}
