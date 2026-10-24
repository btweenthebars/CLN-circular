package graph

import (
	"circular/util"
	"testing"
	"time"

	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
)

// sized is a channel of the given capacity, believed half full.
func (g *testGraph) sized(scid, from, to string, capacity, baseFee uint64) *Channel {
	c := g.channel(scid, from, to, baseFee, 0)
	c.AmountMsat = glightning.AmountFromMSat(capacity)
	c.Liquidity = capacity / 2
	return c
}

func TestSuccessChance(t *testing.T) {
	g := newTestGraph()
	c := g.channel("1x1x1", "02a", "02b", 0, 0) // 10M sats, believed half full: any balance as likely

	assert.InDelta(t, 0.99, c.SuccessChance(100000000), 1e-9)
	assert.InDelta(t, 0.5, c.SuccessChance(5000000000), 1e-9)

	// payments showed it holds about 1M: balances from 0 to 2M
	c.Liquidity = 1000000000
	assert.InDelta(t, 0.75, c.SuccessChance(500000000), 1e-9)
	assert.Equal(t, 0.0, c.SuccessChance(2000000000))

	// about 9M: from 8M to 10M
	c.Liquidity = 9000000000
	assert.Equal(t, 1.0, c.SuccessChance(8000000000))
	assert.InDelta(t, 0.5, c.SuccessChance(9000000000), 1e-9)

	c.Liquidity = 0 // unusable
	assert.Equal(t, 0.0, c.SuccessChance(1))
}

// Routes were chosen by fees alone: a free route over channels barely larger
// than the payment won over one charging 10 ppm over channels a hundred times
// larger, and failed far more often.
func TestLikelyRouteWeighsChanceAgainstFees(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.sized("3x1x1", outP, "02a", 220000000, 0) // 220k sats
	g.sized("4x1x1", "02a", inP, 220000000, 0)
	g.channel("5x1x1", outP, "02b", 0, 0) // 10M sats
	g.channel("6x1x1", "02b", inP, 1000, 0)
	amount := uint64(100000000)

	route, err := g.GetLikelyCircularRoute(out, in, amount, nil, 8, MaxFeeForPPM(amount, 500), 200)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "5x1x1", "6x1x1", "2x1x1"}, scids(route), "about 0.98 against 0.3 for 10 ppm more")

	route, err = g.GetLikelyCircularRoute(out, in, amount, nil, 8, MaxFeeForPPM(amount, 500), 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "3x1x1", "4x1x1", "2x1x1"}, scids(route), "0: fees alone")

	// maxppm still bounds the fees
	route, err = g.GetLikelyCircularRoute(out, in, amount, nil, 8, MaxFeeForPPM(amount, 5), 200)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "3x1x1", "4x1x1", "2x1x1"}, scids(route))

	_, err = g.GetLikelyCircularRoute(out, in, amount, map[string]bool{"02a": true, "02b": true}, 8, MaxFeeForPPM(amount, 500), 200)
	assert.Equal(t, util.ErrNoRoute, err)
}

// The fewest hops used to win whatever the channels: a direct channel
// between our peers barely larger than the payment beat a longer route over
// large ones.
func TestLikelyRouteMayTakeMoreHops(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.sized("3x1x1", outP, inP, 220000000, 0)
	g.channel("4x1x1", outP, "02a", 0, 0)
	g.channel("5x1x1", "02a", inP, 0, 0)
	amount := uint64(100000000)

	route, err := g.GetLikelyCircularRoute(out, in, amount, nil, 8, MaxFeeForPPM(amount, 10), 200)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "4x1x1", "5x1x1", "2x1x1"}, scids(route))

	route, err = g.GetLikelyCircularRoute(out, in, amount, nil, 8, MaxFeeForPPM(amount, 10), 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "3x1x1", "2x1x1"}, scids(route), "0: the fewest hops")

	// a route the graph learned holds little is avoided, not only excluded
	// (a payment of 105k sats failed there: it holds less, any balance under
	// 105k as likely)
	g.LearnUpperBound("4x1x1/"+util.GetDirection(outP, "02a"), 105000000)
	route, err = g.GetLikelyCircularRoute(out, in, amount, nil, 8, MaxFeeForPPM(amount, 10), 200)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "3x1x1", "2x1x1"}, scids(route), "about 0.05 against 0.55 now")
}

// Labels were kept by cost alone: one cheap in cost but dear in fees hid,
// on a shared channel, one within maxFee, and once the first went over
// maxFee the search returned a costlier route.
func TestLikelyRouteKeepsLabelsWithinTheFeeLimit(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.channel("3x1x1", outP, "02a", 0, 30)       // shared: 30 ppm
	g.channel("4x1x1", "02a", inP, 0, 80)        // likely, but 80 ppm more: 110 in all
	g.sized("5x1x1", "02a", "02d", 333333334, 0) // free, chance about 0.7
	g.sized("6x1x1", "02d", inP, 333333334, 0)
	g.sized("7x1x1", outP, "02e", 210000000, 0) // free, chance about 0.52
	g.sized("8x1x1", "02e", inP, 210000000, 0)
	amount := uint64(100000000)

	route, err := g.GetLikelyCircularRoute(out, in, amount, nil, 8, MaxFeeForPPM(amount, 100), 200)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "3x1x1", "5x1x1", "6x1x1", "2x1x1"}, scids(route),
		"30 ppm and two channels at 0.7, within 100 ppm, over two at 0.52")
}

// A failure made the belief an estimate just under the failed amount, read
// as the middle of the possible balances: about an even chance for slightly
// smaller amounts, though the channel had just shown it holds less.
func TestAFailureMakesABound(t *testing.T) {
	g := newTestGraph()
	g.channel("1x1x1", "02a", "02b", 0, 0) // 10M sats
	back := g.channel("1x1x1", "02b", "02a", 0, 0)
	id := "1x1x1/" + util.GetDirection("02a", "02b")
	forward := g.Channels[id]

	g.LearnUpperBound(id, 1000000000) // 1M sats failed
	assert.True(t, forward.Upper)
	assert.False(t, back.Upper, "the other direction holds at least the rest: no bound")
	assert.InDelta(t, 0.1, forward.SuccessChance(900000000), 0.001, "any balance under 1M as likely")
	assert.InDelta(t, 0.9, forward.SuccessChance(100000000), 0.001)

	// it is kept across a restart and a new policy
	saved := g.Beliefs()
	assert.True(t, saved[id].Upper)
	fresh := newTestGraph()
	fresh.channel("1x1x1", "02a", "02b", 0, 0)
	assert.Equal(t, 1, fresh.ApplyBeliefs(map[string]Belief{id: saved[id]}, 0))
	assert.True(t, fresh.Channels[id].Upper)
	g.ApplyChannelUpdate(&ChannelUpdate{ChannelId: id, Timestamp: 10, FeePPM: 5})
	assert.True(t, g.Channels[id].Upper, "a new fee says nothing about the balance")
	forward = g.Channels[id]

	// a payment it carried, more than the bound, ends it
	g.LearnLowerBound(id, 2000000000)
	assert.False(t, forward.Upper)
	assert.True(t, g.Channels["1x1x1/"+util.GetDirection("02b", "02a")].Upper, "the other direction now holds at most the rest")

}

// A bound faded toward half the capacity like an estimate: one above it got
// tighter, which nobody had learned, and the flag outlived it.
func TestABoundLoosensTowardTheCapacity(t *testing.T) {
	g := newTestGraph()
	low := g.channel("1x1x1", "02a", "02b", 0, 0) // 10M sats
	high := g.channel("2x1x1", "02a", "02c", 0, 0)
	half := g.channel("3x1x1", "02a", "02d", 0, 0)
	hour := int64(3600)
	now := time.Now().Unix()
	low.Liquidity, low.Upper, low.Timestamp = 1000000000, true, now-hour // under 1M, an hour ago
	high.Liquidity, high.Upper, high.Timestamp = 8000000000, true, now-hour
	half.Liquidity, half.Upper, half.Timestamp = 5000000000, true, now-hour
	assert.True(t, half.learned(), "a bound at half the capacity is still a bound")

	g.DecayLiquidity(time.Hour)
	assert.InDelta(t, 5500000000, float64(low.Liquidity), 2000000, "half way to the capacity")
	assert.InDelta(t, 9000000000, float64(high.Liquidity), 2000000, "looser, not tighter")
	assert.InDelta(t, 7500000000, float64(half.Liquidity), 2000000)
	for _, c := range []*Channel{low, high, half} {
		assert.True(t, c.Upper)
		assert.Greater(t, c.SuccessChance(4000000000), 0.2)
	}
	assert.Less(t, low.SuccessChance(4000000000), 0.6, "still below an unknown channel's")
	assert.True(t, low.CanForward(5000000000))
	assert.False(t, low.CanForward(5200000000), "no more than an unknown channel would take")

	// at the capacity it is no bound: the default, and an unknown channel
	low.Timestamp, g.lastDecay = now-20*hour, 0
	g.DecayLiquidity(time.Hour)
	assert.False(t, low.Upper)
	assert.Equal(t, low.AmountMsat.MSat()/2, low.Liquidity)
	assert.InDelta(t, 0.6, low.SuccessChance(4000000000), 1e-9)
}

// A channel priced far above its node's others was given the same chance as
// any other unknown channel, though such channels are usually drained.
func TestDearChannelsAreLessLikely(t *testing.T) {
	g := newTestGraph()
	g.channel("1x1x1", "02a", "02b", 0, 100)
	g.channel("2x1x1", "02a", "02c", 0, 100)
	g.channel("3x1x1", "02a", "02d", 0, 120)
	dear := g.channel("4x1x1", "02a", "02e", 0, 300) // three times the median, 100
	usual := g.Channels["3x1x1/"+util.GetDirection("02a", "02d")]
	alone := g.channel("5x1x1", "02z", "02y", 0, 5000) // a node with one channel: no median
	// a node charging nothing: 1 ppm is twice its median, but no sign
	g.channel("6x1x1", "02f", "02b", 0, 0)
	g.channel("7x1x1", "02f", "02c", 0, 0)
	cheap := g.channel("8x1x1", "02f", "02d", 0, 1)

	assert.Equal(t, 1, g.MarkDearChannels())
	assert.True(t, dear.Dear)
	assert.False(t, usual.Dear)
	assert.False(t, alone.Dear)
	assert.False(t, cheap.Dear)

	amount := uint64(1000000000) // 1M sats of 10M
	assert.InDelta(t, 0.9, usual.SuccessChance(amount), 1e-9)
	assert.InDelta(t, 1-1.0/6, dear.SuccessChance(amount), 1e-9, "believed to hold 30%: spread over 0 to 6M")

	// what a payment taught wins over the price
	g.LearnLowerBound("4x1x1/"+util.GetDirection("02a", "02e"), 8000000000)
	assert.Equal(t, 1.0, dear.SuccessChance(amount))

	// a fee update is judged against the node's median at once
	id := "1x1x1/" + util.GetDirection("02a", "02b")
	g.ApplyChannelUpdate(&ChannelUpdate{ChannelId: id, Timestamp: 10, FeePPM: 500})
	assert.True(t, g.Channels[id].Dear)
	g.ApplyChannelUpdate(&ChannelUpdate{ChannelId: id, Timestamp: 11, FeePPM: 100})
	assert.False(t, g.Channels[id].Dear)
}

// Between two otherwise equal routes, the one through a dear channel loses.
func TestLikelyRouteAvoidsDearChannels(t *testing.T) {
	g := newTestGraph()
	out := g.channel("1x1x1", self, outP, 0, 0)
	in := g.channel("2x1x1", inP, self, 0, 0)
	g.channel("3x1x1", outP, "02a", 0, 0)
	g.channel("4x1x1", "02a", inP, 0, 0)
	g.channel("5x1x1", outP, "02b", 0, 0)
	g.channel("6x1x1", "02b", inP, 0, 0)
	// 02a charges far more than usual toward the in-peer
	g.channel("7x1x1", "02a", "02x", 0, 10)
	g.channel("8x1x1", "02a", "02y", 0, 10)
	g.Channels["4x1x1/"+util.GetDirection("02a", inP)].FeePerMillionth = 100
	g.Channels["6x1x1/"+util.GetDirection("02b", inP)].FeePerMillionth = 100 // the same fee: only the signal differs
	g.MarkDearChannels()
	amount := uint64(1000000000)

	route, err := g.GetLikelyCircularRoute(out, in, amount, nil, 8, MaxFeeForPPM(amount, 500), 200)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "5x1x1", "6x1x1", "2x1x1"}, scids(route))
}
