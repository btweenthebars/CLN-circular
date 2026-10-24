package graph

import (
	"circular/util"
	"testing"

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
	// (a payment of 105k sats failed there: about 105k is believed, spread
	// from 0 to 210k)
	g.LearnUpperBound("4x1x1/"+util.GetDirection(outP, "02a"), 105000000)
	route, err = g.GetLikelyCircularRoute(out, in, amount, nil, 8, MaxFeeForPPM(amount, 10), 200)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1x1x1", "3x1x1", "2x1x1"}, scids(route), "about 0.52 against 0.55 now")
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
