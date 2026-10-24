package rebalance

import (
	"circular/util"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A quote returns the route a rebalance would pay first, and pays nothing.
func TestQuoteSendsNothing(t *testing.T) {
	n, out, in := shortRoute()
	r := rebalanceOn(n, out, in, 100000000)

	route, err := r.Quote()
	assert.NoError(t, err)
	if assert.NotNil(t, route) {
		assert.Equal(t, []string{id("1x1x1", self, outP), id("2x1x1", outP, "02a"), id("3x1x1", "02a", "02b"),
			id("4x1x1", "02b", inP), id("5x1x1", inP, self)}, route.ChannelIds())
		chance, guessed := n.Estimate(route)
		assert.Equal(t, 3, guessed, "the three channels between our peers: nothing learned about them")
		assert.InDelta(t, 0.97, chance, 0.005, "100k sats through 10M-sat channels: 0.99 each")
	}
	assert.Empty(t, n.paid)

	// what a payment taught is in the next quote: 02a -> 02b cannot carry 100k
	assert.Equal(t, "failure", r.Run().Status)
	r = rebalanceOn(n, out, in, 100000000)
	_, err = r.Quote()
	assert.True(t, errors.Is(err, util.ErrNoRoute), "%v", err)

	// a learned channel is no longer guessed
	n, out, in = shortRoute()
	n.LearnUpperBound(id("2x1x1", outP, "02a"), 3000000000) // holds less than 3M sats: enough
	route, err = rebalanceOn(n, out, in, 100000000).Quote()
	assert.NoError(t, err)
	_, guessed := n.Estimate(route)
	assert.Equal(t, 2, guessed)
}

// A quote says how cheap the cheapest route is when it costs more than maxppm.
func TestQuoteOfARouteAboveMaxPPM(t *testing.T) {
	n, out, in := shortRoute()
	n.Channels[id("2x1x1", outP, "02a")].BaseFeeMillisatoshi = 50000 // 500 ppm on 100k sats
	r := rebalanceOn(n, out, in, 100000000)
	r.MaxPPM = 400

	route, err := r.Quote()
	assert.Nil(t, route)
	var tooExpensive util.ErrRouteTooExpensive
	if assert.True(t, errors.As(err, &tooExpensive), "%v", err) {
		assert.Equal(t, uint64(500), tooExpensive.FeePPM)
	}
	assert.Empty(t, n.paid)
}
