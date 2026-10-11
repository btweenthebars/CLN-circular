package rebalance

import (
	"circular/util"
	"errors"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"testing"
)

// The parallel commands blamed their candidate for every failed split, even
// one on the target channel or one caused by circular-stop.
func TestFailureSide(t *testing.T) {
	for _, c := range []struct {
		err   error
		side  Side
		retry Retry
	}{
		{nil, SideNone, RetryNo},
		{util.ErrCircularStopped, SideNone, RetryNo},
		{util.ErrSendPayTimeout, SideNone, RetryNo},
		{errors.New("sendpay: connection reset"), SideNone, RetryNo},
		{util.ErrNoRoute, SideRoute, RetryNo},
		{util.NewRouteTooExpensiveError(500, 10), SideRoute, RetryNo},
		{errShortOfLiquidity, SideRoute, RetryNo},
		{util.ErrIncomingChannelDepleted, SideIn, RetryWhenFree},
		{util.ErrIncomingPeerDisconnected, SideIn, RetryNo},
		{fmt.Errorf("%w: it can send 1 msat", util.ErrOutgoingChannelDepleted), SideOut, RetryWhenFree},
		{util.ErrOutgoingPeerDisconnected, SideOut, RetryNo},
		{onIn(util.ErrWireFeeInsufficient, RetryNow), SideIn, RetryNow},
		{onOut(errors.New("our channel could not send"), RetryWhenFree), SideOut, RetryWhenFree},
		{onIn(util.NewRouteTooExpensiveError(500, 10), RetryNo), SideIn, RetryNo},
	} {
		side, retry := failureSide(c.err)
		assert.Equal(t, c.side, side, "%v", c.err)
		assert.Equal(t, c.retry, retry, "%v", c.err)
	}
}

func TestRunReportsWhereItFailed(t *testing.T) {
	r, route := testRoute(t)

	// lightningd refused at once to add the HTLC to our out channel
	err := r.learnFromFailure(route, failureAt(self, "1x1x1", 0, failTemporaryChannelFailure), map[string]bool{})
	side, retry := failureSide(err)
	assert.Equal(t, SideOut, side)
	assert.Equal(t, RetryWhenFree, retry)

	// the in-peer could not send to us
	err = r.learnFromFailure(route, failureAt(inP, "5x1x1", 0, failTemporaryChannelFailure), map[string]bool{})
	side, retry = failureSide(err)
	assert.Equal(t, SideIn, side)
	assert.Equal(t, RetryWhenFree, retry)

	// the in-peer's channel to us is disabled: not passing
	err = r.learnFromFailure(route, failureAt(inP, "5x1x1", 0, failChannelDisabled), map[string]bool{})
	side, retry = failureSide(err)
	assert.Equal(t, SideIn, side)
	assert.Equal(t, RetryNo, retry)

	// a failure in the middle of the route
	err = r.learnFromFailure(route, failureAt("02a", "3x1x1", 0, failTemporaryChannelFailure), map[string]bool{})
	side, _ = failureSide(err)
	assert.Equal(t, SideRoute, side)
}

// Run records the side of the error that ended it.
func TestRunResultCarriesTheFailureSide(t *testing.T) {
	n, out, in := shortRoute()
	r := rebalanceOn(n, out, in, 100000000)
	r.Node.Peers[inP].Channels[0].ReceivableMsat = glightning.AmountFromMSat(0) // the target cannot receive
	result := r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, SideIn, result.FailedAt)
	assert.Equal(t, RetryWhenFree, result.Retry, "receivable leaves out HTLCs in flight")

	n, out, in = shortRoute()
	r = rebalanceOn(n, out, in, 100000000)
	r.Node.Stopped.Store(true)
	result = r.Run()
	assert.Equal(t, SideNone, result.FailedAt)
}

// The in-peer's own fee is the same for every candidate of circular-pull:
// it was blamed on each of them as "route too expensive".
func TestTargetFeeAboveMaxPPMIsTheTargetsFailure(t *testing.T) {
	n, out, in := shortRoute()
	in.Channel.FeePerMillionth = 5000
	r := rebalanceOn(n, out, in, 100000000)
	result := r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, SideIn, result.FailedAt)
	assert.Contains(t, result.Message, "charges at least 5000 ppm to send 100000 sats to us over 5x1x1, more than maxppm 1000")
	assert.NotContains(t, result.Message, "Unable to find a route", "the in-peer's own fee is not a lack of routes")
	assert.Empty(t, n.paid)
}

// The check on the in-peer's fee left out its inbound discounts, which the
// route search counts: it refused routes the search would have found.
func TestTargetFeeCheckCountsInboundDiscounts(t *testing.T) {
	n, out, in := shortRoute()
	in.Channel.FeePerMillionth = 1500
	n.SetInboundFee(id("4x1x1", "02b", inP), 0, -1000)
	r := rebalanceOn(n, out, in, 50000000)
	result := r.Run()
	assert.Equal(t, "success", result.Status, result.Message)
	assert.Less(t, result.PPM, uint64(1000))

	// a discount on another channel into the in-peer gives no route here,
	// but the in-peer's fee is no longer the reason
	n, out, in = shortRoute()
	in.Channel.FeePerMillionth = 1500
	addChannel(n.Graph, "6x1x1", "02c", inP, 0)
	n.SetInboundFee(id("6x1x1", "02c", inP), 0, -1000)
	r = rebalanceOn(n, out, in, 50000000)
	result = r.Run()
	assert.Equal(t, "failure", result.Status)
	assert.Equal(t, SideRoute, result.FailedAt, result.Message)
}

// A node failure from our own in-peer or out-peer was counted as the route's.
func TestPeerNodeFailuresAreOnOurSide(t *testing.T) {
	r, route := testRoute(t)
	err := r.learnFromFailure(route, failureAt(inP, "5x1x1", 0, failNodeBit|2), map[string]bool{})
	side, _ := failureSide(err)
	assert.Equal(t, SideIn, side)
	err = r.learnFromFailure(route, failureAt(outP, "2x1x1", 0, failNodeBit|2), map[string]bool{})
	side, _ = failureSide(err)
	assert.Equal(t, SideOut, side)
	err = r.learnFromFailure(route, failureAt("02a", "3x1x1", 0, failNodeBit|2), map[string]bool{})
	side, _ = failureSide(err)
	assert.Equal(t, SideRoute, side)
}
