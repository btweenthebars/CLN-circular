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
		err       error
		side      Side
		transient bool
	}{
		{nil, SideNone, false},
		{util.ErrCircularStopped, SideNone, false},
		{util.ErrSendPayTimeout, SideNone, false},
		{errors.New("sendpay: connection reset"), SideNone, false},
		{util.ErrNoRoute, SideRoute, false},
		{util.NewRouteTooExpensiveError(500, 10), SideRoute, false},
		{errShortOfLiquidity, SideRoute, false},
		{util.ErrIncomingChannelDepleted, SideIn, false},
		{util.ErrIncomingPeerDisconnected, SideIn, false},
		{fmt.Errorf("%w: it can send 1 msat", util.ErrOutgoingChannelDepleted), SideOut, false},
		{util.ErrOutgoingPeerDisconnected, SideOut, false},
		{onIn(util.ErrWireFeeInsufficient, true), SideIn, true},
		{onOut(errors.New("our channel could not send"), true), SideOut, true},
	} {
		side, transient := failureSide(c.err)
		assert.Equal(t, c.side, side, "%v", c.err)
		assert.Equal(t, c.transient, transient, "%v", c.err)
	}
}

func TestRunReportsWhereItFailed(t *testing.T) {
	r, route := testRoute(t)

	// lightningd refused at once to add the HTLC to our out channel
	err := r.learnFromFailure(route, failureAt(self, "1x1x1", 0, failTemporaryChannelFailure), map[string]bool{})
	side, transient := failureSide(err)
	assert.Equal(t, SideOut, side)
	assert.True(t, transient)

	// the in-peer could not send to us
	err = r.learnFromFailure(route, failureAt(inP, "5x1x1", 0, failTemporaryChannelFailure), map[string]bool{})
	side, transient = failureSide(err)
	assert.Equal(t, SideIn, side)
	assert.True(t, transient)

	// the in-peer's channel to us is disabled: not passing
	err = r.learnFromFailure(route, failureAt(inP, "5x1x1", 0, failChannelDisabled), map[string]bool{})
	side, transient = failureSide(err)
	assert.Equal(t, SideIn, side)
	assert.False(t, transient)

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
	assert.False(t, result.Transient)

	n, out, in = shortRoute()
	r = rebalanceOn(n, out, in, 100000000)
	r.Node.Stopped.Store(true)
	result = r.Run()
	assert.Equal(t, SideNone, result.FailedAt)
}
