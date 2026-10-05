package parallel

import (
	"circular/node"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

// a 10M sats channel with local balance toUs
func channel(toUs, spendable, receivable uint64) *glightning.PeerChannel {
	return &glightning.PeerChannel{
		ShortChannelId: "1x1x1",
		State:          "CHANNELD_NORMAL",
		PeerConnected:  true,
		TotalMsat:      glightning.AmountFromMSat(10000000000),
		ToUsMsat:       glightning.AmountFromMSat(toUs),
		SpendableMsat:  glightning.AmountFromMSat(spendable),
		ReceivableMsat: glightning.AmountFromMSat(receivable),
	}
}

func abstract() AbstractRebalance {
	return AbstractRebalance{Node: &node.Node{}, splitAmount: 100000000, maxPPM: 100}
}

// The deplete threshold was checked before the split, so a channel just above
// it was depleted below it by the split.
func TestPullKeepsTheDepleteThresholdAfterTheSplit(t *testing.T) {
	r := &RebalancePull{DepleteUpToPercent: 0.2, DepleteUpToAmount: 1000000000, AbstractRebalance: abstract()}
	// threshold 1M sats; the split sends 100k sats and up to 10,099 msat of fees (100 ppm)
	assert.Equal(t, util.ErrChannelDepleted, r.CanUseChannel(channel(1000000000, 1000000000, 0)))
	assert.Equal(t, util.ErrChannelDepleted, r.CanUseChannel(channel(1100010098, 1100010098, 0)))
	assert.NoError(t, r.CanUseChannel(channel(1100010099, 1000000000, 0)))

	// an HTLC of ours in flight is still counted in the balance
	c := channel(1200000000, 1000000000, 0)
	c.Htlcs = []*glightning.Htlc{{Direction: "out", AmountMsat: glightning.AmountFromMSat(100000000)}}
	assert.Equal(t, util.ErrChannelDepleted, r.CanUseChannel(c))
	c.Htlcs[0].Direction = "in"
	assert.NoError(t, r.CanUseChannel(c))

	// the split itself must fit in one HTLC
	assert.Equal(t, util.ErrChannelDepleted, r.CanUseChannel(channel(9000000000, 100010098, 0)))
}

func TestPushKeepsTheFillThresholdAfterTheSplit(t *testing.T) {
	r := &RebalancePush{FillUpToPercent: 0.2, FillUpToAmount: 1000000000, AbstractRebalance: abstract()}
	// remote balance 1,099,999,999 msat
	assert.Equal(t, util.ErrChannelFilled, r.CanUseChannel(channel(8900000001, 0, 1000000000)))
	assert.NoError(t, r.CanUseChannel(channel(8900000000, 0, 1000000000)))

	c := channel(8000000000, 0, 1000000000)
	c.Htlcs = []*glightning.Htlc{{Direction: "in", AmountMsat: glightning.AmountFromMSat(900000001)}}
	assert.Equal(t, util.ErrChannelFilled, r.CanUseChannel(c))

	assert.Equal(t, util.ErrChannelFilled, r.CanUseChannel(channel(0, 0, 99999999)))
}

// lightningd caps spendable and receivable at 2^32-1 msat when the peer has
// not negotiated large channels, as LND by default does not. Comparing the
// thresholds with them refused a 6M sats channel with 6M sats of remote
// balance whatever its balance.
func TestThresholdsUseTheBalanceNotTheHTLCLimit(t *testing.T) {
	const maxHTLC = 4294967295
	c := channel(0, 0, maxHTLC)
	c.TotalMsat = glightning.AmountFromMSat(6000000000)

	push := &RebalancePush{AbstractRebalance: abstract()}
	push.setDefaults() // keep at least min(80%, 10M sats) remote: 4.8M sats
	assert.NoError(t, push.CanUseChannel(c))

	c = channel(10000000000, maxHTLC, 0)
	pull := &RebalancePull{DepleteUpToPercent: 0.5, DepleteUpToAmount: 5000000, AbstractRebalance: abstract()}
	pull.setDefaults()
	assert.NoError(t, pull.CanUseChannel(c))
}

func TestSaturatingArithmetic(t *testing.T) {
	assert.Equal(t, uint64(math.MaxUint64), addSaturating(math.MaxUint64-1, 2))
	assert.Equal(t, uint64(0), subSaturating(1, 2))

	// an absurd maxppm makes the fee bound saturate rather than wrap
	r := &RebalancePull{DepleteUpToPercent: 0.2, DepleteUpToAmount: 1000000000, AbstractRebalance: abstract()}
	r.maxPPM = math.MaxUint64
	assert.Equal(t, util.ErrChannelDepleted, r.CanUseChannel(channel(10000000000, 10000000000, 0)))
}

func TestPushDefaultsMatchTheReadme(t *testing.T) {
	r := &RebalancePush{}
	r.setDefaults()
	assert.Equal(t, 0.8, r.FillUpToPercent)
	assert.Equal(t, uint64(10000000000), r.FillUpToAmount)
}
