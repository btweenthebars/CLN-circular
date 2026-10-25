package parallel

import (
	"circular/node"
	"circular/util"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// forwardsOf fakes Node.Earned: each scid's rate and volume (sats), counting
// the reads.
type forwardsOf struct {
	ppm, sats map[string]uint64
	reads     map[string]int
	err       error
}

func (f *forwardsOf) read(scid string, _ time.Time) (uint64, uint64, error) {
	if f.reads == nil {
		f.reads = map[string]int{}
	}
	f.reads[scid]++
	return f.ppm[scid], f.sats[scid] * 1000, f.err
}

// With a fee ratio, a split pays at most ratio × what the filled channel
// earns (its forwards, or our fee on it with too few), less our fee on the
// drained channel, and never more than maxppm. Base fees here are 1 ppm of a
// 100k-sat split per 100 msat.
func TestSplitEconomics(t *testing.T) {
	n, add, _ := quoteNetwork()
	// circular-pull into 9x1x1: we charge 600 ppm on it; candidates drain
	// 1x1x1 (we charge 100 ppm) and 2x1x1 (450 ppm)
	target := add("9x1x1", "02in", "02self", 0)
	add("9x1x1", "02self", "02in", 60000)
	cheap := add("1x1x1", "02self", "02p0", 10000)
	dear := add("2x1x1", "02self", "02p1", 45000)
	a := quoteRun(n, target, "", cheap, dear)
	a.maxPPM = 1000
	fw := &forwardsOf{ppm: map[string]uint64{"9x1x1": 500}, sats: map[string]uint64{"9x1x1": 200000}}
	a.forwards = fw.read

	assert.Equal(t, uint64(1000), a.splitEconomics(cheap).limit, "no fee ratio: maxppm")

	a.feeRatio = 0.8
	e := a.splitEconomics(cheap)
	assert.Equal(t, uint64(300), e.limit, "0.8 × 500 − 100")
	assert.Equal(t, earning{ppm: 500, basis: "earned"}, e.earn)
	assert.Equal(t, uint64(100), e.drain)
	assert.Equal(t, "the filled channel earns 500 ppm (earned), the drained one charges 100 ppm: limit 300 ppm", e.String())
	assert.Equal(t, uint64(0), a.splitEconomics(dear).limit, "0.8 × 500 − 450 < 1: not worth it")
	assert.Equal(t, 1, fw.reads["9x1x1"], "the target's forwards read once")

	a.maxPPM = 200
	assert.Equal(t, uint64(200), a.splitEconomics(cheap).limit, "never above maxppm")

	// a split is fired with its limit, and the run's other settings
	a.maxPPM, a.minSplit, a.maxNodeFails, a.riskPPM = 1000, 50000000, 7, -1
	split := a.newSplit(cheap, target, cheap, 100000000)
	assert.Equal(t, uint64(300), split.MaxPPM)
	assert.Equal(t, uint64(100000000), split.Amount)
	assert.Equal(t, uint64(50000000), split.MinAmount)
	assert.Equal(t, 7, split.MaxNodeFails)
	assert.Equal(t, -1, split.RiskPPM)
	assert.True(t, split.Halt == &a.halt && split.Sent == &a.sent, "the run's stop and payment count")
	a.feeRatio = 0
	assert.Equal(t, uint64(1000), a.newSplit(cheap, target, cheap, 100000000).MaxPPM)
	a.feeRatio = 0.8

	// too few forwards, or none readable: our fee on it stands in
	for _, f := range []*forwardsOf{
		{ppm: map[string]uint64{"9x1x1": 500}, sats: map[string]uint64{"9x1x1": 50000}},
		{err: errors.New("rpc")},
	} {
		a = quoteRun(n, target, "", cheap)
		a.maxPPM, a.feeRatio, a.forwards = 1000, 0.8, f.read
		e = a.splitEconomics(cheap)
		assert.Equal(t, earning{ppm: 600, basis: "current fee"}, e.earn)
		assert.Equal(t, uint64(380), e.limit, "0.8 × 600 − 100")
	}

	// circular-push out of 1x1x1 (we charge 50 ppm) into 8x1x1, whose
	// forwards earned 300 ppm
	n, add, _ = quoteNetwork()
	out := add("1x1x1", "02self", "02t", 5000)
	in := add("8x1x1", "02c", "02self", 0)
	add("8x1x1", "02self", "02c", 90000)
	a = quoteRun(n, out, "", in)
	a.maxPPM, a.feeRatio = 1000, 0.8
	a.forwards = (&forwardsOf{ppm: map[string]uint64{"8x1x1": 300}, sats: map[string]uint64{"8x1x1": 1000000}}).read
	assert.Equal(t, uint64(190), a.splitEconomics(in).limit, "0.8 × 300 − 50")
}

// A candidate not worth it is skipped, and says why: in a run, in stopped
// once none is left, and in a quote, which shows each candidate's limit.
func TestCandidatesNotWorthIt(t *testing.T) {
	n, add, peer := quoteNetwork()
	target := add("9x1x1", "02in", "02self", 0)
	add("9x1x1", "02self", "02in", 60000)
	cheap := add("1x1x1", "02self", "02p0", 10000)
	dear := add("2x1x1", "02self", "02p1", 45000)
	add("4x1x1", "02p0", "02in", 0)
	add("5x1x1", "02p1", "02in", 0)
	peer("02p0", "1x1x1")
	peer("02p1", "2x1x1")
	peer("02in", "9x1x1")
	run := func() *AbstractRebalance {
		a := quoteRun(n, target, "", dear, cheap)
		a.maxPPM, a.feeRatio = 1000, 0.8
		a.forwards = (&forwardsOf{ppm: map[string]uint64{"9x1x1": 500}, sats: map[string]uint64{"9x1x1": 200000}}).read
		return a
	}

	a := run()
	next, err := a.GetNextCandidate()
	assert.NoError(t, err)
	assert.Equal(t, "1x1x1", next.ShortChannelId, "2x1x1 skipped")
	assert.Equal(t, map[string]string{"2x1x1/" + util.GetDirection("02self", "02p1"): notWorthIt}, a.gone)

	quote := run().quote("pull", true)
	if assert.Equal(t, 2, len(quote.Candidates)) {
		c := quote.Candidates[0]
		assert.Equal(t, "2x1x1", c.Scid)
		assert.Equal(t, "not worth it at feeratio 0.8: the filled channel earns 500 ppm (earned), the drained one charges 450 ppm: limit 0 ppm", c.Skipped)
		c = quote.Candidates[1]
		assert.Empty(t, c.Error)
		if assert.NotNil(t, c.MaxPPM) {
			assert.Equal(t, uint64(300), *c.MaxPPM)
		}
		out, _ := json.Marshal(c)
		assert.Contains(t, string(out), `"maxppm":300,"economics":"the filled channel earns 500 ppm (earned)`)
	}

	// without a fee ratio, a quote shows no limit per candidate
	a = run()
	a.feeRatio = 0
	out, _ := json.Marshal(a.quote("pull", true))
	assert.NotContains(t, string(out), "economics")
}

func TestSetFeeRatio(t *testing.T) {
	a := &AbstractRebalance{Node: &node.Node{Defaults: node.Defaults{FeeRatio: 0.7, EarnDays: 15}}}
	a.setFeeRatio(0)
	assert.Equal(t, 0.7, a.feeRatio, "the node's default")
	assert.WithinDuration(t, time.Now().Add(-15*24*time.Hour), a.earnSince, time.Minute)
	a.setFeeRatio(0.9)
	assert.Equal(t, 0.9, a.feeRatio)
	a.setFeeRatio(-1)
	assert.Equal(t, 0.0, a.feeRatio, "off whatever the node's default")

	a = &AbstractRebalance{Node: &node.Node{}}
	a.setFeeRatio(0)
	assert.Equal(t, 0.0, a.feeRatio, "off by default")
	assert.WithinDuration(t, time.Now().Add(-node.DEFAULT_EARN_DAYS*24*time.Hour), a.earnSince, time.Minute)
}
