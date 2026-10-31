package parallel

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
)

// forwardsOf fakes Node.Earned: each scid's rate and volume (sats), counting
// the reads.
type forwardsOf struct {
	ppm, sats map[string]uint64
	reads     map[string]int
	err       error
	lock      sync.Mutex // prefetchEarnings reads several at once
}

func (f *forwardsOf) read(scid string, _ time.Time) (uint64, uint64, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
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
	assert.Zero(t, a.splitEconomics(cheap).most, "no fee ratio: no bound")

	a.feeRatio = 0.8
	e := a.splitEconomics(cheap)
	assert.Equal(t, uint64(300), e.limit, "0.8 × 500 − 100")
	assert.Equal(t, earning{ppm: 500, basis: "earned"}, e.earn)
	assert.Equal(t, uint64(100), e.drain)
	assert.Equal(t, "the filled channel earns 500 ppm (earned), the drained one charges 100 ppm: limit 300 ppm", e.String())
	assert.Equal(t, uint64(300), e.most)
	assert.Equal(t, "feeratio 0.8 × 500 ppm earned by the filled channel − 100 ppm our fee on the drained one", e.why(0.8))
	assert.Equal(t, uint64(0), a.splitEconomics(dear).limit, "0.8 × 500 − 450 < 1: not worth it")
	assert.Zero(t, a.splitEconomics(dear).most)
	assert.Equal(t, 1, fw.reads["9x1x1"], "the target's forwards read once")

	a.maxPPM = 200
	assert.Equal(t, uint64(200), a.splitEconomics(cheap).limit, "never above maxppm")
	assert.Equal(t, uint64(300), a.splitEconomics(cheap).most, "what it could pay at a higher maxppm")

	// the round keeps the highest limit and the most its splits could pay
	a.roundTopLimit, a.roundMost = 0, 0
	a.newSplit(cheap, target, cheap, 100000000)
	assert.Equal(t, uint64(200), a.roundTopLimit)
	assert.Equal(t, uint64(300), a.roundMost)

	// a split is fired with its limit, and the run's other settings
	a.maxPPM, a.minSplit, a.maxNodeFails, a.riskPPM = 1000, 50000000, 7, -1
	split := a.newSplit(cheap, target, cheap, 100000000)
	assert.Equal(t, uint64(300), split.MaxPPM)
	assert.Equal(t, uint64(100000000), split.Amount)
	assert.Equal(t, uint64(50000000), split.MinAmount)
	assert.Equal(t, 7, split.MaxNodeFails)
	assert.Equal(t, -1, split.RiskPPM)
	assert.True(t, split.Halt == &a.halt && split.Sent == &a.sent, "the run's stop and payment count")
	assert.Equal(t, uint64(1000), split.RunMaxPPM)
	assert.Equal(t, "feeratio 0.8 × 500 ppm earned by the filled channel − 100 ppm our fee on the drained one",
		split.LimitWhy, "how its limit came about, for its messages")
	a.feeRatio = 0
	split = a.newSplit(cheap, target, cheap, 100000000)
	assert.Equal(t, uint64(1000), split.MaxPPM)
	assert.Empty(t, split.LimitWhy, "no fee ratio: no limit of its own")
	a.feeRatio, a.maxPPM = 0.8, 200
	assert.Empty(t, a.newSplit(cheap, target, cheap, 100000000).LimitWhy, "held at maxppm: no limit of its own")
	a.maxPPM = 1000

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
		assert.Equal(t, "feeratio 0.8 × 600 ppm, our fee on the filled channel (too few forwards to go by), − 100 ppm "+
			"our fee on the drained one", e.why(0.8))
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

// A candidate whose in-peer charges more than its own limit is skipped up
// front: its split failed the in-peer check, which in circular-pull reads as
// the target refusing, and stopped the run.
func TestInPeerAboveACandidatesLimit(t *testing.T) {
	n, add, _ := quoteNetwork()
	target := add("9x1x1", "02in", "02self", 0)
	add("9x1x1", "02self", "02in", 60000)
	add("3x1x1", "02m", "02in", 20000) // the in-peer charges 200 ppm to send to us
	target.BaseFeeMillisatoshi = 20000
	cheap := add("1x1x1", "02self", "02p0", 0)
	dear := add("2x1x1", "02self", "02p1", 30000)
	a := quoteRun(n, target, "", cheap, dear)
	a.maxPPM, a.feeRatio = 1000, 0.5
	a.forwards = (&forwardsOf{ppm: map[string]uint64{"9x1x1": 1000}, sats: map[string]uint64{"9x1x1": 200000}}).read

	e, why := a.notWorth(dear)
	assert.Equal(t, uint64(200), e.limit, "0.5 × 1000 − 300")
	assert.Empty(t, why, "200 ppm is within its limit")
	a.feeRatio = 0.45
	e, why = a.notWorth(dear)
	assert.Equal(t, uint64(150), e.limit)
	assert.Contains(t, why, "charges at least 200 ppm to send to us, more than this candidate's limit")
	_, why = a.notWorth(cheap)
	assert.Empty(t, why, "450 ppm")
	assert.Equal(t, uint64(1000), a.newSplit(dear, target, dear, 100000000).RunMaxPPM)

	// above the run's maxppm too, the in-peer's fee is the target refusing,
	// as without a fee ratio: left to the split, and the run says so
	a.maxPPM = 190
	e, why = a.notWorth(dear)
	assert.Equal(t, uint64(150), e.limit)
	assert.Empty(t, why)
	// at the run's maxppm: no limit of its own
	a.maxPPM = 150
	_, why = a.notWorth(cheap)
	assert.Empty(t, why)
	assert.Equal(t, uint64(0), a.newSplit(cheap, target, cheap, 100000000).RunMaxPPM)
}

// A ratio that is not a finite number never lifts a split above maxppm.
func TestFeeRatioNotANumber(t *testing.T) {
	a := &AbstractRebalance{Node: &node.Node{}}
	a.setFeeRatio(math.Inf(1))
	assert.Equal(t, 0.0, a.feeRatio)
	a.setFeeRatio(math.NaN())
	assert.Equal(t, 0.0, a.feeRatio)

	n, add, _ := quoteNetwork()
	target := add("9x1x1", "02in", "02self", 0) // we charge nothing on it
	candidate := add("1x1x1", "02self", "02p0", 0)
	a = quoteRun(n, target, "", candidate)
	a.maxPPM, a.feeRatio = 100, math.Inf(1) // Inf × 0 − 0 is NaN
	a.forwards = (&forwardsOf{}).read
	assert.Equal(t, uint64(0), a.splitEconomics(candidate).limit)
}

// What the channels a run fills earn is read before it fires anything: the
// target in circular-pull, every candidate in circular-push.
func TestEarningsAreReadBeforeTheRun(t *testing.T) {
	n, add, _ := quoteNetwork()
	out := add("1x1x1", "02self", "02t", 0)
	var candidates []*graph.Channel
	for _, scid := range []string{"7x1x1", "8x1x1", "9x1x1"} {
		candidates = append(candidates, add(scid, "02c"+scid[:1], "02self", 0))
	}
	a := quoteRun(n, out, "", candidates...)
	a.maxPPM, a.feeRatio = 1000, 0.8
	fw := &forwardsOf{ppm: map[string]uint64{"7x1x1": 100, "8x1x1": 200, "9x1x1": 300},
		sats: map[string]uint64{"7x1x1": 200000, "8x1x1": 200000, "9x1x1": 200000}}
	a.forwards = fw.read
	a.prefetchEarnings()
	assert.Equal(t, map[string]int{"7x1x1": 1, "8x1x1": 1, "9x1x1": 1}, fw.reads)
	assert.Equal(t, uint64(240), a.splitEconomics(candidates[2]).limit)
	assert.Equal(t, 1, fw.reads["9x1x1"], "not read again")

	// pull: the target only
	target := add("6x1x1", "02in", "02self", 0)
	a = quoteRun(n, target, "", out)
	a.maxPPM, a.feeRatio = 1000, 0.8
	fw = &forwardsOf{}
	a.forwards = fw.read
	a.prefetchEarnings()
	assert.Equal(t, map[string]int{"6x1x1": 1}, fw.reads)

	// without a fee ratio: nothing
	a = quoteRun(n, target, "", out)
	fw = &forwardsOf{}
	a.forwards = fw.read
	a.prefetchEarnings()
	assert.Empty(t, fw.reads)
}

// When a run with a fee ratio moved nothing, the result did not say whether
// the ratio or maxppm held it back: it counts the candidates the ratio left
// out, and the splits that failed on their own limit where maxppm would have
// let them go on.
func TestFeeratioCountsInTheResult(t *testing.T) {
	f := splitsOn(t, 4, func(int, uint64) uint64 { return 0 })
	f.splits = 1
	f.maxPPM, f.feeRatio = 1000, 0.8
	f.TargetChannel = graph.NewChannel(&glightning.Channel{ShortChannelId: "9x1x1", Source: "02in", Destination: "02self"}, 0, 0)
	f.forwards = (&forwardsOf{ppm: map[string]uint64{"9x1x1": 500}, sats: map[string]uint64{"9x1x1": 200000}}).read
	// we charge 450 ppm on 1x1x1 and 3x1x1: 0.8 × 500 − 450 < 1, not worth it
	for _, scid := range []string{"1x1x1", "3x1x1"} {
		peer := fmt.Sprintf("02peer%d", scid[0]-'1') // splitsOn: 1x1x1 is with 02peer0
		dear := graph.NewChannel(&glightning.Channel{ShortChannelId: scid, Source: "02self", Destination: peer,
			BaseFeeMillisatoshi: 45000}, 0, 0)
		f.Node.Graph.Channels[scid+"/"+util.GetDirection("02self", peer)] = dear
	}
	f.held = func(call int) bool { return call == 0 }
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	res := result.(*Result)
	assert.Equal(t, 2, res.FeeratioSkipped)
	assert.Equal(t, 1, res.FeeratioHeld, "2x1x1's split; 4x1x1's failed otherwise")
	assert.Equal(t, 2, len(f.asked))
	out, err := json.Marshal(res)
	assert.NoError(t, err)
	assert.Contains(t, string(out), `"feeratio_skipped":2,"feeratio_held":1`)

	// without a ratio: neither field
	f = splitsOn(t, 1, func(int, uint64) uint64 { return 0 })
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	out, err = json.Marshal(result)
	assert.NoError(t, err)
	assert.NotContains(t, string(out), "feeratio")
}

// The user's run: circular-pull into a channel that earned 1300 ppm, at
// feeratio 0.9 and maxppm 1600, returned at once saying "Cheapest route found
// was 1519 ppm, but maxppm is 1170", and nothing said that 1170 was the
// feeratio limit. Its last error and stopped now say so, and what to change.
func TestFeeratioHoldSaysWhy(t *testing.T) {
	r := stepPull(t, 0, stepWay{via: "02a", ppm1: 760, ppm2: 760})
	r.stepPPM, r.capPPM, r.probeMsat, r.retryEvery = 0, 0, 0, 0 // a plain run
	r.maxPPM, r.feeRatio = 1600, 0.9
	r.forwards = (&forwardsOf{ppm: map[string]uint64{"9x1x1": 1300}, sats: map[string]uint64{"9x1x1": 200000}}).read
	res := stepRun(t, r)
	assert.Zero(t, res.RebalancedAmount)
	assert.Equal(t, 1, res.FeeratioHeld)
	cheapest := res.CheapestPPMOverMax
	assert.InDelta(t, 1520, float64(cheapest), 2)
	assert.Contains(t, res.LastError, fmt.Sprintf("Cheapest route found was %d ppm, but this split's own limit is "+
		"1170 ppm (feeratio 0.9 × 1300 ppm earned by the filled channel − 0 ppm our fee on the drained one; "+
		"maxppm 1600)", cheapest))
	assert.Equal(t, fmt.Sprintf("no candidates left: 1 failed; feeratio held its splits to at most 1170 ppm, and the "+
		"cheapest route found cost %d ppm: raise feeratio, or pass feeratio=-1 to pay up to maxppm 1600", cheapest),
		res.Stopped)

	// a route over maxppm too: feeratio is not what held it
	r = stepPull(t, 0, stepWay{via: "02a", ppm1: 900, ppm2: 900})
	r.stepPPM, r.capPPM, r.probeMsat, r.retryEvery = 0, 0, 0, 0
	r.maxPPM, r.feeRatio = 1600, 0.9
	r.forwards = (&forwardsOf{ppm: map[string]uint64{"9x1x1": 1300}, sats: map[string]uint64{"9x1x1": 200000}}).read
	res = stepRun(t, r)
	assert.Zero(t, res.FeeratioHeld)
	assert.Equal(t, "no candidates left: 1 failed", res.Stopped)
}

func TestFeeratioHint(t *testing.T) {
	a := &AbstractRebalance{maxPPM: 1600, roundTopLimit: 1170, Result: &Result{FeeratioHeld: 2, CheapestPPMOverMax: 1519}}
	assert.Equal(t, "; feeratio held its splits to at most 1170 ppm, and the cheapest route found cost 1519 ppm: "+
		"raise feeratio, or pass feeratio=-1 to pay up to maxppm 1600", a.feeratioHint())
	a.Result.CheapestPPMOverMax = 0
	assert.Equal(t, "; feeratio held its splits to at most 1170 ppm: raise feeratio, or pass feeratio=-1 to pay up "+
		"to maxppm 1600", a.feeratioHint(), "no route found over the limit")
	a.Result.CheapestPPMOverMax = 1000
	assert.NotContains(t, a.feeratioHint(), "cheapest", "over a lower limit than the highest: it would not tell")
	a.Result.FeeratioHeld = 0
	assert.Empty(t, a.feeratioHint(), "no split held by its own limit")
	a.Result.FeeratioHeld, a.roundTopLimit = 2, 1600
	assert.Empty(t, a.feeratioHint(), "a split could pay up to maxppm")
}
