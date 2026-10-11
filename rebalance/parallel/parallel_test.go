package parallel

import (
	"circular/graph"
	"circular/node"
	"circular/rebalance"
	"circular/util"
	"encoding/json"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/gammazero/deque"
	"github.com/stretchr/testify/assert"
	"math"
	"sync"
	"testing"
	"time"
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
	return AbstractRebalance{Node: &node.Node{PeersLock: &sync.RWMutex{}}, splitAmount: 100000000, maxPPM: 100}
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

// Successes were keyed by alias only, so channels to the same peer, or peers
// sharing an alias, were merged, and the node id was missing.
func TestSuccessesAreKeyedByChannel(t *testing.T) {
	g := graph.NewGraph()
	g.Aliases["02bcash"] = "BCash_Is_Trash"
	route := func(scids ...string) *graph.PrettyRoute {
		r := &graph.PrettyRoute{}
		for _, scid := range scids {
			r.Hops = append(r.Hops, graph.PrettyRouteHop{ShortChannelId: scid})
		}
		return r
	}
	success := func(out, in string, ppm, amount uint64, r *graph.PrettyRoute) *rebalance.Result {
		return &rebalance.Result{Status: "success", Out: out, In: in, PPM: ppm, Amount: amount, Route: r}
	}

	pull := &RebalancePull{AbstractRebalance: AbstractRebalance{Node: &node.Node{Graph: g}, Result: NewResult(2926336000)}}
	pull.AddSuccess(success("02bcash", "02in", 756, 600000, route("1x1x1", "2x2x2", "9x9x9")))
	pull.AddSuccess(success("02bcash", "02in", 756, 40136, route("1x1x1", "3x3x3", "9x9x9")))
	pull.AddSuccess(success("02bcash", "02in", 12, 1000, route("4x4x4", "3x3x3", "9x9x9"))) // another channel to the same peer
	pull.AddSuccess(success("02noalias", "02in", 30, 5, route("5x5x5", "9x9x9")))

	out, err := json.Marshal(pull.Result)
	assert.NoError(t, err)
	assert.JSONEq(t, `{
		"rebalance_target": 2926336,
		"rebalanced_amount": 641141,
		"attempts": 0,
		"time": "",
		"successes": {
			"1x1x1": {"alias": "BCash_Is_Trash", "node_id": "02bcash", "756": 640136},
			"4x4x4": {"alias": "BCash_Is_Trash", "node_id": "02bcash", "12": 1000},
			"5x5x5": {"node_id": "02noalias", "30": 5}
		}
	}`, string(out))
	assert.Contains(t, string(out), `{"alias":"BCash_Is_Trash","node_id":"02bcash","756":640136}`, "alias and node id come first")

	// circular-push keys by the channel it fills: the route's last
	push := &RebalancePush{AbstractRebalance: AbstractRebalance{Node: &node.Node{Graph: g}, Result: NewResult(0)}}
	push.AddSuccess(success("02out", "02bcash", 5, 7, route("8x8x8", "2x2x2", "1x1x1")))
	push.AddSuccess(success("02out", "02bcash", 3, 1, route("8x8x8", "2x2x2", "1x1x1")))
	out, err = json.Marshal(push.Result.Successes)
	assert.NoError(t, err)
	assert.Equal(t, `{"1x1x1":{"alias":"BCash_Is_Trash","node_id":"02bcash","3":1,"5":7}}`, string(out))
}

// fakeSplits stands in for circular-pull: each split delivers what deliver
// gives it, at most what it asked for.
type fakeSplits struct {
	*AbstractRebalance
	deliver func(call int, amount uint64) uint64
	// failure says where call number call failed, and whether it may pass
	failure func(call int) (rebalance.Side, rebalance.Retry)
	calls   int
	asked   []uint64
	lock    sync.Mutex
}

func (f *fakeSplits) IsGoodCandidate(*glightning.PeerChannel) bool { return true }
func (f *fakeSplits) CanUseChannel(*glightning.PeerChannel) error  { return nil }
func (f *fakeSplits) GetCandidateDirection(string) string          { return "0" }
func (f *fakeSplits) AddSuccess(result *rebalance.Result) {
	f.AddSuccessGeneric(result.Route.Hops[0].ShortChannelId, result.Out, result.PPM, result.Amount)
}
func (f *fakeSplits) EnqueueCandidate(result *rebalance.Result) {
	c, _ := f.Node.Graph.GetChannel(result.Route.Hops[0].ShortChannelId + "/0")
	f.QueueLock.Lock()
	f.Candidates.PushFront(c)
	f.QueueLock.Unlock()
}
func (f *fakeSplits) Fire(candidate *graph.Channel, amount uint64) {
	f.lock.Lock()
	delivered := f.deliver(f.calls, amount)
	side, retry := rebalance.SideRoute, rebalance.RetryNo
	if f.failure != nil {
		side, retry = f.failure(f.calls)
	}
	f.calls++
	f.launched[candidateId(candidate)] = candidate
	f.asked = append(f.asked, amount)
	f.lock.Unlock()
	result := rebalance.NewResult("failure", amount/1000, candidate.Destination, "02target")
	result.Requested = amount
	result.Candidate = candidateId(candidate)
	result.FailedAt, result.Retry = side, retry
	if delivered > 0 {
		result = rebalance.NewResult("success", delivered/1000, candidate.Destination, "02target")
		result.AmountMsat, result.Requested, result.PPM = delivered, amount, 10
		result.Candidate, result.Fee = candidateId(candidate), delivered/100000
		result.Route = &graph.PrettyRoute{Hops: []graph.PrettyRouteHop{{ShortChannelId: candidate.ShortChannelId}}}
	}
	go func() { f.RebalanceResultChan <- result }()
}

func splitsOn(t *testing.T, candidates int, deliver func(int, uint64) uint64) *fakeSplits {
	g := graph.NewGraph()
	n := &node.Node{Id: "02self", Graph: g, PeersLock: &sync.RWMutex{}, Peers: map[string]*glightning.Peer{}}
	q := deque.New[*graph.Channel]()
	for i := 0; i < candidates; i++ {
		scid := fmt.Sprintf("%dx1x1", i+1)
		peer := fmt.Sprintf("02peer%d", i)
		c := graph.NewChannel(&glightning.Channel{Source: "02self", Destination: peer, ShortChannelId: scid,
			AmountMsat: glightning.AmountFromMSat(10000000000)}, 0, 0)
		g.AddChannel(c)
		g.Channels[scid+"/0"] = c
		n.Peers[peer] = &glightning.Peer{Id: peer, Channels: []*glightning.PeerChannel{{ShortChannelId: scid}}}
		q.PushBack(c)
	}
	a := &AbstractRebalance{Node: n, Candidates: q, AmountLock: &sync.Mutex{}, QueueLock: &sync.Mutex{},
		RebalanceResultChan: make(chan *rebalance.Result, 4), amount: 300000000, splitAmount: 100000000, splits: 2,
		routes: map[string][]string{}, candidateSide: rebalance.SideOut,
		launched: map[string]*graph.Channel{}, retried: map[string]bool{}}
	f := &fakeSplits{AbstractRebalance: a, deliver: deliver}
	a.RebalanceMethods = f
	return f
}

// Splits that deliver less than they ask for are counted as what they
// delivered, and the next splits ask for what is left.
func TestPartialSplitsAreCountedByWhatTheyDelivered(t *testing.T) {
	f := splitsOn(t, 2, func(call int, amount uint64) uint64 {
		if call == 0 {
			return amount / 2 // the first split is retried with half
		}
		return amount
	})
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	res := result.(*Result)
	assert.Equal(t, uint64(300000), res.RebalancedAmount)
	assert.Equal(t, uint64(300000000), f.AmountRebalanced)
	assert.Equal(t, uint64(0), f.InFlightAmount)
	assert.Equal(t, 0, f.splitsInFlight)
	var asked uint64
	for _, a := range f.asked {
		asked += a
		assert.LessOrEqual(t, a, uint64(100000000))
	}
	assert.Equal(t, uint64(350000000), asked, "the 50k it did not deliver was asked for again")
	assert.Equal(t, uint64(50000000), f.asked[len(f.asked)-1], "the last split asks only for what is left")
}

func TestFailedSplitsStopWhenCandidatesRunOut(t *testing.T) {
	f := splitsOn(t, 2, func(int, uint64) uint64 { return 0 })
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), result.(*Result).RebalancedAmount)
	assert.Equal(t, 2, len(f.asked))
	assert.Equal(t, 0, f.splitsInFlight)
}

func TestMinSplitDefaults(t *testing.T) {
	r := &AbstractRebalance{amount: 400000, splitAmount: 100000}
	r.setGenericDefaults()
	assert.Equal(t, uint64(25000000), r.minSplit)
	r = &AbstractRebalance{amount: 400000, splitAmount: 100000, minSplit: 200000}
	r.setGenericDefaults()
	assert.Equal(t, r.splitAmount, r.minSplit, "above the split amount: no retries with less")
}

// Splits record how their candidate did, and the next run tries the
// candidates that did best first.
func TestCandidatesAreRankedByEarlierSplits(t *testing.T) {
	f := splitsOn(t, 3, func(call int, amount uint64) uint64 {
		if call == 0 {
			return 0 // the first candidate fails
		}
		return amount
	})
	f.FireCandidates()
	_, err := f.WaitForResult()
	assert.NoError(t, err)
	assert.NoError(t, f.FindCandidates("02target"))
	var order []string
	for f.Candidates.Len() > 0 {
		order = append(order, f.Candidates.PopFront().ShortChannelId)
	}
	assert.Equal(t, "1x1x1", order[len(order)-1], "the candidate that just failed comes last")
	assert.Len(t, order, 3)
}

// A target channel that refused for good, or with nothing else in flight,
// made every following candidate fail the same way.
func TestTargetThatTakesNoMoreStopsTheRun(t *testing.T) {
	f := splitsOn(t, 3, func(int, uint64) uint64 { return 0 })
	f.splits = 1
	f.failure = func(int) (rebalance.Side, rebalance.Retry) { return rebalance.SideIn, rebalance.RetryWhenFree }
	f.FireCandidates()
	_, _ = f.WaitForResult()
	assert.Equal(t, 1, len(f.asked), "nothing else was in flight: the target is full")
	assert.Equal(t, uint64(0), f.Node.CandidateRecord(candidateId(f.launched[firstLaunched(f)])).Failures)
}

// A policy read again after a stale-fee failure gives the candidate one more
// try, at once, counted against nobody.
func TestStalePolicyGivesOneMoreTry(t *testing.T) {
	f := splitsOn(t, 1, func(call int, amount uint64) uint64 {
		if call == 0 {
			return 0
		}
		return amount
	})
	f.failure = func(int) (rebalance.Side, rebalance.Retry) { return rebalance.SideIn, rebalance.RetryNow }
	f.FireCandidates()
	result, _ := f.WaitForResult()
	assert.Equal(t, uint64(300000), result.(*Result).RebalancedAmount)
}

func firstLaunched(f *fakeSplits) string {
	for id := range f.launched {
		return id
	}
	return ""
}

// busyTarget stands in for circular-push through a target channel that takes
// at most capacity splits at a time: lightningd refuses more at once, while
// accepted splits take a while. Each candidate can take one split.
type busyTarget struct {
	*fakeSplits
	capacity int
	accepted int
	used     map[string]bool
}

func (b *busyTarget) EnqueueCandidate(*rebalance.Result) {} // each candidate takes one split

func (b *busyTarget) Fire(candidate *graph.Channel, amount uint64) {
	b.lock.Lock()
	b.asked = append(b.asked, amount)
	b.launched[candidateId(candidate)] = candidate
	accept := b.accepted < b.capacity
	if accept {
		b.accepted++
	}
	b.lock.Unlock()

	result := rebalance.NewResult("failure", amount/1000, candidate.Destination, "02target")
	result.Requested, result.Candidate = amount, candidateId(candidate)
	if !accept {
		result.FailedAt, result.Retry = rebalance.SideIn, rebalance.RetryWhenFree
		go func() { b.RebalanceResultChan <- result }()
		return
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		b.lock.Lock()
		b.accepted--
		b.lock.Unlock()
		ok := rebalance.NewResult("success", amount/1000, candidate.Destination, "02target")
		ok.AmountMsat, ok.Requested, ok.Candidate, ok.PPM = amount, amount, candidateId(candidate), 10
		ok.Route = &graph.PrettyRoute{Hops: []graph.PrettyRouteHop{{ShortChannelId: candidate.ShortChannelId}}}
		b.RebalanceResultChan <- ok
	}()
}

// With 4 splits but a target that takes 2 at a time, the refused splits were
// retried at once, refused again and dropped: 200k of 600k moved.
func TestBusyTargetLimitsTheSplitsInFlight(t *testing.T) {
	b := &busyTarget{fakeSplits: splitsOn(t, 6, nil), capacity: 2}
	b.RebalanceMethods = b
	b.amount, b.splits = 600000000, 4
	b.RebalanceResultChan = make(chan *rebalance.Result, 4)

	b.FireCandidates()
	result, err := b.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, uint64(600000), result.(*Result).RebalancedAmount, "every candidate got its split")
	assert.Equal(t, 2, b.targetCap)
	assert.LessOrEqual(t, len(b.asked), 8, "at most the first round over the cap is refused")
}

func TestOnlyTheCandidatesOwnFailuresCount(t *testing.T) {
	for _, c := range []struct {
		side    rebalance.Side
		counted bool
	}{
		{rebalance.SideOut, true},   // the candidate's channel (circular-pull)
		{rebalance.SideRoute, true}, // no route from it
		{rebalance.SideIn, false},   // the target
		{rebalance.SideNone, false}, // circular-stop, a timeout
	} {
		f := splitsOn(t, 1, func(int, uint64) uint64 { return 0 })
		side := c.side
		f.failure = func(int) (rebalance.Side, rebalance.Retry) { return side, rebalance.RetryNo }
		f.FireCandidates()
		_, _ = f.WaitForResult()
		assert.Equal(t, 1, len(f.asked), "not retried: the failure does not pass")
		failures := f.Node.CandidateRecord("1x1x1/" + util.GetDirection("02self", "02peer0")).Failures
		if c.counted {
			assert.Equal(t, uint64(1), failures, "side %d", c.side)
		} else {
			assert.Equal(t, uint64(0), failures, "side %d", c.side)
		}
	}
}
