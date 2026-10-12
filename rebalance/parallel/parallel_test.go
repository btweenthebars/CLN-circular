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
	// successDelay is how long a successful split takes; failures are at once
	successDelay time.Duration
	// precheck: failures are our copy of a channel's balance found too low
	precheck bool
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
	result.Precheck = f.precheck
	if delivered > 0 {
		result = rebalance.NewResult("success", delivered/1000, candidate.Destination, "02target")
		result.AmountMsat, result.Requested, result.PPM = delivered, amount, 10
		result.Candidate, result.Fee = candidateId(candidate), delivered/100000
		result.Route = &graph.PrettyRoute{Hops: []graph.PrettyRouteHop{{ShortChannelId: candidate.ShortChannelId}}}
	}
	go func() {
		if delivered > 0 {
			time.Sleep(f.successDelay)
		}
		f.RebalanceResultChan <- result
	}()
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

// Splits sending less over a route that was short (minsplit splitamount/4)
// filled live logs with halved payments; it is off unless minsplit is set.
func TestMinSplitDefaults(t *testing.T) {
	r := &AbstractRebalance{amount: 400000, splitAmount: 100000}
	r.setGenericDefaults()
	assert.Equal(t, r.splitAmount, r.minSplit, "by default a split never sends less")
	r = &AbstractRebalance{amount: 400000, splitAmount: 100000, minSplit: 25000}
	r.setGenericDefaults()
	assert.Equal(t, uint64(25000000), r.minSplit, "asked for")
	r = &AbstractRebalance{amount: 400000, splitAmount: 100000, minSplit: 200000}
	r.setGenericDefaults()
	assert.Equal(t, r.splitAmount, r.minSplit, "above the split amount: never sends less")
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
	full := func(precheck bool, refresh func(...string) error) *fakeSplits {
		f := splitsOn(t, 3, func(int, uint64) uint64 { return 0 })
		f.splits = 1
		f.precheck = precheck
		f.refresh = refresh
		f.failure = func(int) (rebalance.Side, rebalance.Retry) { return rebalance.SideIn, rebalance.RetryWhenFree }
		f.FireCandidates()
		_, _ = f.WaitForResult()
		assert.True(t, f.targetStopped)
		return f
	}
	var refreshed [][]string
	record := func(peerIds ...string) error {
		refreshed = append(refreshed, peerIds)
		return nil
	}

	// our copy of its balance was too low with nothing else in flight: read
	// again and tried once more; refused again, the candidate is left out
	// (the target read again for the next one), and when the next one is
	// refused too the target is full
	f := full(true, record)
	assert.Equal(t, 3, len(f.asked))
	assert.Equal(t, [][]string{{"02target"}, {"02target"}}, refreshed)
	assert.Equal(t, uint64(0), f.Node.CandidateRecord(candidateId(f.launched[firstLaunched(f)])).Failures)

	// lightningd or the peer refused: a new read would not change that
	refreshed = nil
	f = full(false, record)
	assert.Equal(t, 2, len(f.asked))
	assert.Empty(t, refreshed)

	// the target cannot be read again
	f = full(true, func(...string) error { return fmt.Errorf("lightningd is gone") })
	assert.Equal(t, 1, len(f.asked))

	// refused for good
	f = splitsOn(t, 3, func(int, uint64) uint64 { return 0 })
	f.splits = 1
	f.refresh = func(...string) error { return nil }
	f.failure = func(int) (rebalance.Side, rebalance.Retry) { return rebalance.SideIn, rebalance.RetryNo }
	f.FireCandidates()
	_, _ = f.WaitForResult()
	assert.Equal(t, 1, len(f.asked))
}

// The balances a depleted-target refusal is based on are re-read only after
// a success, which can be while other splits hold the target: once they
// failed, the run stopped on balances that were no longer true.
func TestTargetIsReadAgainBeforeTheRunStops(t *testing.T) {
	f := splitsOn(t, 3, func(call int, amount uint64) uint64 {
		if call == 1 {
			return 0
		}
		return amount
	})
	f.splits = 1
	f.precheck = true
	refreshes := 0
	f.refresh = func(peerIds ...string) error {
		if len(peerIds) == 1 {
			refreshes++
		}
		return nil
	}
	f.failure = func(int) (rebalance.Side, rebalance.Retry) { return rebalance.SideIn, rebalance.RetryWhenFree }
	f.FireCandidates()
	result, _ := f.WaitForResult()
	assert.Equal(t, uint64(300000), result.(*Result).RebalancedAmount, "the target had room after all")
	assert.Equal(t, 1, refreshes)
	assert.False(t, f.targetStopped)
}

// A stale-fee failure whose re-read changed nothing came back as RetryNow
// again; the candidate, already retried, was dropped without a word, and the
// run never stopped: every candidate sent two payments into the same refusal.
func TestRepeatedStalePolicyIsJudgedAsAFailure(t *testing.T) {
	f := splitsOn(t, 3, func(int, uint64) uint64 { return 0 })
	f.splits = 1
	f.refresh = func(...string) error { return nil }
	f.failure = func(int) (rebalance.Side, rebalance.Retry) { return rebalance.SideIn, rebalance.RetryNow }
	f.FireCandidates()
	_, _ = f.WaitForResult()
	assert.Equal(t, 2, len(f.asked), "one more try, then the target's failure stops the run")
	assert.True(t, f.targetStopped)

	// on the candidate's own side, the second one counts against it
	f = splitsOn(t, 2, func(int, uint64) uint64 { return 0 })
	f.splits = 1
	f.failure = func(int) (rebalance.Side, rebalance.Retry) { return rebalance.SideOut, rebalance.RetryNow }
	f.FireCandidates()
	_, _ = f.WaitForResult()
	assert.Equal(t, 4, len(f.asked))
	for _, peer := range []string{"02peer0", "02peer1"} {
		scid := map[string]string{"02peer0": "1x1x1", "02peer1": "2x1x1"}[peer]
		assert.Equal(t, uint64(1), f.Node.CandidateRecord(scid+"/"+util.GetDirection("02self", peer)).Failures, peer)
	}
}

// One refusal by the target lowered how many splits it was given at once for
// the rest of the run, though in circular-pull our other splits rarely cause
// it.
func TestTargetCapGrowsBackWithSuccesses(t *testing.T) {
	f := splitsOn(t, 3, func(call int, amount uint64) uint64 {
		if call == 1 {
			return 0
		}
		return amount
	})
	f.refresh = func(...string) error { return nil }
	f.failure = func(int) (rebalance.Side, rebalance.Retry) { return rebalance.SideIn, rebalance.RetryWhenFree }
	f.successDelay = 20 * time.Millisecond // the refusal comes back while the first split is in flight
	f.FireCandidates()
	result, _ := f.WaitForResult()
	assert.Equal(t, uint64(300000), result.(*Result).RebalancedAmount)
	assert.Equal(t, 2, f.targetCap, "back to the splits allowed")
	assert.Equal(t, 4, len(f.asked), "one refusal")
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

	b.refresh = func(...string) error { return nil }
	b.FireCandidates()
	result, err := b.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, uint64(600000), result.(*Result).RebalancedAmount, "every candidate got its split")
	assert.GreaterOrEqual(t, b.targetCap, 2)
	assert.LessOrEqual(t, b.targetCap, 3)
	// the first round over the cap, then one probe each time the cap grows
	// back after 2 successes
	assert.LessOrEqual(t, len(b.asked)-6, 2+3, "refused splits")
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

// judge fires a split for candidate c with n-1 others in flight, and feeds
// judgeCandidate its result. It reports whether c went back in the queue.
func judge(f *fakeSplits, c *graph.Channel, n int, result *rebalance.Result) bool {
	f.splitsInFlight = n - 1
	f.noteFired(c)
	return judgeFired(f, c, n, f.firedAt[candidateId(c)], result)
}

// judgeFired feeds judgeCandidate the result of the seq-th split fired, for
// candidate c, with n splits in flight, this one included.
func judgeFired(f *fakeSplits, c *graph.Channel, n, seq int, result *rebalance.Result) bool {
	f.Candidates.Clear()
	f.launched[candidateId(c)] = c
	if f.firedAt == nil {
		f.firedAt = map[string]int{}
	}
	f.firedAt[candidateId(c)] = seq
	result.Candidate = candidateId(c)
	f.splitsInFlight = n
	f.judgeCandidate(result)
	return f.Candidates.Len() == 1
}

func busy() *rebalance.Result {
	return &rebalance.Result{Status: "failure", FailedAt: rebalance.SideIn, Retry: rebalance.RetryWhenFree}
}

func success() *rebalance.Result {
	return &rebalance.Result{Status: "success", AmountMsat: 1000}
}

// Every growth of the target's limit tried one more split at once: with a
// target that takes 2, a third of all splits were refused.
func TestTargetCapWaitsLongerAfterARefusedGrowth(t *testing.T) {
	f := splitsOn(t, 2, nil)
	f.splits = 4
	c := f.Candidates.At(0)
	judge(f, c, 3, busy()) // 2 others in flight
	assert.Equal(t, 2, f.targetCap)

	growsAfter := func() int {
		cap := f.targetCap
		for n := 1; n <= 1000; n++ {
			judge(f, c, 2, success())
			if f.targetCap > cap {
				return n
			}
		}
		return -1
	}
	assert.Equal(t, 2, growsAfter(), "as many successes as the limit")
	judge(f, c, 3, busy()) // the third at once was refused
	assert.Equal(t, 2, f.targetCap)
	assert.Equal(t, 4, growsAfter(), "twice as many")
	judge(f, c, 3, busy())
	assert.Equal(t, 8, growsAfter())

	// one more at once held: half the wait
	assert.Equal(t, 3, f.targetCap)
	judge(f, c, 3, success()) // the split fired up to 3 in flight
	assert.Equal(t, 2, f.capBackoff)
	assert.Equal(t, 3*2-1, growsAfter())
	judge(f, c, 4, success())
	assert.Equal(t, 1, f.capBackoff)

	// it never goes above the splits allowed
	f.targetCap = 4
	for n := 0; n < 100; n++ {
		judge(f, c, 2, success())
	}
	assert.Equal(t, 4, f.targetCap)
}

// A growth was judged by the successes of any split, and the limit could
// grow again before the refusal of the split that tried it came back: when
// refusals take longer than successes, the backoff never engaged and the
// limit cycled above what the target takes.
func TestAGrowthIsJudgedByTheSplitThatTriesIt(t *testing.T) {
	f := splitsOn(t, 2, nil)
	f.splits = 4
	c, d := f.Candidates.At(0), f.Candidates.At(1)
	f.targetCap, f.capBackoff = 2, 4
	for f.targetCap == 2 {
		judge(f, c, 2, success())
	}
	assert.True(t, f.probing)

	// the split fired up to 3 in flight tries the growth
	f.splitsInFlight = 1
	f.noteFired(c)
	f.splitsInFlight = 2
	f.noteFired(d)
	probe := f.firedAt[candidateId(d)]
	assert.Equal(t, probe, f.probeSeq)

	// splits fired before it succeed, many times over: no growth, no change
	for i := 0; i < 20; i++ {
		judgeFired(f, c, 3, probe-1, success())
	}
	assert.Equal(t, 3, f.targetCap)
	assert.Equal(t, 4, f.capBackoff)

	// then it is refused: twice the wait
	judgeFired(f, d, 3, probe, busy())
	assert.Equal(t, 2, f.targetCap)
	assert.Equal(t, 8, f.capBackoff)
	assert.False(t, f.probing)
	assert.Empty(t, f.busyRefusals, "the growth explains it")

	// a probe that fails on the route tells nothing: the next split fired up
	// to the limit tries the growth instead
	f.capBackoff = 1
	for f.targetCap == 2 {
		judge(f, c, 2, success())
	}
	f.splitsInFlight = 2
	f.noteFired(d)
	route := &rebalance.Result{Status: "failure", FailedAt: rebalance.SideRoute}
	judgeFired(f, d, 3, f.probeSeq, route)
	assert.True(t, f.probing)
	assert.Equal(t, 0, f.probeSeq)
	f.splitsInFlight = 2
	f.noteFired(c)
	assert.Equal(t, f.firedAt[candidateId(c)], f.probeSeq)

	// so does one retried after a stale fee: the growth is not stuck
	judgeFired(f, c, 3, f.probeSeq, &rebalance.Result{Status: "failure", FailedAt: rebalance.SideIn, Retry: rebalance.RetryNow})
	assert.Equal(t, 0, f.probeSeq)
	judge(f, c, 3, success())
	assert.False(t, f.probing, "held")
}

// A candidate the target refused as busy came back after every growth of
// the limit, for the whole run, though the target may refuse it alone (for
// the fees its route adds to the HTLC, say). Refusals the target explains
// do not count: those in the first round, and those of one more at once.
func TestCandidateOftenRefusedAsBusyGoesToTheBack(t *testing.T) {
	f := splitsOn(t, 2, nil)
	f.splits = 4
	c, d := f.Candidates.At(0), f.Candidates.At(1)
	// refuse has the target refuse c, the seq-th split fired, with inFlight
	// splits in flight, when its limit was cap as of the capSetAt-th split
	refuse := func(cap, capSetAt int, grown bool, seq, inFlight int) bool {
		f.Candidates.Clear()
		f.Candidates.PushBack(d)
		f.launched[candidateId(c)] = c
		f.targetCap, f.capSetAt, f.probing, f.fireSeq = cap, capSetAt, grown, seq
		f.firedAt = map[string]int{candidateId(c): seq}
		f.splitsInFlight = inFlight
		result := busy()
		result.Candidate = candidateId(c)
		f.judgeCandidate(result)
		return f.Candidates.Front() == c // back in front of the others
	}
	assert.True(t, refuse(0, 0, false, 4, 4), "the first refusal of the first round")
	for i := 0; i < 10; i++ {
		assert.True(t, refuse(3, 4, false, 3, 3), "a later refusal of the first round")
		assert.True(t, refuse(3, 10, true, 11, 3), "one more at once")
		assert.True(t, refuse(1, 10, false, 8, 3), "fired before the limit was lowered")
	}
	assert.Empty(t, f.busyRefusals)
	for i := 1; i <= 3; i++ {
		assert.True(t, refuse(2, 10, false, 11, 2), "refusal %d: the target had been taking 2", i)
	}
	assert.False(t, refuse(2, 10, false, 11, 2), "the others first")
	assert.Equal(t, 2, f.Candidates.Len(), "not dropped")
	assert.False(t, f.targetStopped)
	assert.Equal(t, uint64(0), f.Node.CandidateRecord(candidateId(c)).Failures, "not counted against it")
}

// roomyTarget stands in for a run whose target takes capacity splits at a
// time: refused splits come back at once, accepted ones after a while. Each
// candidate can take room[scid] splits (any number if absent); the target
// always refuses those in refused, and the route fails for those in
// routeFails.
type roomyTarget struct {
	*fakeSplits
	capacity   int
	accepted   int
	room       map[string]int
	refused    map[string]bool
	routeFails map[string]bool
	// how long an accepted split takes (20ms if 0), and a refused one
	successTime, refusalTime time.Duration
}

func (b *roomyTarget) CanUseChannel(c *glightning.PeerChannel) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if n, ok := b.room[c.ShortChannelId]; ok && n <= 0 {
		return util.ErrChannelDepleted
	}
	return nil
}

func (b *roomyTarget) Fire(candidate *graph.Channel, amount uint64) {
	scid := candidate.ShortChannelId
	b.lock.Lock()
	b.asked = append(b.asked, amount)
	b.launched[candidateId(candidate)] = candidate
	refuse := b.refused[scid] || b.accepted >= b.capacity
	route := !refuse && b.routeFails[scid]
	if !refuse && !route {
		b.accepted++
	}
	b.lock.Unlock()

	result := rebalance.NewResult("failure", amount/1000, candidate.Destination, "02target")
	result.Requested, result.Candidate = amount, candidateId(candidate)
	switch {
	case refuse:
		result.FailedAt, result.Retry = rebalance.SideIn, rebalance.RetryWhenFree
		go func() {
			time.Sleep(b.refusalTime)
			b.RebalanceResultChan <- result
		}()
	case route:
		result.FailedAt, result.Retry = rebalance.SideRoute, rebalance.RetryNo
		go func() { b.RebalanceResultChan <- result }()
	default:
		go func() {
			if b.successTime == 0 {
				b.successTime = 20 * time.Millisecond
			}
			time.Sleep(b.successTime)
			b.lock.Lock()
			b.accepted--
			if n, ok := b.room[scid]; ok {
				b.room[scid] = n - 1
			}
			b.lock.Unlock()
			ok := rebalance.NewResult("success", amount/1000, candidate.Destination, "02target")
			ok.AmountMsat, ok.Requested, ok.Candidate, ok.PPM = amount, amount, candidateId(candidate), 10
			ok.Route = &graph.PrettyRoute{Hops: []graph.PrettyRouteHop{{ShortChannelId: scid}}}
			b.RebalanceResultChan <- ok
		}()
	}
}

func roomy(t *testing.T, candidates int, amount uint64, splits, capacity int) *roomyTarget {
	b := &roomyTarget{fakeSplits: splitsOn(t, candidates, nil), capacity: capacity,
		room: map[string]int{}, refused: map[string]bool{}, routeFails: map[string]bool{}}
	b.RebalanceMethods = b
	b.amount, b.splits = amount, splits
	b.RebalanceResultChan = make(chan *rebalance.Result, splits)
	b.refresh = func(...string) error { return nil }
	return b
}

// The busy limit counted the refusals of the first round and of one more at
// once against the candidate fired second, and dropped it: once the other
// candidate was full, the run ended at half.
func TestTargetsOwnRefusalsDoNotDropACandidate(t *testing.T) {
	b := roomy(t, 2, 1600000000, 2, 1) // 16 splits wanted
	b.room["1x1x1"], b.room["2x1x1"] = 8, 8
	b.FireCandidates()
	result, err := b.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1600000), result.(*Result).RebalancedAmount)
}

// A candidate the target refuses alone was fired alone, refused, read again,
// refused again, and the run stopped though other candidates fit.
func TestCandidateRefusedAloneDoesNotStopTheRun(t *testing.T) {
	for _, splits := range []int{1, 2} {
		b := roomy(t, 4, 1200000000, splits, 2)
		b.refused["1x1x1"] = true    // its route's fees make the HTLC too big
		b.routeFails["2x1x1"] = true // no route from it
		b.FireCandidates()
		result, err := b.WaitForResult()
		assert.NoError(t, err)
		assert.Equal(t, uint64(1200000), result.(*Result).RebalancedAmount, "%d splits", splits)
		assert.False(t, b.targetStopped)
	}
}

// A target that takes 2 splits at a time, over a long run.
func TestBusyTargetIsNotProbedAfterEverySuccess(t *testing.T) {
	b := &busyTarget{fakeSplits: splitsOn(t, 40, nil), capacity: 2}
	b.RebalanceMethods = b
	b.amount, b.splits = 4000000000, 4 // 40 splits of 100k sats
	b.RebalanceResultChan = make(chan *rebalance.Result, 4)
	b.refresh = func(...string) error { return nil }
	b.FireCandidates()
	result, err := b.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4000000), result.(*Result).RebalancedAmount)
	// the first round over the limit, then one refusal per growth, after 2,
	// 4, 8 then 16 successes; growing after every 2 refused about 20
	assert.LessOrEqual(t, len(b.asked)-40, 2+5, "refused splits")
}

// circular-push left out candidates whose peer charges more than maxppm for
// its channel to us, though its inbound discounts made it fit.
func TestPushCandidateFeeCountsInboundDiscounts(t *testing.T) {
	g := graph.NewGraph()
	in := graph.NewChannel(&glightning.Channel{Source: "02peer", Destination: "02self", ShortChannelId: "1x1x1",
		FeePerMillionth: 150, AmountMsat: glightning.AmountFromMSat(10000000000), IsActive: true}, 0, 0)
	g.AddChannel(in)
	g.Channels["1x1x1/"+util.GetDirection("02peer", "02self")] = in
	before := graph.NewChannel(&glightning.Channel{Source: "02a", Destination: "02peer", ShortChannelId: "2x1x1",
		AmountMsat: glightning.AmountFromMSat(10000000000), IsActive: true}, 0, 0)
	g.AddChannel(before)
	g.Channels["2x1x1/"+util.GetDirection("02a", "02peer")] = before

	ours := graph.NewChannel(&glightning.Channel{Source: "02self", Destination: "02peer", ShortChannelId: "1x1x1",
		AmountMsat: glightning.AmountFromMSat(10000000000), IsActive: true}, 0, 0)
	g.AddChannel(ours)
	g.Channels["1x1x1/"+util.GetDirection("02self", "02peer")] = ours

	r := &RebalancePush{AbstractRebalance: abstract()}
	r.Node.Graph = g
	r.TargetChannel = graph.NewChannel(&glightning.Channel{Source: "02self", Destination: "02target", ShortChannelId: "9x1x1"}, 0, 0)
	assert.False(t, r.inPeerFeeFits(in), "150 ppm, maxppm 100")
	// a discount on our own channel to the peer: no route of ours arrives on it
	g.SetInboundFee("1x1x1/"+util.GetDirection("02self", "02peer"), 0, -100)
	assert.False(t, r.inPeerFeeFits(in))
	g.SetInboundFee("2x1x1/"+util.GetDirection("02a", "02peer"), 0, -100)
	assert.True(t, r.inPeerFeeFits(in), "about 50 ppm after the discount")
}

// In circular-pull a refusal at the last hop can take longer than a success:
// the limit grew again before the refusal of the split that tried the last
// growth came back, and a third of the splits were refused.
func TestSlowRefusalsStillBackOff(t *testing.T) {
	for _, slower := range []time.Duration{1, 3, 5} {
		b := roomy(t, 4, 15000000000, 4, 2) // 150 splits
		b.successTime, b.refusalTime = 4*time.Millisecond, slower*4*time.Millisecond
		b.FireCandidates()
		result, err := b.WaitForResult()
		assert.NoError(t, err)
		assert.Equal(t, uint64(15000000), result.(*Result).RebalancedAmount)
		// 7 or 8 at any speed; it was 37 when 3 times slower
		assert.LessOrEqual(t, len(b.asked)-150, 12, "refused splits, refusals %dx slower", slower)
	}
}
