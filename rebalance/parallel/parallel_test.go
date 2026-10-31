package parallel

import (
	"bytes"
	"circular/graph"
	"circular/node"
	"circular/rebalance"
	"circular/util"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"github.com/gammazero/deque"
	"github.com/stretchr/testify/assert"
	"log"
	"math"
	"os"
	"strings"
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
		"fees_paid_msat": 0,
		"avg_ppm": 0,
		"splits_succeeded": 0,
		"splits_failed": 0,
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

// A success said which channel moved how much at which rate, but not which
// nodes its routes went through.
func TestSuccessesListTheNodesTheyWentThrough(t *testing.T) {
	g := graph.NewGraph()
	g.Aliases["02bcash"] = "BCash_Is_Trash"
	// hops name the node each leaves from: us, the out-peer, ..., the in-peer
	route := func(nodes ...string) *graph.PrettyRoute {
		r := &graph.PrettyRoute{}
		for i, n := range nodes {
			r.Hops = append(r.Hops, graph.PrettyRouteHop{Id: n, ShortChannelId: fmt.Sprintf("%dx1x1", i+1)})
		}
		return r
	}
	success := func(out, in string, ppm uint64, r *graph.PrettyRoute) *rebalance.Result {
		return &rebalance.Result{Status: "success", Out: out, In: in, PPM: ppm, Amount: 100, Route: r}
	}

	pull := &RebalancePull{AbstractRebalance: AbstractRebalance{Node: &node.Node{Id: "02self", Graph: g}, Result: NewResult(0)}}
	pull.AddSuccess(success("02bcash", "02in", 229, route("02self", "02bcash", "02a", "02b", "02in")))
	pull.AddSuccess(success("02bcash", "02in", 250, route("02self", "02bcash", "02c", "02a", "02in")))
	out, err := json.Marshal(pull.Result.Successes)
	assert.NoError(t, err)
	assert.Equal(t, `{"1x1x1":{"alias":"BCash_Is_Trash","node_id":"02bcash","229":100,"250":100,`+
		`"involving":["02a","02b","02in","02c"]}}`, string(out),
		"after the rates; each node once, in the order first seen; not us nor the channel's peer")

	// circular-push: the candidate's peer is the in-peer
	push := &RebalancePush{AbstractRebalance: AbstractRebalance{Node: &node.Node{Id: "02self", Graph: g}, Result: NewResult(0)}}
	push.AddSuccess(success("02out", "02bcash", 5, route("02self", "02out", "02a", "02bcash")))
	out, err = json.Marshal(push.Result.Successes)
	assert.NoError(t, err)
	assert.Equal(t, `{"4x1x1":{"alias":"BCash_Is_Trash","node_id":"02bcash","5":100,"involving":["02out","02a"]}}`, string(out))
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
	// message is a failed split's message
	message string
	// payments is how many payments a failed split made
	payments uint64
	// halted says whether call number call was ended by the run's stop
	halted func(call int) bool
	// cheapest gives, for a call, the rate of the cheapest route over maxppm
	// (0: none); a successful call carries it too, which a run must ignore
	cheapest func(call int) uint64
	// fee gives a successful call's fee for what it delivered (default 10 ppm)
	fee func(call int, delivered uint64) uint64
	// failedNodes gives, for a call, the payments each node failed
	failedNodes func(call int) map[string]int
	// middle is how many payments of a failed split failed in the middle of
	// their routes
	middle int
	// failureDelay gives how long a failing call takes (default none)
	failureDelay func(call int) time.Duration
	// fired, if set, is called as each split is fired, before it counts as
	// in flight
	fired func()
	// held says whether a failing call failed on its own feeratio limit
	held func(call int) bool
	// ppm gives a successful call's fee rate (default 10)
	ppm func(call int) uint64
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
	if f.fired != nil {
		f.fired()
	}
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
	result.Message = f.message
	result.Attempts = f.payments
	if f.cheapest != nil {
		result.CheapestPPM = f.cheapest(f.calls - 1)
	}
	if f.held != nil {
		result.HeldByOwnLimit = f.held(f.calls - 1)
		result.OwnLimit = result.HeldByOwnLimit // a limit of its own, below the run's
	}
	if f.halted != nil && f.halted(f.calls-1) {
		result.Halted = true
		result.Message = "rebalance failed after 1 payments. the run stopped: no more payments for this split"
	}
	if f.failedNodes != nil {
		result.FailedNodes = f.failedNodes(f.calls - 1)
	}
	result.MiddleFailures = f.middle
	if delivered > 0 {
		cheapest, failed := result.CheapestPPM, result.FailedNodes
		result = rebalance.NewResult("success", delivered/1000, candidate.Destination, "02target")
		result.AmountMsat, result.Requested, result.PPM = delivered, amount, 10
		result.Candidate, result.Fee = candidateId(candidate), delivered/100000
		if f.fee != nil {
			result.Fee = f.fee(f.calls-1, delivered)
		}
		if f.ppm != nil {
			result.PPM = f.ppm(f.calls - 1)
		}
		result.CheapestPPM = cheapest
		result.FailedNodes = failed
		result.Route = &graph.PrettyRoute{Hops: []graph.PrettyRouteHop{{ShortChannelId: candidate.ShortChannelId}}}
	}
	delay := f.successDelay
	if delivered == 0 && f.failureDelay != nil {
		delay = f.failureDelay(f.calls - 1)
	} else if delivered == 0 {
		delay = 0
	}
	go func() {
		time.Sleep(delay)
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
		assert.Contains(t, f.Result.Stopped, "the target channel cannot take more", "the result says why")
		assert.True(t, f.halt.Load(), "the splits in flight are told to stop")
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
	assert.False(t, r.roundPriced)
	assert.False(t, r.inPeerFeeFits(in), "150 ppm, maxppm 100")
	assert.True(t, r.roundPriced, "left out for its price: a step up could make it a candidate")
	assert.True(t, r.roundLeftOut, "what it could pay is not known")
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

// last_error read as the run's failure, though it is the last failed split's.
func TestLastErrorNamesTheSplit(t *testing.T) {
	f := splitsOn(t, 2, func(int, uint64) uint64 { return 0 })
	f.message = "rebalance failed after 8 payments. no route"
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	lastError := result.(*Result).LastError
	assert.Regexp(t, `^last failed split, via [12]x1x1 \(02peer[01]\): rebalance failed after 8 payments\. no route$`, lastError)
}

// A run whose target nothing reached within maxppm went on through every
// candidate's attempts: 1,511 payments in 49 minutes, nothing moved.
func TestARunStopsAfterTooManyFailuresInARow(t *testing.T) {
	failing := func(int, uint64) uint64 { return 0 }
	run := func(limit int, deliver func(int, uint64) uint64) *fakeSplits {
		f := splitsOn(t, 20, deliver)
		f.amount = 2000000000 // room for all 20 candidates
		f.payments = 50
		f.maxFailStreak = limit
		f.FireCandidates()
		_, err := f.WaitForResult()
		assert.NoError(t, err)
		return f
	}

	f := run(0, failing)
	assert.Equal(t, 6+1, len(f.asked), "300 failed payments after 6 splits; the one in flight then finishes")
	assert.Contains(t, f.Result.Stopped, "300 payments failed in a row")
	assert.True(t, f.halt.Load(), "the splits in flight are told to stop")
	assert.Equal(t, uint64(7*50), f.Result.Attempts)

	f = run(-1, failing)
	assert.Equal(t, 20, len(f.asked), "below 0: never")
	assert.Equal(t, "no candidates left: 20 failed", f.Result.Stopped)
	assert.False(t, f.halt.Load())

	// a success ends the streak (every third split succeeds: whatever order
	// the two in flight come back in, at most 4 failures, 200 payments, fall
	// between two successes)
	f = run(0, func(call int, amount uint64) uint64 {
		if call%3 == 2 {
			return amount
		}
		return 0
	})
	assert.GreaterOrEqual(t, len(f.asked), 20, "every candidate, and those that succeeded again")
	assert.True(t, strings.HasPrefix(f.Result.Stopped, "no candidates left: "), f.Result.Stopped)

	f = run(100, failing)
	assert.Equal(t, 2+1, len(f.asked))
}

// Splits the run's stop ended overwrote last_error with "the run stopped",
// hiding the failure that mattered.
func TestHaltedSplitsKeepTheLastRealError(t *testing.T) {
	f := splitsOn(t, 2, func(int, uint64) uint64 { return 0 })
	f.splits = 1 // one at a time: the halted split comes back last
	f.message = "rebalance failed after 8 payments. route too expensive"
	f.halted = func(call int) bool { return call == 1 }
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	assert.Contains(t, result.(*Result).LastError, "route too expensive")
	assert.Greater(t, len(f.asked), 1)
	assert.Equal(t, len(f.asked)-1, result.(*Result).SplitsFailed, "the halted split is not a failure")
}

// A run's result listed what moved at each rate, but not what the run cost,
// how many splits failed, or what maxppm would have let the failed ones search.
func TestRunTotals(t *testing.T) {
	f := splitsOn(t, 3, func(call int, amount uint64) uint64 {
		switch call {
		case 0:
			return amount
		case 1:
			return amount / 2
		}
		return 0
	})
	f.splits = 1
	f.amount = 400000000
	f.fee = func(call int, delivered uint64) uint64 {
		if call == 0 {
			return delivered / 100000 // 10 ppm
		}
		return delivered / 10000 // 100 ppm
	}
	f.cheapest = func(call int) uint64 {
		switch call {
		case 0, 1:
			return 600 // successful splits: not theirs to say
		case 2:
			return 700
		}
		return 650
	}
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	res := result.(*Result)
	assert.Equal(t, uint64(150000), res.RebalancedAmount)
	assert.Equal(t, 2, res.SplitsSucceeded)
	assert.Equal(t, len(f.asked)-2, res.SplitsFailed, "every other split fired")
	assert.Greater(t, res.SplitsFailed, 1)
	assert.Equal(t, uint64(1000+5000), res.FeesPaidMsat, "100k sats at 10 ppm, 50k at 100")
	assert.Equal(t, uint64(40), res.AvgPPM, "weighted by what moved: 6,000 msat on 150k sats")
	assert.Equal(t, uint64(650), res.CheapestPPMOverMax)
}

func TestAvgPPM(t *testing.T) {
	assert.Equal(t, uint64(0), avgPPM(0, 0))
	assert.Equal(t, uint64(756), avgPPM(483943, 640136000))
	// 200 BTC in fees on 1,000 BTC: fee*1e6 does not fit in 64 bits
	assert.Equal(t, uint64(200000), avgPPM(20000000000000, 100000000000000))
}

// The result lists the nodes that failed the most payments, over every
// split, whether it succeeded, failed or was ended by the run's stop.
func TestWorstNodes(t *testing.T) {
	f := splitsOn(t, 3, func(call int, amount uint64) uint64 {
		if call == 0 {
			return amount
		}
		return 0
	})
	f.splits = 1
	f.Node.Graph.Aliases["02n1"], f.Node.Graph.Aliases["02n6"] = "One", "Six"
	f.halted = func(call int) bool { return call == 2 }
	f.failedNodes = func(call int) map[string]int {
		switch call {
		case 0: // succeeded after these
			return map[string]int{"02n1": 3, "02n2": 1, "02n7": 1}
		case 1:
			return map[string]int{"02n1": 2, "02n3": 2, "02n4": 2}
		case 2: // halted
			return map[string]int{"02n5": 2, "02n6": 4}
		}
		return nil
	}
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	worst := result.(*Result).WorstNodes
	ids := []string{}
	for _, n := range worst {
		ids = append(ids, n.NodeId)
	}
	assert.Equal(t, []string{"02n1", "02n6", "02n3", "02n4", "02n5"}, ids, "most first, then by id; at most 5")
	if assert.Equal(t, WORST_NODES, len(worst)) {
		assert.Equal(t, 5, worst[0].FailedPayments)
		assert.Equal(t, "One", worst[0].Alias)
		assert.Equal(t, "Six", worst[1].Alias)
		assert.Empty(t, worst[2].Alias)
	}
	out, err := json.Marshal(worst[0])
	assert.NoError(t, err)
	assert.Equal(t, `{"alias":"One","node_id":"02n1","failed_payments":5}`, string(out))

	// no failures, no list
	f = splitsOn(t, 1, func(_ int, amount uint64) uint64 { return amount })
	f.amount = 100000000
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	out, err = json.Marshal(result)
	assert.NoError(t, err)
	assert.NotContains(t, string(out), "worst_nodes")
}

// A run that fell short because no candidate was left said nothing in
// stopped: it now says how many failed, how many the target refused, and how
// many were skipped, and why.
func TestRunningOutOfCandidatesSaysWhy(t *testing.T) {
	f := splitsOn(t, 4, func(call int, amount uint64) uint64 {
		if call == 0 {
			return amount
		}
		return 0
	})
	f.splits = 1
	f.amount = 1000000000
	d := &depletedOne{fakeSplits: f, depleted: "4x1x1"}
	f.RebalanceMethods = d
	f.failure = func(call int) (rebalance.Side, rebalance.Retry) {
		if call == 2 {
			return rebalance.SideIn, rebalance.RetryWhenFree // the target refuses it alone
		}
		return rebalance.SideRoute, rebalance.RetryNo
	}
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	res := result.(*Result)
	assert.Equal(t, uint64(100000), res.RebalancedAmount)
	// 1x1x1 succeeds, then fails; the target refuses 2x1x1 alone; 3x1x1
	// fails; 4x1x1 is depleted
	assert.Equal(t, 4, len(f.asked))
	assert.Equal(t, "no candidates left: 2 failed, 1 refused by the target, 1 skipped (channel is depleted)", res.Stopped)

	// several reasons to skip, most first
	f = splitsOn(t, 3, func(int, uint64) uint64 { return 0 })
	why := map[string]error{"1x1x1": util.ErrChannelDepleted, "2x1x1": util.ErrOutgoingPeerDisconnected, "3x1x1": util.ErrChannelDepleted}
	f.RebalanceMethods = &unusable{fakeSplits: f, why: func(scid string) error { return why[scid] }}
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Empty(t, f.asked)
	assert.Equal(t, "no candidates left: 3 skipped (channel is depleted: 2, outgoing peer is disconnected: 1)",
		result.(*Result).Stopped)

	// a candidate tried again leaves for its own reason: failed (retry now),
	// then skipped
	f = splitsOn(t, 2, func(int, uint64) uint64 { return 0 })
	f.splits = 1
	checks := map[string]int{}
	f.RebalanceMethods = &unusable{fakeSplits: f, why: func(scid string) error {
		checks[scid]++
		if scid == "1x1x1" && checks[scid] > 1 {
			return util.ErrChannelDepleted
		}
		return nil
	}}
	f.failure = func(call int) (rebalance.Side, rebalance.Retry) {
		if call == 0 {
			return rebalance.SideOut, rebalance.RetryNow // a stale fee on our channel
		}
		return rebalance.SideNone, rebalance.RetryNo // a timeout, say
	}
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(f.asked))
	assert.Equal(t, "no candidates left: 1 failed, 1 skipped (channel is depleted)", result.(*Result).Stopped)

	// in a push the target is the out side
	f = splitsOn(t, 2, func(int, uint64) uint64 { return 0 })
	f.splits = 1
	f.candidateSide = rebalance.SideIn
	f.failure = func(call int) (rebalance.Side, rebalance.Retry) {
		if call == 0 {
			return rebalance.SideOut, rebalance.RetryWhenFree // the target refuses it alone
		}
		return rebalance.SideIn, rebalance.RetryNo
	}
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, "no candidates left: 1 failed, 1 refused by the target", result.(*Result).Stopped)

	// none at all
	f = splitsOn(t, 0, nil)
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, "no candidates to try", result.(*Result).Stopped)

	// the target reached: nothing to say
	f = splitsOn(t, 1, func(_ int, amount uint64) uint64 { return amount })
	f.amount = 100000000
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Empty(t, result.(*Result).Stopped)
}

// unusable is a run whose candidates cannot be used, each for its reason.
type unusable struct {
	*fakeSplits
	why func(scid string) error
}

func (u *unusable) CanUseChannel(c *glightning.PeerChannel) error { return u.why(c.ShortChannelId) }

// After circular-stop, the splits in flight end at their next payment: the
// run fired every other candidate, each failing at once, and then blamed
// them in stopped.
func TestCircularStopEndsTheRun(t *testing.T) {
	var f *fakeSplits
	f = splitsOn(t, 3, func(call int, amount uint64) uint64 {
		f.Node.Stopped.Store(true)
		return 0
	})
	f.splits = 1
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(f.asked), "no split fired after the stop")
	assert.Equal(t, "circular was stopped (circular-stop): firing no more splits", result.(*Result).Stopped)

	// stopped before the run
	f = splitsOn(t, 3, func(int, uint64) uint64 { return 0 })
	f.Node.Stopped.Store(true)
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Empty(t, f.asked)
	assert.Equal(t, "circular was stopped (circular-stop): firing no more splits", result.(*Result).Stopped)

	// resumed while a split was in flight: the run goes on, and ends for
	// its own reason
	f = splitsOn(t, 2, func(call int, amount uint64) uint64 { return 0 })
	f.splits = 1
	f.Node.Stopped.Store(true)
	f.FireCandidates() // held back
	f.Node.Stopped.Store(false)
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(f.asked))
	assert.Equal(t, "no candidates left: 2 failed", result.(*Result).Stopped)
}

// Runs that took an hour logged nothing on how they were going until the
// end. While splits are in flight, the run now logs a line every
// progressEvery.
func TestProgressIsLoggedWhileSplitsAreInFlight(t *testing.T) {
	var out lockedBuffer
	log.SetOutput(&out)
	defer log.SetOutput(os.Stderr)

	f := splitsOn(t, 1, func(_ int, amount uint64) uint64 { return amount })
	f.amount = 100000000
	f.successDelay = 100 * time.Millisecond
	f.progressEvery = 10 * time.Millisecond
	f.FireCandidates()
	_, err := f.WaitForResult()
	assert.NoError(t, err)
	assert.Contains(t, out.String(), "rebalance: 0/100000 sats moved in 0s, 0 payments, 1 splits in flight")

	// none when the run is shorter
	out.Reset()
	f = splitsOn(t, 1, func(_ int, amount uint64) uint64 { return amount })
	f.amount = 100000000
	f.FireCandidates()
	_, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.NotContains(t, out.String(), "sats moved in")
}

func TestProgressLine(t *testing.T) {
	f := splitsOn(t, 1, nil)
	f.TargetChannel = graph.NewChannel(&glightning.Channel{ShortChannelId: "965538x2935x4"}, 0, 0)
	f.Result = NewResult(3000000000)
	f.amount = 3000000000
	start := time.Now().Add(-312 * time.Second)
	assert.Equal(t, "rebalance 965538x2935x4: 0/3000000 sats moved in 312s, 0 payments, 0 splits in flight", f.progress(start))

	f.AmountRebalanced, f.movedMsat, f.Result.FeesPaidMsat = 1200000000, 1200000000, 336000 // 280 ppm
	f.splitsInFlight = 2
	f.TotalAttempts = 120
	f.sent.Store(143) // payments of the splits in flight included
	f.failStreak = 23
	assert.Equal(t, "rebalance 965538x2935x4: 1200000/3000000 sats moved in 312s, 143 payments, avg 280 ppm, "+
		"2 splits in flight, 23 payments failed in a row", f.progress(start))

	f.RebalanceMethods = &RebalancePull{}
	assert.True(t, strings.HasPrefix(f.progress(start), "circular-pull 965538x2935x4: "))
}

// lockedBuffer is a bytes.Buffer safe to write from several goroutines.
type lockedBuffer struct {
	lock sync.Mutex
	buf  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.buf.Reset()
}

// A run with a time limit stops when it is reached, as at maxfailstreak:
// no more splits, those in flight told to stop before their next payment.
func TestARunStopsAtItsTimeLimit(t *testing.T) {
	f := splitsOn(t, 3, func(_ int, amount uint64) uint64 { return amount })
	f.amount = 100 * 100000000 // 100 splits
	f.splits = 1
	f.successDelay = 20 * time.Millisecond
	f.maxTime = 100 * time.Millisecond
	start := time.Now()
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	res := result.(*Result)
	assert.Less(t, time.Since(start), 2*time.Second, "it did not wait for the target")
	assert.Equal(t, "the run reached its time limit (maxtime 0s): firing no more splits", res.Stopped)
	assert.True(t, f.halt.Load(), "the splits in flight are told to stop")
	assert.Greater(t, len(f.asked), 0)
	assert.Less(t, len(f.asked), 100)
	assert.Equal(t, uint64(len(f.asked))*100000, res.RebalancedAmount, "the split in flight still counts")

	// no limit
	f = splitsOn(t, 1, func(_ int, amount uint64) uint64 { return amount })
	f.amount = 3 * 100000000
	f.splits = 1
	f.successDelay = 20 * time.Millisecond
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, uint64(300000), result.(*Result).RebalancedAmount)
	assert.Empty(t, result.(*Result).Stopped)
	assert.False(t, f.halt.Load())
}

func TestSetMaxTime(t *testing.T) {
	a := &AbstractRebalance{}
	a.setMaxTime(3600)
	assert.Equal(t, time.Hour, a.maxTime)
	a.setMaxTime(0)
	assert.Equal(t, time.Duration(0), a.maxTime)
	a.setMaxTime(-1)
	assert.Equal(t, time.Duration(0), a.maxTime)
}

// A stop that came later took over stopped: a fail streak reached by the
// splits a time limit ended, or a time limit reached while circular-stop
// was letting the splits in flight finish.
func TestTheFirstStopSaysWhy(t *testing.T) {
	// the time limit, then a split in flight takes the streak over its limit
	f := splitsOn(t, 3, func(int, uint64) uint64 { return 0 })
	f.splits = 1
	f.payments = 200
	f.maxFailStreak = 300
	f.maxTime = 200 * time.Millisecond
	f.failureDelay = func(call int) time.Duration {
		if call == 1 {
			return time.Second
		}
		return 0
	}
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(f.asked))
	assert.Contains(t, result.(*Result).Stopped, "time limit")

	// circular-stop, then the time limit while the split in flight finishes
	f = splitsOn(t, 2, func(call int, _ uint64) uint64 {
		f.Node.Stopped.Store(true)
		return 0
	})
	f.maxTime = 200 * time.Millisecond
	f.failureDelay = func(int) time.Duration { return time.Second }
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(f.asked))
	assert.True(t, f.timeStopped)
	assert.Equal(t, "circular was stopped (circular-stop): firing no more splits", result.(*Result).Stopped)

	// circular-stop, then circular-resume with nothing fired since: the
	// hold is over, so a later stop (here: no candidate left) says why
	f = splitsOn(t, 1, func(call int, _ uint64) uint64 {
		f.Node.Stopped.Store(true)
		go func(n *node.Node) {
			time.Sleep(20 * time.Millisecond)
			n.Stopped.Store(false)
		}(f.Node)
		return 0
	})
	f.failureDelay = func(int) time.Duration { return 300 * time.Millisecond }
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.True(t, f.heldByStop, "nothing fired since the resume")
	assert.Equal(t, "no candidates left: 1 failed", result.(*Result).Stopped)

	// the same with a fail streak reached after the resume
	f = splitsOn(t, 2, func(call int, _ uint64) uint64 {
		f.Node.Stopped.Store(true)
		go func(n *node.Node) {
			time.Sleep(20 * time.Millisecond)
			n.Stopped.Store(false)
		}(f.Node)
		return 0
	})
	f.payments = 300
	f.maxFailStreak = 300
	f.failureDelay = func(int) time.Duration { return 300 * time.Millisecond }
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(f.asked))
	assert.Contains(t, result.(*Result).Stopped, "maxfailstreak 300")

	// circular-stop, the time limit during the hold, then circular-resume
	// before the split in flight finishes: the time limit says why
	f = splitsOn(t, 2, func(call int, _ uint64) uint64 {
		f.Node.Stopped.Store(true)
		go func(n *node.Node) {
			time.Sleep(300 * time.Millisecond)
			n.Stopped.Store(false)
		}(f.Node)
		return 0
	})
	f.maxTime = 100 * time.Millisecond
	f.failureDelay = func(int) time.Duration { return 600 * time.Millisecond }
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(f.asked))
	assert.True(t, f.heldByStop && f.timeStopped)
	assert.Contains(t, result.(*Result).Stopped, "time limit")
}

// A run with a fee budget never pays more than it: a split is fired only if
// what was paid, the most the splits in flight can still cost, and the most
// it can cost fit; with nothing in flight and the next split not fitting,
// the run stops and says so.
func TestARunKeepsToItsFeeBudget(t *testing.T) {
	worst := graph.MaxFeeForPPM(100000000, 100) // a 100k-sat split at maxppm 100
	succeeding := func(_ int, amount uint64) uint64 { return amount }

	// one at a time, each paying 1 sat (the fake's 10 ppm)
	f := splitsOn(t, 3, succeeding)
	f.splits = 1
	f.amount = 30 * 100000000
	f.maxPPM = 100
	f.setMaxFee(25)
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	res := result.(*Result)
	fits := 0
	for paid := uint64(0); paid+worst <= 25000; paid += 1000 {
		fits++
	}
	assert.Equal(t, fits, len(f.asked))
	assert.Equal(t, uint64(fits)*1000, res.FeesPaidMsat)
	assert.Equal(t, fmt.Sprintf("the run reached its fee budget (maxfee 25 sats): %d sats paid, and the next split could cost up to %d more: firing no more splits",
		fits, worst/1000), res.Stopped)

	// splits in flight hold the most they can cost: never more than the
	// budget at once
	f = splitsOn(t, 4, succeeding)
	f.splits = 3
	f.amount = 30 * 100000000
	f.maxPPM = 100
	f.successDelay = 10 * time.Millisecond
	f.setMaxFee(int(2*worst/1000 + 1)) // two splits' worst case, not three
	most := 0
	f.fired = func() {
		assert.LessOrEqual(t, f.feesPaid()+f.feeReserved, f.maxFeeMsat)
		if f.splitsInFlight+1 > most {
			most = f.splitsInFlight + 1
		}
	}
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, 2, most, "the third split did not fit")
	assert.LessOrEqual(t, result.(*Result).FeesPaidMsat, f.maxFeeMsat)
	assert.Contains(t, result.(*Result).Stopped, "fee budget")
	assert.Equal(t, uint64(0), f.feeReserved, "all given back")

	// a failed split gives back what it held
	f = splitsOn(t, 3, func(int, uint64) uint64 { return 0 })
	f.maxPPM = 100
	f.setMaxFee(int(worst * 3 / 2 / 1000)) // one split at a time
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(f.asked))
	assert.Equal(t, "no candidates left: 3 failed", result.(*Result).Stopped)

	// a budget below one split's worst case: nothing is fired
	f = splitsOn(t, 3, succeeding)
	f.maxPPM = 100
	f.setMaxFee(5)
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Empty(t, f.asked)
	assert.Contains(t, result.(*Result).Stopped, "fee budget (maxfee 5 sats): 0 sats paid")

	// no budget
	f = splitsOn(t, 1, succeeding)
	f.amount = 3 * 100000000
	f.splits = 1
	f.maxPPM = 100
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, uint64(300000), result.(*Result).RebalancedAmount)
	assert.Empty(t, result.(*Result).Stopped)
}

func TestSetMaxFee(t *testing.T) {
	a := &AbstractRebalance{}
	a.setMaxFee(500)
	assert.Equal(t, uint64(500000), a.maxFeeMsat)
	a.setMaxFee(0)
	assert.Equal(t, uint64(0), a.maxFeeMsat)
	a.setMaxFee(-1)
	assert.Equal(t, uint64(0), a.maxFeeMsat)
	a.setMaxFee(math.MaxInt64)
	assert.Equal(t, uint64(math.MaxUint64), a.maxFeeMsat)
}

// The node's startup options fill what a run's call leaves out: 0 takes the
// node's value, below 0 turns the limit off whatever the node says.
func TestRunNodeDefaults(t *testing.T) {
	n := &node.Node{Defaults: node.Defaults{Attempts: 50, MaxHops: 6, MaxFailStreak: 120, MaxTime: 900, MaxFee: 400}}
	a := &AbstractRebalance{Node: n}
	a.setGenericDefaults()
	assert.Equal(t, 50, a.attempts)
	assert.Equal(t, 6, a.maxHops)
	a.setMaxTime(0)
	assert.Equal(t, 15*time.Minute, a.maxTime)
	a.setMaxTime(-1)
	assert.Equal(t, time.Duration(0), a.maxTime)
	a.setMaxTime(60)
	assert.Equal(t, time.Minute, a.maxTime)
	a.setMaxFee(0)
	assert.Equal(t, uint64(400000), a.maxFeeMsat)
	a.setMaxFee(-1)
	assert.Equal(t, uint64(0), a.maxFeeMsat)
	a.setMaxFee(100)
	assert.Equal(t, uint64(100000), a.maxFeeMsat)

	// the fail streak: the node's limit when the call gives none
	streak := func(nodeLimit, callLimit int) bool {
		f := splitsOn(t, 1, nil)
		f.Node.Defaults.MaxFailStreak = nodeLimit
		f.maxFailStreak = callLimit
		f.Result = NewResult(f.amount)
		f.countFailStreak(&rebalance.Result{Status: "failure", Attempts: 150})
		return f.streakStopped
	}
	assert.True(t, streak(120, 0), "150 failed payments: over the node's 120")
	assert.False(t, streak(-1, 0), "the node turned it off")
	assert.False(t, streak(120, 200), "the call's own limit wins")
	assert.True(t, streak(-1, 100), "the call's own limit wins")
	assert.False(t, streak(0, 0), "the built-in 300")
}

// refillQueue puts candidates back for a slow-mode round, as FindCandidates
// would.
func refillQueue(f *fakeSplits, candidates int) func() error {
	return func() error {
		f.QueueLock.Lock()
		defer f.QueueLock.Unlock()
		f.Candidates = deque.New[*graph.Channel]()
		for i := 0; i < candidates; i++ {
			c, _ := f.Node.Graph.GetChannel(fmt.Sprintf("%dx1x1/0", i+1))
			f.Candidates.PushBack(c)
		}
		return nil
	}
}

// A run that found no route within maxppm gave up at once, though routes
// and fees change through the day. In slow mode it waits and tries again,
// round after round, until its target, time limit, fee budget or
// circular-stop.
func TestSlowMode(t *testing.T) {
	// round 1: every candidate fails; round 2: the target is reached
	f := splitsOn(t, 3, func(call int, amount uint64) uint64 {
		if call < 3 {
			return 0
		}
		return amount
	})
	f.splits, f.amount = 1, 200000000
	f.retryEvery, f.maxTime = 20*time.Millisecond, 5*time.Second
	f.refind = refillQueue(f, 3)
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	res := result.(*Result)
	assert.Equal(t, 2, res.Rounds)
	assert.Equal(t, uint64(200000), res.RebalancedAmount)
	assert.Empty(t, res.Stopped)
	assert.Equal(t, 5, len(f.asked))
	assert.Equal(t, 3, res.SplitsFailed, "totals across rounds")

	// never: rounds until the time limit
	f = splitsOn(t, 2, func(int, uint64) uint64 { return 0 })
	f.retryEvery, f.maxTime = 20*time.Millisecond, 200*time.Millisecond
	f.refind = refillQueue(f, 2)
	begin := time.Now()
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	res = result.(*Result)
	assert.Less(t, time.Since(begin), 2*time.Second)
	assert.GreaterOrEqual(t, res.Rounds, 2)
	assert.Contains(t, res.Stopped, "time limit")
	assert.Equal(t, 2*res.Rounds, len(f.asked), "each round tries every candidate once")

	// the fee budget ends it: no other round
	f = splitsOn(t, 2, func(_ int, amount uint64) uint64 { return amount })
	f.maxPPM = 100
	f.setMaxFee(5)
	f.retryEvery, f.maxTime = 20*time.Millisecond, 5*time.Second
	f.refind = refillQueue(f, 2)
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Equal(t, 1, result.(*Result).Rounds)
	assert.Contains(t, result.(*Result).Stopped, "fee budget")

	// circular-stop while it waits: it ends, and says so
	f = splitsOn(t, 1, func(int, uint64) uint64 { return 0 })
	f.retryEvery, f.maxTime = 300*time.Millisecond, 5*time.Second
	f.refind = refillQueue(f, 1)
	go func(n *node.Node) {
		time.Sleep(50 * time.Millisecond)
		n.Stopped.Store(true)
	}(f.Node)
	begin = time.Now()
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	assert.Less(t, time.Since(begin), 280*time.Millisecond, "not the whole wait")
	assert.Equal(t, 1, result.(*Result).Rounds)
	assert.Equal(t, "circular was stopped (circular-stop): firing no more splits", result.(*Result).Stopped)

	// a round the fail streak stopped is followed by one that fires again
	f = splitsOn(t, 3, func(call int, amount uint64) uint64 {
		if call < 2 {
			return 0
		}
		return amount
	})
	f.splits, f.amount, f.payments, f.maxFailStreak = 1, 100000000, 100, 150
	f.retryEvery, f.maxTime = 20*time.Millisecond, 5*time.Second
	f.refind = refillQueue(f, 3)
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	res = result.(*Result)
	assert.Equal(t, uint64(100000), res.RebalancedAmount, "round 2 fired")
	assert.Equal(t, 2, res.Rounds)
	assert.Contains(t, res.LastRound, "maxfailstreak 150")
	assert.Empty(t, res.Stopped)

	// the time limit while it waits after such a round: that ends the run,
	// and the round's own stop is in last_round
	f = splitsOn(t, 3, func(int, uint64) uint64 { return 0 })
	f.splits, f.payments, f.maxFailStreak = 1, 100, 150
	f.retryEvery, f.maxTime = 150*time.Millisecond, 100*time.Millisecond
	f.refind = refillQueue(f, 3)
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	res = result.(*Result)
	assert.Equal(t, 1, res.Rounds)
	assert.Contains(t, res.Stopped, "time limit")
	assert.Contains(t, res.LastRound, "maxfailstreak 150")

	// the time limit while a round's last split finishes, after the fail
	// streak stopped the round: the time limit ended the run
	f = splitsOn(t, 2, func(int, uint64) uint64 { return 0 })
	f.payments, f.maxFailStreak = 100, 100
	f.retryEvery, f.maxTime = 50*time.Millisecond, 200*time.Millisecond
	f.failureDelay = func(call int) time.Duration {
		if call == 1 {
			return 400 * time.Millisecond
		}
		return 0
	}
	f.refind = refillQueue(f, 2)
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	res = result.(*Result)
	assert.Equal(t, 1, res.Rounds)
	assert.Contains(t, res.Stopped, "time limit")
	assert.Contains(t, res.LastRound, "maxfailstreak 100")

	// a round that finds no candidate fires nothing, and waits for the next
	f = splitsOn(t, 1, func(int, uint64) uint64 { return 0 })
	f.retryEvery, f.maxTime = 20*time.Millisecond, 150*time.Millisecond
	f.refind = func() error {
		f.Candidates = deque.New[*graph.Channel]()
		return errors.New("no peers")
	}
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	res = result.(*Result)
	assert.Equal(t, 1, len(f.asked), "only round 1 had a candidate")
	assert.GreaterOrEqual(t, res.Rounds, 2)
	assert.Contains(t, res.Stopped, "time limit")

	// without slow mode: one round, no rounds field
	f = splitsOn(t, 1, func(int, uint64) uint64 { return 0 })
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	out, _ := json.Marshal(result)
	assert.NotContains(t, string(out), "rounds")
	assert.Equal(t, 1, len(f.asked))
}

func TestSetRetryEvery(t *testing.T) {
	a := &AbstractRebalance{}
	a.setRetryEvery(0)
	assert.Equal(t, time.Duration(0), a.retryEvery)
	assert.Equal(t, time.Duration(0), a.maxTime)
	a.setRetryEvery(30)
	assert.Equal(t, MIN_RETRY_EVERY, a.retryEvery)
	assert.Equal(t, DEFAULT_SLOW_MAXTIME, a.maxTime, "slow mode always ends")
	a = &AbstractRebalance{maxTime: time.Hour}
	a.setRetryEvery(600)
	assert.Equal(t, 10*time.Minute, a.retryEvery)
	assert.Equal(t, time.Hour, a.maxTime)
}

// The user's way to find a price: nothing goes through at 1100, so try a
// small amount a step higher; once it goes through, send the rest at what
// it paid plus a little.
func TestSteppingUpTheFee(t *testing.T) {
	var f *fakeSplits
	routesFrom := func(ppm uint64) func(int, uint64) uint64 {
		return func(_ int, amount uint64) uint64 {
			if f.maxPPM >= ppm {
				return amount
			}
			return 0
		}
	}
	overFrom := func(ppm uint64) func(int) uint64 {
		return func(int) uint64 {
			if f.maxPPM < ppm {
				return ppm // every route found costs more than maxppm
			}
			return 0
		}
	}
	f = splitsOn(t, 3, routesFrom(1300))
	f.cheapest = overFrom(1300)
	f.splits, f.amount, f.maxPPM = 1, 500000000, 1100
	f.stepPPM, f.capPPM, f.probeMsat = 300, 1700, 100000000
	f.retryEvery, f.maxTime = time.Hour, 5*time.Second // steps don't wait
	f.refind = refillQueue(f, 3)
	f.ppm = func(int) uint64 { return 1310 }
	f.fee = func(_ int, delivered uint64) uint64 { return delivered * 1310 / 1000000 }
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	res := result.(*Result)
	assert.Equal(t, uint64(500000), res.RebalancedAmount)
	assert.Empty(t, res.Stopped)
	type round struct {
		ppm, asked, moved uint64
		probe             bool
	}
	var got []round
	for _, h := range res.RoundHistory {
		got = append(got, round{h.MaxPPM, h.Asked, h.Moved, h.Probe})
	}
	assert.Equal(t, []round{{1100, 500000, 0, false}, {1400, 100000, 100000, true}, {1460, 400000, 400000, false}}, got,
		"1100: nothing; probe 100k at 1400: paid 1310; the rest at 1310 + 150")
	assert.Equal(t, uint64(1310), res.RoundHistory[1].PaidPPM)
	assert.Equal(t, uint64(1310), res.RoundHistory[1].MaxPaidPPM)
	assert.Contains(t, res.LastRound, "no candidates left", "round 1 fell short; the probe didn't")

	// nothing at any price up to the cap: probes a step higher, then at the
	// cap after each wait, until the time limit
	f = splitsOn(t, 2, routesFrom(5000))
	f.cheapest = overFrom(5000)
	f.splits, f.amount, f.maxPPM = 1, 500000000, 1100
	f.stepPPM, f.capPPM = 300, 1500
	f.retryEvery, f.maxTime = 20*time.Millisecond, 300*time.Millisecond
	f.refind = refillQueue(f, 2)
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	res = result.(*Result)
	assert.Contains(t, res.Stopped, "time limit")
	if assert.GreaterOrEqual(t, len(res.RoundHistory), 4) {
		assert.Equal(t, uint64(1100), res.RoundHistory[0].MaxPPM)
		assert.Equal(t, uint64(1400), res.RoundHistory[1].MaxPPM)
		assert.True(t, res.RoundHistory[1].Probe)
		assert.Equal(t, uint64(100000), res.RoundHistory[1].Asked, "20% of what is left")
		for _, h := range res.RoundHistory[2:] {
			assert.Equal(t, uint64(1500), h.MaxPPM, "the cap")
		}
	}

	// a probe that paid near the cap: the rest goes at the cap, not above
	f = splitsOn(t, 2, routesFrom(1300))
	f.cheapest = overFrom(1300)
	f.splits, f.amount, f.maxPPM = 1, 300000000, 1100
	f.stepPPM, f.capPPM, f.probeMsat = 300, 1450, 100000000
	f.retryEvery, f.maxTime = time.Hour, 5*time.Second
	f.refind = refillQueue(f, 2)
	f.ppm = func(int) uint64 { return 1390 }
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	res = result.(*Result)
	if assert.Equal(t, 3, len(res.RoundHistory)) {
		assert.Equal(t, uint64(1400), res.RoundHistory[1].MaxPPM)
		assert.Equal(t, uint64(1450), res.RoundHistory[2].MaxPPM, "1390 + 150, capped at 1450")
	}

	// payments that failed in the middle of their routes (the routes within
	// maxppm did not work): a step up may find others
	f = splitsOn(t, 2, routesFrom(1300))
	f.failedNodes = func(int) map[string]int { return map[string]int{"02middle": 3} }
	f.middle = 3
	f.splits, f.amount, f.maxPPM = 1, 300000000, 1100
	f.stepPPM, f.capPPM, f.probeMsat = 300, 1700, 100000000
	f.retryEvery, f.maxTime = time.Hour, 5*time.Second
	f.refind = refillQueue(f, 2)
	f.FireCandidates()
	result, err = f.WaitForResult()
	assert.NoError(t, err)
	res = result.(*Result)
	assert.Equal(t, uint64(300000), res.RebalancedAmount)
	if assert.GreaterOrEqual(t, len(res.RoundHistory), 2) {
		assert.Equal(t, uint64(1400), res.RoundHistory[1].MaxPPM)
	}

	// nothing could be tried (no sign of the price: depleted, disconnected,
	// refused): the same round again, after the wait, not a step up
	for name, fail := range map[string]func(*fakeSplits){
		"our channels": func(f *fakeSplits) {
			f.failure = func(int) (rebalance.Side, rebalance.Retry) { return rebalance.SideOut, rebalance.RetryNo }
		},
		"payments failed only at our peers": func(f *fakeSplits) {
			f.failedNodes = func(int) map[string]int { return map[string]int{"02peer0": 5} }
		},
		"held by its own limit": func(f *fakeSplits) {
			// the cheapest route within the run's maxppm, above the split's
			// own (feeratio) limit: a higher maxppm does not raise that
			f.cheapest = func(int) uint64 { return 900 }
			f.held = func(int) bool { return true }
		},
	} {
		f = splitsOn(t, 2, routesFrom(1300))
		fail(f)
		f.splits, f.amount, f.maxPPM = 1, 300000000, 1100
		f.stepPPM, f.capPPM = 300, 1700
		f.retryEvery, f.maxTime = 30*time.Millisecond, 200*time.Millisecond
		f.refind = refillQueue(f, 2)
		f.FireCandidates()
		result, err = f.WaitForResult()
		assert.NoError(t, err, name)
		res = result.(*Result)
		assert.Equal(t, uint64(0), res.RebalancedAmount, name)
		if assert.GreaterOrEqual(t, len(res.RoundHistory), 2, name) {
			for _, h := range res.RoundHistory {
				assert.Equal(t, uint64(1100), h.MaxPPM, name)
				assert.False(t, h.Probe, name)
			}
			assert.LessOrEqual(t, len(res.RoundHistory), 8, name+": rounds wait")
		}
	}
}

// What a round learned of its price is its own: a probe that could try
// nothing after a round held back by the price is tried again at its price.
func TestRoundPricedIsPerRound(t *testing.T) {
	var f *fakeSplits
	f = splitsOn(t, 2, func(int, uint64) uint64 { return 0 })
	f.cheapest = func(int) uint64 {
		if f.maxPPM == 1100 {
			return 1300
		}
		return 0 // at 1400: nothing tried, nothing learned
	}
	f.splits, f.amount, f.maxPPM = 1, 300000000, 1100
	f.stepPPM, f.capPPM = 300, 1700
	f.retryEvery, f.maxTime = 30*time.Millisecond, 200*time.Millisecond
	f.refind = refillQueue(f, 2)
	f.FireCandidates()
	result, err := f.WaitForResult()
	assert.NoError(t, err)
	res := result.(*Result)
	if assert.GreaterOrEqual(t, len(res.RoundHistory), 3) {
		assert.Equal(t, uint64(1100), res.RoundHistory[0].MaxPPM)
		for _, h := range res.RoundHistory[1:] {
			assert.Equal(t, uint64(1400), h.MaxPPM, "no higher without a sign of the price")
			assert.True(t, h.Probe)
		}
	}
}

func TestPriceHeld(t *testing.T) {
	for _, c := range []struct {
		name string
		res  rebalance.Result
		want bool
	}{
		{"nothing tried", rebalance.Result{}, false},
		{"every route above maxppm", rebalance.Result{CheapestPPM: 1300}, true},
		{"a route within maxppm, above its own limit", rebalance.Result{CheapestPPM: 900, HeldByOwnLimit: true, OwnLimit: true}, false},
		{"above the run's maxppm, with a lower limit of its own", rebalance.Result{CheapestPPM: 1300, OwnLimit: true}, false},
		{"the in-peer above maxppm", rebalance.Result{InOverMax: true}, true},
		{"the in-peer above maxppm, with a lower limit of its own", rebalance.Result{InOverMax: true, OwnLimit: true}, false},
		{"payments failed in the middle", rebalance.Result{FailedNodes: map[string]int{"02x": 1}, MiddleFailures: 1}, true},
		{"payments failed only at our peers", rebalance.Result{FailedNodes: map[string]int{"02outpeer": 5}}, false},
		{"failed in the middle, within its own limit", rebalance.Result{MiddleFailures: 1, OwnLimit: true}, false},
	} {
		assert.Equal(t, c.want, priceHeld(&c.res), c.name)
	}
}

// With a fee ratio, stepping goes no higher than the most any split of the
// round could pay, and waits there as at the cap.
func TestStepCapAtTheFeeRatio(t *testing.T) {
	a := &AbstractRebalance{maxPPM: 1100, stepPPM: 300, capPPM: 1700, roundPriced: true}
	price, probe, wait := a.nextStep()
	assert.Equal(t, []any{uint64(1400), true, false}, []any{price, probe, wait}, "no fee ratio: a step")
	a.roundMost = 1274
	price, probe, wait = a.nextStep()
	assert.Equal(t, []any{uint64(1274), true, false}, []any{price, probe, wait}, "up to 1274, not 1400")
	a.maxPPM, a.stepProbe = 1274, true
	price, probe, wait = a.nextStep()
	assert.Equal(t, []any{uint64(1274), true, true}, []any{price, probe, wait}, "there: probe again after the wait")
	a.roundMost = 5000
	price, _, _ = a.nextStep()
	assert.Equal(t, uint64(1574), price, "above: capppm decides")

	// circular-push left a candidate out for its peer's fee: what it could pay
	// is not known, so the splits fired don't bound the step
	a = &AbstractRebalance{maxPPM: 1100, stepPPM: 300, capPPM: 2000, roundPriced: true, roundMost: 900,
		roundLeftOut: true}
	price, _, wait = a.nextStep()
	assert.Equal(t, uint64(1400), price)
	assert.False(t, wait)
	assert.False(t, a.atTop())

	// a probe that moved: the rest at what it paid + half a step, no higher
	a = &AbstractRebalance{maxPPM: 1274, stepPPM: 300, capPPM: 1700, stepProbe: true, roundMost: 1274,
		roundMaxPaid: 1250, AmountRebalanced: 100000000}
	price, probe, wait = a.nextStep()
	assert.Equal(t, []any{uint64(1274), false, false}, []any{price, probe, wait})

	// what a round reports: its splits' highest limit when below its maxppm
	a = &AbstractRebalance{maxPPM: 1400, roundTopLimit: 1274}
	assert.Equal(t, uint64(1274), a.heldLimit())
	a.roundTopLimit = 1400
	assert.Zero(t, a.heldLimit())
}

func TestSetStepping(t *testing.T) {
	a := &AbstractRebalance{maxPPM: 1100}
	assert.NoError(t, a.setStepping(0, 0, 0))
	assert.Equal(t, uint64(0), a.stepPPM)
	assert.Error(t, a.setStepping(300, 0, 0), "capppm is required")
	assert.Error(t, a.setStepping(300, 1000, 0), "capppm below maxppm")
	assert.NoError(t, a.setStepping(300, 1700, 1000000))
	assert.Equal(t, uint64(300), a.stepPPM)
	assert.Equal(t, uint64(1700), a.capPPM)
	assert.Equal(t, uint64(1000000000), a.probeMsat)
	assert.Equal(t, DEFAULT_STEP_RETRY_EVERY, a.retryEvery, "slow mode on")
	assert.Equal(t, DEFAULT_SLOW_MAXTIME, a.maxTime)

	a.setMaxIdleRounds(0)
	assert.Equal(t, DEFAULT_MAX_IDLE_ROUNDS, a.maxIdleRounds, "stepping: 3 by default")
	a.setMaxIdleRounds(5)
	assert.Equal(t, 5, a.maxIdleRounds)
	a.setMaxIdleRounds(-1)
	assert.Zero(t, a.maxIdleRounds, "-1: never")
	b := &AbstractRebalance{}
	b.setMaxIdleRounds(0)
	assert.Zero(t, b.maxIdleRounds, "no stepping: never by default")
	b.setMaxIdleRounds(2)
	assert.Equal(t, 2, b.maxIdleRounds, "plain slow mode, when asked")
}

// A run that keeps moving nothing at the most it can pay, held back by the
// price, ends after maxidlerounds such rounds instead of retrying until
// maxtime; rounds below the top, or that could try nothing, don't count,
// and one that moves something starts the count over.
func TestGiveUpAtTheTop(t *testing.T) {
	var f *fakeSplits
	never := func(int, uint64) uint64 { return 0 }
	priced := func(int) uint64 { return 5000 } // every route costs 5000 ppm
	run := func() (*Result, time.Duration) {
		start := time.Now()
		f.FireCandidates()
		result, err := f.WaitForResult()
		assert.NoError(t, err)
		return result.(*Result), time.Since(start)
	}

	// stepping: 1100 and 1400 are below the cap; two rounds at 1500, then done
	// (below the cap the cheapest route is just above maxppm: what the last
	// round found is said)
	f = splitsOn(t, 2, never)
	f.cheapest = func(int) uint64 {
		if f.maxPPM < 1500 {
			return f.maxPPM + 50
		}
		return 5000
	}
	f.splits, f.amount, f.maxPPM = 1, 500000000, 1100
	f.stepPPM, f.capPPM, f.maxIdleRounds = 300, 1500, 2
	f.retryEvery, f.maxTime = 20*time.Millisecond, 5*time.Second
	f.refind = refillQueue(f, 2)
	res, took := run()
	assert.Less(t, took, 2*time.Second, "not until maxtime")
	assert.Equal(t, "gave up: 2 rounds in a row at the most this run can pay (1500 ppm, capppm) moved nothing; "+
		"the cheapest route found cost 5000 ppm", res.Stopped)
	got := []uint64{}
	for _, h := range res.RoundHistory {
		got = append(got, h.MaxPPM)
	}
	assert.Equal(t, []uint64{1100, 1400, 1500, 1500}, got)
	assert.NotEmpty(t, res.LastRound, "why the last round fell short")

	// plain slow mode, when asked: every round is at the top; its rounds end
	// on the fail streak, as on the node: that goes to last_round, the
	// giving up to stopped
	f = splitsOn(t, 2, never)
	f.cheapest = priced
	f.payments, f.maxFailStreak = 10, 5
	f.splits, f.amount, f.maxPPM, f.maxIdleRounds = 1, 500000000, 1100, 2
	f.retryEvery, f.maxTime = 20*time.Millisecond, 5*time.Second
	f.refind = refillQueue(f, 2)
	res, _ = run()
	assert.Contains(t, res.Stopped, "gave up: 2 rounds in a row at the most this run can pay (1100 ppm, maxppm)")
	assert.Contains(t, res.LastRound, "maxfailstreak")
	assert.Equal(t, 2, len(res.RoundHistory))

	// a round at the top that could try nothing leaves the count as it is:
	// priced, nothing tried, priced is two
	f = splitsOn(t, 2, never)
	f.cheapest = func(call int) uint64 {
		if (call/2)%2 == 0 { // rounds 1, 3, ...: two calls each
			return 5000
		}
		return 0
	}
	f.splits, f.amount, f.maxPPM, f.maxIdleRounds = 1, 500000000, 1100, 2
	f.retryEvery, f.maxTime = 20*time.Millisecond, 5*time.Second
	f.refind = refillQueue(f, 2)
	res, _ = run()
	assert.Contains(t, res.Stopped, "gave up: 2 rounds")
	assert.Equal(t, 3, len(res.RoundHistory))

	// rounds that could try nothing (no sign of the price) don't count
	f = splitsOn(t, 2, never)
	f.splits, f.amount, f.maxPPM, f.maxIdleRounds = 1, 500000000, 1100, 2
	f.retryEvery, f.maxTime = 20*time.Millisecond, 200*time.Millisecond
	f.refind = refillQueue(f, 2)
	res, _ = run()
	assert.Contains(t, res.Stopped, "time limit")
	assert.GreaterOrEqual(t, len(res.RoundHistory), 3)

	// a round that moves something starts the count over: the third split
	// (round 2's first) goes through
	f = splitsOn(t, 2, func(call int, amount uint64) uint64 {
		if call == 2 {
			return amount
		}
		return 0
	})
	f.cheapest = func(call int) uint64 {
		if call == 2 {
			return 0
		}
		return 5000
	}
	f.splits, f.amount, f.maxPPM, f.maxIdleRounds = 1, 500000000, 1100, 2
	f.retryEvery, f.maxTime = 20*time.Millisecond, 5*time.Second
	f.refind = refillQueue(f, 2)
	res, _ = run()
	if assert.Equal(t, 4, len(res.RoundHistory), res.Stopped) {
		assert.Equal(t, uint64(100000), res.RoundHistory[1].Moved)
	}
	assert.Contains(t, res.Stopped, "gave up: 2 rounds")
}

func TestCountIdle(t *testing.T) {
	stepping := func(maxPPM uint64) *AbstractRebalance {
		return &AbstractRebalance{maxPPM: maxPPM, stepPPM: 300, capPPM: 1700, idleRounds: 2}
	}
	for _, c := range []struct {
		name string
		r    *AbstractRebalance
		want int
	}{
		{"price held at the feeratio ceiling", func() *AbstractRebalance {
			r := stepping(1274)
			r.roundPriced, r.roundTopLimit, r.roundMost = true, 1274, 1274
			return r
		}(), 3},
		{"moved something", func() *AbstractRebalance {
			r := stepping(1274)
			r.roundPriced, r.roundTopLimit, r.roundMost, r.AmountRebalanced = true, 1274, 1274, 1000
			return r
		}(), 0},
		{"fired no split: nothing learned", stepping(1274), 2},
		{"tried, no sign of the price, at the top", func() *AbstractRebalance {
			r := stepping(1274)
			r.roundTopLimit, r.roundMost = 1274, 1274
			return r
		}(), 2},
		{"below a top that rose", func() *AbstractRebalance {
			r := stepping(1274)
			r.roundTopLimit, r.roundMost = 1274, 1530
			return r
		}(), 0},
		{"push left candidates out, below capppm: the top is capppm", func() *AbstractRebalance {
			r := stepping(1274)
			r.roundPriced, r.roundLeftOut = true, true
			return r
		}(), 0},
		{"push left candidates out, at capppm", func() *AbstractRebalance {
			r := stepping(1700)
			r.roundPriced, r.roundLeftOut = true, true
			return r
		}(), 3},
	} {
		c.r.countIdle()
		assert.Equal(t, c.want, c.r.idleRounds, c.name)
	}
}

func TestSetMaxIdleRoundsFollowsStepping(t *testing.T) {
	r := &RebalancePull{}
	r.Node = &node.Node{}
	assert.NoError(t, r.setStepping(300, 1700, 0))
	r.setMaxIdleRounds(r.MaxIdleRounds)
	assert.Equal(t, DEFAULT_MAX_IDLE_ROUNDS, r.maxIdleRounds)
}

// realSplits fires real splits (rebalance.Run: route search, payments through
// pay, learning from what failed) over a small network; the candidate queue
// is the fake's.
type realSplits struct{ *fakeSplits }

func (r *realSplits) Fire(candidate *graph.Channel, amount uint64) {
	r.launch(candidate, r.TargetChannel, candidate, amount)
}

func (r *realSplits) EnqueueCandidate(result *rebalance.Result) {
	r.QueueLock.Lock()
	r.Candidates.PushFront(r.launched[result.Candidate])
	r.QueueLock.Unlock()
}

// netChannel adds a public channel from -> to charging ppm.
func netChannel(g *graph.Graph, scid, from, to string, ppm uint64) *graph.Channel {
	c := graph.NewChannel(&glightning.Channel{Source: from, Destination: to, ShortChannelId: scid, IsActive: true,
		AmountMsat: glightning.AmountFromMSat(10000000000), FeePerMillionth: ppm, Delay: 10,
		HtlcMinimumMilliSatoshis: glightning.AmountFromMSat(0),
		HtlcMaximumMilliSatoshis: glightning.AmountFromMSat(10000000000)}, 5000000000, 0)
	g.AddChannel(c)
	g.Channels[scid+"/"+util.GetDirection(from, to)] = c
	return c
}

// stepWay is a way from the out-peer to the in-peer through via, at ppm1
// (out-peer -> via) and ppm2 (via -> in-peer); empty: the hop (1 or 2)
// that cannot carry a split.
type stepWay struct {
	via        string
	ppm1, ppm2 uint64
	empty      int
}

// stepPull is a circular-pull of 300k sats in splits of 100k, one at a
// time, from our channel to 02out into the target from 02in (charging
// inPPM), with fee stepping from 1100 by 300 up to 1700, probing 100k.
func stepPull(t *testing.T, inPPM uint64, ways ...stepWay) *realSplits {
	const self, outP, inP = "02self", "02out", "02in"
	g := graph.NewGraph()
	out := netChannel(g, "1x1x1", self, outP, 0)
	in := netChannel(g, "9x1x1", inP, self, inPPM)
	var lock sync.Mutex
	full := map[string]bool{}
	for i, w := range ways {
		a := netChannel(g, fmt.Sprintf("%dx2x1", i+1), outP, w.via, w.ppm1)
		b := netChannel(g, fmt.Sprintf("%dx3x1", i+1), w.via, inP, w.ppm2)
		switch w.empty {
		case 1:
			full[candidateId(a)] = true
		case 2:
			full[candidateId(b)] = true
		}
	}
	pay := func(route *graph.Route) (*graph.PrettyRoute, error) {
		lock.Lock()
		defer lock.Unlock()
		for i, id := range route.ChannelIds() {
			if full[id] {
				hop := route.Hops[i]
				return nil, &glightning.PaymentError{Data: &glightning.PaymentErrorData{
					ErringNode: hop.Source, ErringChannel: hop.ShortChannelId,
					ErringDirection: int(id[len(id)-1] - '0'), ErringIndex: uint64(i),
					FailCode: 0x1007, FailCodeName: "WIRE_TEMPORARY_CHANNEL_FAILURE"}}
			}
		}
		return graph.NewPrettyRoute(route, ""), nil
	}
	peer := func(id, scid string) *glightning.Peer {
		return &glightning.Peer{Id: id, Connected: true, Channels: []*glightning.PeerChannel{{
			PeerId: id, ShortChannelId: scid, State: rebalance.NORMAL, PeerConnected: true,
			TotalMsat: glightning.AmountFromMSat(10000000000), ToUsMsat: glightning.AmountFromMSat(5000000000),
			SpendableMsat: glightning.AmountFromMSat(5000000000), ReceivableMsat: glightning.AmountFromMSat(5000000000),
		}}}
	}
	n := &node.Node{Id: self, Graph: g, PeersLock: &sync.RWMutex{},
		Peers: map[string]*glightning.Peer{outP: peer(outP, "1x1x1"), inP: peer(inP, "9x1x1")}}
	a := &AbstractRebalance{Node: n, Candidates: deque.New[*graph.Channel](), AmountLock: &sync.Mutex{},
		QueueLock: &sync.Mutex{}, RebalanceResultChan: make(chan *rebalance.Result, 1),
		amount: 300000000, splitAmount: 100000000, splits: 1, attempts: 10, maxHops: 6, maxNodeFails: 5,
		maxPPM: 1100, routes: map[string][]string{}, candidateSide: rebalance.SideOut,
		launched: map[string]*graph.Channel{}, retried: map[string]bool{}, TargetChannel: in, pay: pay}
	a.stepPPM, a.capPPM, a.probeMsat = 300, 1700, 100000000
	a.retryEvery, a.maxTime = 30*time.Millisecond, 300*time.Millisecond
	a.Candidates.PushBack(out)
	a.refind = func() error {
		a.QueueLock.Lock()
		defer a.QueueLock.Unlock()
		a.Candidates = deque.New[*graph.Channel]()
		a.Candidates.PushBack(out)
		return nil
	}
	r := &realSplits{&fakeSplits{AbstractRebalance: a}}
	a.RebalanceMethods = r
	return r
}

func stepRun(t *testing.T, r *realSplits) *Result {
	r.FireCandidates()
	result, err := r.WaitForResult()
	assert.NoError(t, err)
	return result.(*Result)
}

// Fee stepping on what real splits learn: the route search, the payments
// that fail and where, the in-peer's own fee.
func TestSteppingWithRealSplits(t *testing.T) {
	// the cheap way (1000 ppm) fails in the middle, the dear one (1300 ppm)
	// works: 1100 tries the cheap one and finds the dear one over maxppm; a
	// probe at 1400 goes the dear way; the rest at what it paid + 150
	res := stepRun(t, stepPull(t, 0,
		stepWay{via: "02cheap", ppm1: 500, ppm2: 500, empty: 2},
		stepWay{via: "02dear", ppm1: 700, ppm2: 600}))
	assert.Equal(t, uint64(300000), res.RebalancedAmount, res.Stopped)
	if assert.Equal(t, 3, len(res.RoundHistory)) {
		h := res.RoundHistory
		assert.Equal(t, uint64(1100), h[0].MaxPPM)
		assert.Equal(t, uint64(0), h[0].Moved)
		assert.Equal(t, uint64(1400), h[1].MaxPPM)
		assert.True(t, h[1].Probe)
		assert.Equal(t, uint64(100000), h[1].Moved)
		assert.InDelta(t, 1300, float64(h[1].MaxPaidPPM), 2, "the dear way")
		assert.Equal(t, h[1].MaxPaidPPM+150, h[2].MaxPPM, "what the probe paid + half a step")
		assert.Equal(t, uint64(200000), h[2].Moved)
	}
	assert.GreaterOrEqual(t, res.CheapestPPMOverMax, uint64(1300), "round 1 found the dear way over 1100")

	// as on the node: the ways within maxppm fail in the middle until the
	// split's attempts run out, before any search finds the way over it;
	// those failures alone step up (the failed channels are believed short
	// afterwards, so the probe tries the last cheap way, then the dear one)
	r := stepPull(t, 0,
		stepWay{via: "02a", ppm1: 500, ppm2: 500, empty: 2},
		stepWay{via: "02b", ppm1: 500, ppm2: 500, empty: 2},
		stepWay{via: "02c", ppm1: 500, ppm2: 500, empty: 2},
		stepWay{via: "02dear", ppm1: 700, ppm2: 600})
	r.attempts = 2
	res = stepRun(t, r)
	assert.Equal(t, uint64(300000), res.RebalancedAmount, res.Stopped)
	assert.Zero(t, res.CheapestPPMOverMax, "no search found a way over maxppm: the payments' failures stepped")
	if assert.GreaterOrEqual(t, len(res.RoundHistory), 2) {
		assert.Equal(t, uint64(0), res.RoundHistory[0].Moved)
		assert.Equal(t, uint64(1400), res.RoundHistory[1].MaxPPM)
		assert.True(t, res.RoundHistory[1].Probe)
	}

	// only a way at 1300 ppm, which works: 1100 sends nothing and finds it
	// over maxppm, which alone steps up
	res = stepRun(t, stepPull(t, 0, stepWay{via: "02dear", ppm1: 700, ppm2: 600}))
	assert.Equal(t, uint64(300000), res.RebalancedAmount, res.Stopped)
	if assert.Equal(t, 3, len(res.RoundHistory)) {
		assert.Equal(t, uint64(1400), res.RoundHistory[1].MaxPPM)
	}

	// both ways (1000, 1050 ppm) within maxppm, but the out-peer cannot
	// send on either: no route avoids it, so no step up, the same round
	// after each wait
	res = stepRun(t, stepPull(t, 0,
		stepWay{via: "02a", ppm1: 500, ppm2: 500, empty: 1},
		stepWay{via: "02b", ppm1: 550, ppm2: 500, empty: 1}))
	assert.Equal(t, uint64(0), res.RebalancedAmount)
	if assert.GreaterOrEqual(t, len(res.RoundHistory), 2) {
		for _, h := range res.RoundHistory {
			assert.Equal(t, uint64(1100), h.MaxPPM)
			assert.False(t, h.Probe)
		}
	}

	// feeratio 0.9 of a target that earned 1416 ppm: no split pays more than
	// 1274, so a step goes there, not to 1400; a way at 1250 works there,
	// and the rest goes at 1274 too
	r = stepPull(t, 0, stepWay{via: "02a", ppm1: 650, ppm2: 600})
	r.feeRatio, r.forwards = 0.9, func(string, time.Time) (uint64, uint64, error) { return 1416, 200000000, nil }
	res = stepRun(t, r)
	assert.Equal(t, uint64(300000), res.RebalancedAmount, res.Stopped)
	got := []uint64{}
	for _, h := range res.RoundHistory {
		got = append(got, h.MaxPPM)
	}
	assert.Equal(t, []uint64{1100, 1274, 1274}, got)

	// and when nothing goes through below that: it waits at 1274, and a
	// round started above it says feeratio held its splits
	r = stepPull(t, 0, stepWay{via: "02a", ppm1: 700, ppm2: 600})
	r.feeRatio, r.forwards = 0.9, func(string, time.Time) (uint64, uint64, error) { return 1416, 200000000, nil }
	res = stepRun(t, r)
	assert.Equal(t, uint64(0), res.RebalancedAmount)
	if assert.GreaterOrEqual(t, len(res.RoundHistory), 3) {
		for _, h := range res.RoundHistory[1:] {
			assert.Equal(t, uint64(1274), h.MaxPPM, "no higher than its splits can pay")
		}
	}
	// it gives up there after maxidlerounds rounds, saying why
	r = stepPull(t, 0, stepWay{via: "02a", ppm1: 700, ppm2: 600})
	r.feeRatio, r.forwards = 0.9, func(string, time.Time) (uint64, uint64, error) { return 1416, 200000000, nil }
	r.maxIdleRounds, r.maxTime = 2, 5*time.Second
	res = stepRun(t, r)
	assert.Equal(t, "gave up: 2 rounds in a row at the most this run can pay (1274 ppm, feeratio) moved nothing; "+
		"the cheapest route found cost 1300 ppm", res.Stopped)
	assert.Equal(t, 3, len(res.RoundHistory), "1100, then two at 1274")

	// when what the splits can pay rises (the target earned more), the round
	// at the old top is below the new one: the count starts over, and the
	// run steps to the new top
	r = stepPull(t, 0, stepWay{via: "02a", ppm1: 800, ppm2: 800}) // 1600 ppm: never within
	reads := 0
	r.feeRatio, r.forwards = 0.9, func(string, time.Time) (uint64, uint64, error) {
		reads++
		if reads <= 2 {
			return 1416, 200000000, nil // 1274
		}
		return 1700, 200000000, nil // 1530
	}
	r.maxIdleRounds, r.maxTime = 2, 5*time.Second
	res = stepRun(t, r)
	got = nil
	for _, h := range res.RoundHistory {
		got = append(got, h.MaxPPM)
	}
	assert.Equal(t, []uint64{1100, 1274, 1274, 1530, 1530}, got, res.Stopped)
	assert.Contains(t, res.Stopped, "gave up: 2 rounds in a row at the most this run can pay (1530 ppm, feeratio)")

	// at the feeratio ceiling, a round that fires no split (no candidate
	// left) learns nothing of the top: the count stays
	r = stepPull(t, 0, stepWay{via: "02a", ppm1: 700, ppm2: 600})
	r.feeRatio, r.forwards = 0.9, func(string, time.Time) (uint64, uint64, error) { return 1416, 200000000, nil }
	r.maxIdleRounds, r.maxTime = 2, 3*time.Second
	refill, refinds := r.refind, 0
	r.refind = func() error {
		refinds++
		if refinds == 2 { // round 3: nothing to fire
			r.QueueLock.Lock()
			r.Candidates = deque.New[*graph.Channel]()
			r.QueueLock.Unlock()
			return nil
		}
		return refill()
	}
	res = stepRun(t, r)
	got = nil
	for _, h := range res.RoundHistory {
		got = append(got, h.MaxPPM)
	}
	assert.Equal(t, []uint64{1100, 1274, 1274, 1274}, got, res.Stopped)
	assert.Contains(t, res.Stopped, "gave up: 2 rounds in a row at the most this run can pay (1274 ppm, feeratio)")

	r = stepPull(t, 0, stepWay{via: "02a", ppm1: 700, ppm2: 600})
	r.maxPPM = 1400
	r.feeRatio, r.forwards = 0.9, func(string, time.Time) (uint64, uint64, error) { return 1416, 200000000, nil }
	res = stepRun(t, r)
	if assert.NotEmpty(t, res.RoundHistory) {
		assert.Equal(t, uint64(1274), res.RoundHistory[0].LimitPPM, "held below its 1400")
		assert.Equal(t, uint64(1400), res.RoundHistory[0].MaxPPM, "not priced: no step")
	}
	assert.Contains(t, res.LastRound, "feeratio held its splits to at most 1274 ppm")

	// what a round reports is its own: the target's forwards are read again
	// each round, and earned less (1000 ppm) after the first
	r = stepPull(t, 0, stepWay{via: "02a", ppm1: 700, ppm2: 600})
	reads = 0
	r.feeRatio, r.forwards = 0.9, func(string, time.Time) (uint64, uint64, error) {
		reads++
		if reads == 1 {
			return 1416, 200000000, nil
		}
		return 1000, 200000000, nil
	}
	res = stepRun(t, r)
	if assert.GreaterOrEqual(t, len(res.RoundHistory), 2) {
		assert.Equal(t, uint64(1274), res.RoundHistory[1].MaxPPM)
		assert.Equal(t, uint64(900), res.RoundHistory[1].LimitPPM, "0.9 × 1000, not round 1's 1100")
	}

	// the in-peer alone charges 1500 ppm: no route at 1100 or 1400, then
	// at the cap a probe goes, and the rest at the cap (its rate + 150 is
	// above it)
	res = stepRun(t, stepPull(t, 1500, stepWay{via: "02a", ppm1: 50, ppm2: 50}))
	assert.Equal(t, uint64(300000), res.RebalancedAmount, res.Stopped)
	got = nil
	for _, h := range res.RoundHistory {
		got = append(got, h.MaxPPM)
	}
	assert.Equal(t, []uint64{1100, 1400, 1700, 1700}, got)
}
