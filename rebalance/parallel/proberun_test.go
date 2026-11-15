package parallel

import (
	"circular/graph"
	"circular/node"
	"circular/rebalance"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/elementsproject/glightning/glightning"
	"github.com/elementsproject/glightning/jrpc2"
	"github.com/gammazero/deque"
	"github.com/stretchr/testify/assert"
)

// probeNet is a small network for probe-only pulls into 9x1x1 (from 02in):
// our channels 1x1x1 (to 02o1) and 2x1x1 (to 02o2) are the candidates, and
// each way is out-peer -> via -> 02in. holds says how much (msat) a channel
// can carry (absent: anything); probes records each probe's first channel
// and amount.
type probeNet struct {
	*realSplits
	lock   sync.Mutex
	holds  map[string]uint64
	probes []string
}

type probeWay struct {
	from, via  string // the out-peer, and the node between it and 02in
	ppm        uint64 // out-peer -> via
	holds      uint64 // what via -> 02in carries (0: anything)
	firstHolds uint64 // what out-peer -> via carries (0: anything)
}

func probePull(t *testing.T, splits int, ways ...probeWay) *probeNet {
	const self, inP = "02self", "02in"
	g := graph.NewGraph()
	p := &probeNet{holds: map[string]uint64{}}
	outs := []*graph.Channel{netChannel(g, "1x1x1", self, "02o1", 0), netChannel(g, "2x1x1", self, "02o2", 0)}
	in := netChannel(g, "9x1x1", inP, self, 0)
	for i, w := range ways {
		a := netChannel(g, fmt.Sprintf("%dx2x1", i+1), w.from, w.via, w.ppm)
		b := netChannel(g, fmt.Sprintf("%dx3x1", i+1), w.via, inP, 0)
		if w.firstHolds > 0 {
			p.holds[candidateId(a)] = w.firstHolds
		}
		if w.holds > 0 {
			p.holds[candidateId(b)] = w.holds
		}
	}
	probe := func(route *graph.Route) error {
		p.lock.Lock()
		defer p.lock.Unlock()
		p.probes = append(p.probes, fmt.Sprintf("%s %d", route.Hops[0].ShortChannelId, route.Amount/1000000))
		for i, id := range route.ChannelIds() {
			if holds, ok := p.holds[id]; ok && holds < route.Hops[i].MilliSatoshi {
				hop := route.Hops[i]
				return &glightning.PaymentError{Data: &glightning.PaymentErrorData{
					ErringNode: hop.Source, ErringChannel: hop.ShortChannelId,
					ErringDirection: int(id[len(id)-1] - '0'), ErringIndex: uint64(i),
					FailCode: 0x1007, FailCodeName: "WIRE_TEMPORARY_CHANNEL_FAILURE"}}
			}
		}
		return &glightning.PaymentError{Data: &glightning.PaymentErrorData{ErringNode: self,
			ErringIndex: uint64(len(route.Hops)), FailCode: 0x400f, FailCodeName: "WIRE_INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS"}}
	}
	peer := func(id, scid string) *glightning.Peer {
		return &glightning.Peer{Id: id, Connected: true, Channels: []*glightning.PeerChannel{{
			PeerId: id, ShortChannelId: scid, State: rebalance.NORMAL, PeerConnected: true,
			TotalMsat: glightning.AmountFromMSat(10000000000), ToUsMsat: glightning.AmountFromMSat(5000000000),
			SpendableMsat: glightning.AmountFromMSat(5000000000), ReceivableMsat: glightning.AmountFromMSat(5000000000),
		}}}
	}
	n := &node.Node{Id: self, Graph: g, PeersLock: &sync.RWMutex{}, Peers: map[string]*glightning.Peer{
		"02o1": peer("02o1", "1x1x1"), "02o2": peer("02o2", "2x1x1"), inP: peer(inP, "9x1x1")}}
	a := &AbstractRebalance{Node: n, Candidates: deque.New[*graph.Channel](), AmountLock: &sync.Mutex{},
		QueueLock: &sync.Mutex{}, RebalanceResultChan: make(chan *rebalance.Result, splits),
		amount: 300000000, splitAmount: 100000000, splits: splits, attempts: 10, maxHops: 6, maxNodeFails: 5,
		maxPPM: 2000, routes: map[string][]string{}, candidateSide: rebalance.SideOut,
		launched: map[string]*graph.Channel{}, retried: map[string]bool{}, TargetChannel: in,
		probe: probe, progressEvery: time.Hour,
		pay: func(*graph.Route) (*graph.PrettyRoute, error) { panic("a probe-only run never pays") }}
	for _, c := range outs {
		a.Candidates.PushBack(c)
	}
	p.realSplits = &realSplits{&fakeSplits{AbstractRebalance: a}}
	a.RebalanceMethods = p.realSplits
	assert.NoError(t, a.setProbeOnly(true))
	return p
}

// The user's case: the cheap ways fail, a dearer one carries. A probe-only
// run finds it as a run would, pays nothing, and says what it carries.
func TestAProbeOnlyRun(t *testing.T) {
	p := probePull(t, 1,
		probeWay{from: "02o1", via: "02cheap", ppm: 100, holds: 1},
		probeWay{from: "02o1", via: "02cheap2", ppm: 200, holds: 1},
		probeWay{from: "02o1", via: "02dear", ppm: 800, holds: 250000000},
		probeWay{from: "02o2", via: "02none", ppm: 100, holds: 1})
	refreshed := 0
	p.refresh = func(...string) error { refreshed++; return nil }
	res := stepRun(t, p.realSplits)
	assert.True(t, res.ProbeOnly)
	assert.Equal(t, [3]uint64{0, 0, 0}, [3]uint64{res.RebalancedAmount, res.FeesPaidMsat, res.AvgPPM},
		"nothing moved, nothing paid: a reader that does not know probe_only sees a run that moved nothing")
	assert.Empty(t, res.Successes)
	assert.NotNil(t, res.Successes)
	assert.Equal(t, 0, refreshed, "nothing moved: no channel to read again")
	assert.Equal(t, int64(6), p.sent.Load(), "the sizing probes count as they are sent")
	assert.Contains(t, p.progress(time.Now()), "rebalance 9x1x1: 200000/300000 sats carried by probes in 0s, 6 probes")
	assert.Equal(t, uint64(200000), res.CarriedAmount, "the dear way carries two splits of the three")
	assert.Equal(t, 1, res.SplitsSucceeded)
	assert.Equal(t, 2, res.SplitsFailed, "1x1x1 found no further way, 2x1x1 none")
	assert.Equal(t, uint64(160000), res.CarriedFeeMsat, "what two splits at 800 ppm would cost")
	assert.Equal(t, uint64(800), res.CarriedAvgPPM)
	if s := res.Carried["1x1x1"]; assert.NotNil(t, s) {
		assert.Equal(t, map[uint64]uint64{800: 200000}, s.ByPPM)
	}
	assert.Equal(t, "every candidate was probed: 1 carried what their routes can; 1 failed", res.Stopped)
	// 1x1x1: two cheap ways fail, the dear one arrives, then sizing: 300k
	// fails, 200k goes (nothing between it and 300k); fired again, it finds
	// no further way (no probe); then 2x1x1 fails once
	assert.Equal(t, []string{"1x1x1 100", "1x1x1 100", "1x1x1 100", "1x1x1 300", "1x1x1 200", "2x1x1 100"}, p.probes)
	assert.Equal(t, uint64(6), res.Attempts)

	// the route is kept for the next run between the two channels, and the
	// candidate's record has a success with nothing moved
	proven := p.Node.ProvenRoute("1x1x1", "9x1x1", false)
	assert.Equal(t, []string{"1x1x1/" + dirOf("02self", "02o1"), "3x2x1/" + dirOf("02o1", "02dear"),
		"3x3x1/" + dirOf("02dear", "02in"), "9x1x1/" + dirOf("02in", "02self")}, proven)
	assert.Nil(t, p.Node.ProvenRoute("2x1x1", "9x1x1", false))
	ranked := p.Node.RankCandidates([]string{"2x1x1/" + dirOf("02self", "02o2"), "1x1x1/" + dirOf("02self", "02o1")}, nil)
	assert.Equal(t, "1x1x1/"+dirOf("02self", "02o1"), ranked[0], "the one that carried comes first")
	record := p.Node.CandidateRecord("1x1x1/" + dirOf("02self", "02o1"))
	assert.Equal(t, [3]uint64{1, 0, 0}, [3]uint64{uint64(record.Successes), record.AmountMsat, record.FeeMsat},
		"it carried a probe: no amount moved, no fee paid")
	assert.Equal(t, uint64(0), uint64(record.Failures), "finding no further way is not the candidate failing")
	assert.False(t, record.Failing)
	assert.Equal(t, uint64(1), uint64(p.Node.CandidateRecord("2x1x1/"+dirOf("02self", "02o2")).Failures))
}

func dirOf(from, to string) string {
	if from < to {
		return "0"
	}
	return "1"
}

// What is found counts up to the amount: two splits at once may each size
// their route to all that is left.
func TestAProbeOnlyRunCoversItsAmount(t *testing.T) {
	p := probePull(t, 2,
		probeWay{from: "02o1", via: "02a", ppm: 400},
		probeWay{from: "02o2", via: "02b", ppm: 600})
	res := stepRun(t, p.realSplits)
	assert.Equal(t, uint64(300000), res.CarriedAmount, "not more than asked for")
	assert.Equal(t, "", res.Stopped)
	// each sized its route to the whole 300k: the first to come back counts
	// in full, the other, with nothing left to cover, not at all
	assert.Equal(t, 1, res.SplitsSucceeded)
	assert.Equal(t, 0, res.SplitsFailed)
	assert.Equal(t, 1, len(res.Carried))
	for _, s := range res.Carried {
		assert.Equal(t, 1, len(s.ByPPM))
		for ppm, sats := range s.ByPPM {
			assert.Equal(t, uint64(300000), sats)
			assert.Equal(t, res.CarriedAvgPPM, ppm)
			assert.Equal(t, 300*ppm, res.CarriedFeeMsat)
			assert.Contains(t, []uint64{400, 600}, ppm)
		}
	}
	assert.Equal(t, 4, len(p.probes), "a split and the whole of what is left, each")

	// what a route carried counts once: one candidate, a way that carries
	// one split only. Fired again, it finds no other way.
	p = probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400, holds: 150000000})
	res = stepRun(t, p.realSplits)
	assert.Equal(t, uint64(100000), res.CarriedAmount)
	assert.Equal(t, 1, res.SplitsSucceeded)
	assert.Equal(t, "every candidate was probed: 1 carried what their routes can; 1 failed", res.Stopped,
		"2x1x1 has no way at all")
	n := 0
	for _, probe := range p.probes {
		if probe == "1x1x1 100" {
			n++
		}
	}
	assert.Equal(t, 1, n, "one split over it, not one per 100k of the amount")
}

// A run keeps firing a candidate: when its first route runs short, the next
// split finds another. So does a probe-only run, leaving out the channels of
// the route it counted, so that what the second carries is on top of it.
func TestAProbeOnlyRunTriesACandidatesOtherRoutes(t *testing.T) {
	p := probePull(t, 1,
		probeWay{from: "02o1", via: "02a", ppm: 400, holds: 150000000},
		probeWay{from: "02o1", via: "02b", ppm: 800, holds: 250000000})
	res := stepRun(t, p.realSplits)
	assert.Equal(t, uint64(300000), res.CarriedAmount, "100k over 02a, then 200k over 02b")
	assert.Equal(t, "", res.Stopped)
	assert.Equal(t, 2, res.SplitsSucceeded)
	if s := res.Carried["1x1x1"]; assert.NotNil(t, s) {
		assert.Equal(t, map[uint64]uint64{400: 100000, 800: 200000}, s.ByPPM)
	}
	assert.Equal(t, uint64(40000+160000), res.CarriedFeeMsat)
	// 02a: a split, 300k and 200k fail; 02b: a split, then the 200k left
	assert.Equal(t, []string{"1x1x1 100", "1x1x1 300", "1x1x1 200", "1x1x1 100", "1x1x1 200"}, p.probes)
	// the first route found is the one kept for the run that pays
	assert.Contains(t, p.Node.ProvenRoute("1x1x1", "9x1x1", false), "1x2x1/"+dirOf("02o1", "02a"))
	// the second split's searches left the first route's channels out
	assert.Equal(t, []string{"1x2x1/" + dirOf("02o1", "02a"), "1x3x1/" + dirOf("02a", "02in"),
		"2x2x1/" + dirOf("02o1", "02b"), "2x3x1/" + dirOf("02b", "02in")}, p.probeExclude())

	// sizing probes that fail say nothing against the node that just
	// carried the split: not among the worst nodes, no score
	assert.Empty(t, res.WorstNodes)
	assert.Empty(t, p.Node.Graph.NodeScores())
}

// Two splits at once may find the same way: nothing moved, so it counts once.
func TestAProbeOnlyRunCountsASharedChannelOnce(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400})
	shared := &rebalance.Result{Status: "success", Candidate: "x", AmountMsat: 100000000, Amount: 100000, Fee: 40000,
		Channels: []string{"1x1x1/0", "5x5x5/0", "9x1x1/0"}}
	assert.True(t, p.probeSucceeded(shared))
	again := &rebalance.Result{Status: "success", Candidate: "y", AmountMsat: 100000000, Amount: 100000, Fee: 40000,
		Channels: []string{"2x1x1/0", "7x7x7/0", "5x5x5/0", "9x1x1/0"}}
	assert.False(t, p.probeSucceeded(again), "it goes over a channel already counted")
	assert.Equal(t, [3]uint64{0, 0, 0}, [3]uint64{again.AmountMsat, again.Amount, again.Fee})
	assert.False(t, p.probeProven["7x7x7/0"], "what is not counted rules nothing out")
	assert.False(t, p.probeProven["1x1x1/0"] || p.probeProven["9x1x1/0"], "our own two channels are on every route")
	other := &rebalance.Result{Status: "success", Candidate: "y", AmountMsat: 300000000, Amount: 300000, Fee: 90000,
		Channels: []string{"2x1x1/0", "8x8x8/0", "9x1x1/0"}}
	p.AmountRebalanced = 100000000
	assert.True(t, p.probeSucceeded(other))
	assert.Equal(t, [3]uint64{200000000, 200000, 60000}, [3]uint64{other.AmountMsat, other.Amount, other.Fee},
		"trimmed to what is left, the fee with it")
	assert.Equal(t, uint64(200000000), p.probeCarried["y"])
	p.AmountRebalanced = 300000000
	late := &rebalance.Result{Status: "success", Candidate: "z", AmountMsat: 100000000, Amount: 100000, Fee: 1,
		Channels: []string{"3x1x1/0", "6x6x6/0", "9x1x1/0"}}
	assert.False(t, p.probeSucceeded(late), "the amount is covered")
	assert.False(t, p.probeProven["6x6x6/0"])
	assert.Equal(t, uint64(0), late.AmountMsat)

	// a whole run: both candidates' only way into 02in is the channel
	// 02hub -> 02in, which carries one split. Both splits' probes arrive;
	// one counts; the other is neither a success nor a failure, searches
	// again without that channel, and finds nothing.
	q := probePull(t, 2,
		probeWay{from: "02o1", via: "02a", ppm: 400, holds: 1},
		probeWay{from: "02o2", via: "02b", ppm: 400, holds: 1})
	g := q.Node.Graph
	netChannel(g, "7x1x1", "02a", "02hub", 0)
	netChannel(g, "7x2x1", "02b", "02hub", 0)
	hub := netChannel(g, "7x3x1", "02hub", "02in", 0)
	q.holds[candidateId(hub)] = 150000000
	res := stepRun(t, q.realSplits)
	assert.Equal(t, uint64(100000), res.CarriedAmount, "once, not once per candidate")
	assert.Equal(t, 1, res.SplitsSucceeded)
	assert.Equal(t, 1, len(res.Carried))
	assert.Equal(t, "every candidate was probed: 1 carried what their routes can; 1 failed", res.Stopped)
}

// A candidate's room bounds what its route is sized to.
func TestProbeWant(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400})
	out := p.Candidates.Front()
	assert.Equal(t, uint64(300000000), p.probeWant(out), "no room known: what is left")
	p.AmountRebalanced = 100000000
	assert.Equal(t, uint64(200000000), p.probeWant(out))
	p.RebalanceMethods = &probeRoom{p.realSplits, 150000000}
	assert.Equal(t, uint64(150000000), p.probeWant(out))
	p.RebalanceMethods = &probeRoom{p.realSplits, 900000000}
	assert.Equal(t, uint64(200000000), p.probeWant(out))
	// what its routes already carried is taken off its room
	p.RebalanceMethods = &probeRoom{p.realSplits, 250000000}
	p.probeCarried = map[string]uint64{candidateId(out): 100000000}
	assert.Equal(t, uint64(150000000), p.probeWant(out))
	p.probeCarried[candidateId(out)] = 300000000
	assert.Equal(t, uint64(0), p.probeWant(out))
	p.probeCarried = nil
	p.RebalanceMethods = &probeRoom{p.realSplits, 900000000}
	split := p.newSplit(out, p.TargetChannel, out, 100000000)
	assert.True(t, split.ProbeOnly)
	assert.Equal(t, uint64(200000000), split.ProbeWant)
}

// outward gives our side of a candidate channel, as circular-pull does.
type outward struct{ *realSplits }

func (o *outward) GetCandidateDirection(id string) string { return dirOf("02self", id) }

type probeRoom struct {
	*realSplits
	sats uint64
}

func (r *probeRoom) room(*glightning.PeerChannel) uint64 { return r.sats }

// probeonly pays nothing, so it has no fee budget; and it runs one round.
func TestSetProbeOnly(t *testing.T) {
	a := &AbstractRebalance{maxFeeMsat: 5000}
	assert.NoError(t, a.setProbeOnly(false))
	assert.False(t, a.probeOnly)
	assert.Equal(t, uint64(5000), a.maxFeeMsat)
	assert.NoError(t, a.setProbeOnly(true))
	assert.True(t, a.probeOnly)
	assert.Equal(t, uint64(0), a.maxFeeMsat)
	assert.Equal(t, errProbeOnlyRounds, (&AbstractRebalance{retryEvery: time.Minute}).setProbeOnly(true))
	assert.Equal(t, errProbeOnlyRounds, (&AbstractRebalance{stepPPM: 100}).setProbeOnly(true))
	assert.NoError(t, (&AbstractRebalance{retryEvery: time.Minute, stepPPM: 100}).setProbeOnly(false))

	// the commands take it, by name, as their last parameter
	for _, cmd := range []jrpc2.ServerMethod{&RebalancePull{}, &RebalancePush{}} {
		assert.NoError(t, jrpc2.ParseNamedParams(cmd, map[string]interface{}{"probeonly": true}))
	}
	pull := &RebalancePull{}
	assert.NoError(t, jrpc2.ParseNamedParams(pull, map[string]interface{}{"inscid": "1x1x1", "probeonly": true}))
	assert.True(t, pull.ProbeOnly)
	// refused before anything is read or fired
	pull = &RebalancePull{InScid: "1x1x1", Amount: 100000, ProbeOnly: true, RetryEvery: 60}
	assert.Equal(t, errProbeOnlyRounds, pull.prepare())
	push := &RebalancePush{OutScid: "1x1x1", Amount: 100000, ProbeOnly: true, StepPPM: 100, CapPPM: 500}
	assert.Equal(t, errProbeOnlyRounds, push.prepare())
}

// The next run between the two channels starts on what the probes found:
// that candidate first, and its first split over the proven route, once.
func TestARunStartsOnWhatAProbeOnlyRunFound(t *testing.T) {
	p := probePull(t, 1, probeWay{from: "02o2", via: "02a", ppm: 400})
	route := []string{"2x1x1/x", "1x2x1/x", "1x3x1/x", "9x1x1/x"}
	p.Node.RememberRoute("2x1x1", "9x1x1", route)
	out1, _ := p.Node.Graph.GetChannel("1x1x1/" + dirOf("02self", "02o1"))
	out2, _ := p.Node.Graph.GetChannel("2x1x1/" + dirOf("02self", "02o2"))
	found := map[string]*graph.Channel{candidateId(out1): out1, candidateId(out2): out2}
	assert.Equal(t, []string{candidateId(out2), candidateId(out1)},
		p.provenFirst([]string{candidateId(out1), candidateId(out2)}, found))
	assert.Equal(t, []string{candidateId(out2), candidateId(out1)},
		p.provenFirst([]string{candidateId(out2), candidateId(out1)}, found), "the others keep their order")

	// the candidates are found in that order
	p.RebalanceMethods = &outward{p.realSplits}
	assert.NoError(t, p.FindCandidates("02in"))
	assert.Equal(t, "2x1x1", p.Candidates.Front().ShortChannelId)
	p.Node.ProvenRoute("2x1x1", "9x1x1", true)
	assert.NoError(t, p.FindCandidates("02in"))
	assert.Equal(t, "1x1x1", p.Candidates.Front().ShortChannelId, "without one: as ranked")
	p.Node.RememberRoute("2x1x1", "9x1x1", route)

	// a probe-only run leaves it for the run that pays
	assert.Nil(t, p.newSplit(out2, p.TargetChannel, out2, 100000000).Route)
	assert.NotNil(t, p.Node.ProvenRoute("2x1x1", "9x1x1", false))
	// a run that pays: its first split over that pair takes it
	p.probeOnly = false
	assert.Nil(t, p.newSplit(out1, p.TargetChannel, out1, 100000000).Route, "another candidate's")
	assert.Equal(t, route, p.newSplit(out2, p.TargetChannel, out2, 100000000).Route)
	assert.Nil(t, p.newSplit(out2, p.TargetChannel, out2, 100000000).Route, "once")
	// its own route of this run comes first
	p.Node.RememberRoute("2x1x1", "9x1x1", route)
	p.routes["2x1x1"] = []string{"own"}
	assert.Equal(t, []string{"own"}, p.newSplit(out2, p.TargetChannel, out2, 100000000).Route)
	assert.NotNil(t, p.Node.ProvenRoute("2x1x1", "9x1x1", false), "not taken")
}

// circular-results says which runs paid nothing.
func TestProbeOnlyRunsAreMarked(t *testing.T) {
	run := startRun("pull", "77x1x1", 1000000, 500)
	probeOnlyRun(run, true)
	other := startRun("pull", "77x1x1", 1000000, 500)
	probeOnlyRun(other, false)
	endRun(run, nil, nil)
	endRun(other, nil, nil)
	listed := listRuns("77x1x1")
	if assert.GreaterOrEqual(t, len(listed), 2) { // newest first
		assert.False(t, listed[0].ProbeOnly)
		assert.True(t, listed[1].ProbeOnly)
	}
}

// A candidate whose route counted is fired again for another, unless the
// amount is covered, it has no room for a split, or its route has no channel
// but our own two (it would be found again).
func TestAProbedCandidateIsFiredAgain(t *testing.T) {
	carried := func(prepare func(p *probeNet), channels ...string) int {
		p := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400})
		out := p.Candidates.Front()
		p.launched[candidateId(out)] = out
		for p.Candidates.Len() > 0 {
			p.Candidates.PopFront()
		}
		if prepare != nil {
			prepare(p)
		}
		if channels == nil {
			channels = []string{"1x1x1/0", "5x5x5/0", "9x1x1/0"}
		}
		res := &rebalance.Result{Status: "success", Candidate: candidateId(out), AmountMsat: 100000000, Amount: 100000,
			Fee: 40000, Channels: channels}
		assert.True(t, p.probeSucceeded(res))
		return p.Candidates.Len()
	}
	assert.Equal(t, 1, carried(nil), "200k are left to cover")
	assert.Equal(t, 0, carried(func(p *probeNet) { p.AmountRebalanced = 200000000 }), "the amount is covered")
	assert.Equal(t, 0, carried(func(p *probeNet) { p.RebalanceMethods = &probeRoom{p.realSplits, 150000000} }),
		"no room for another split")
	assert.Equal(t, 1, carried(func(p *probeNet) { p.RebalanceMethods = &probeRoom{p.realSplits, 200000000} }))
	assert.Equal(t, 0, carried(nil, "1x1x1/0", "9x1x1/0"), "no channel to leave out: it would be found again")
}

func TestProbeRanOut(t *testing.T) {
	// nothing carried: as a run
	p := probePull(t, 1, probeWay{from: "02o1", via: "02a", ppm: 400, holds: 1})
	res := stepRun(t, p.realSplits)
	assert.Equal(t, uint64(0), res.CarriedAmount)
	assert.Equal(t, "no candidates left: 2 failed", res.Stopped)
	assert.Empty(t, res.Carried)
	// all that were tried carried
	a := &AbstractRebalance{probeCarried: map[string]uint64{"a": 5, "b": 0}, gone: map[string]string{"a": goneFailed}}
	assert.Equal(t, "every candidate was probed: 1 carried what their routes can", a.probeRanOut())
	a = &AbstractRebalance{probeCarried: map[string]uint64{"a": 5, "c": 7},
		gone: map[string]string{"a": goneFailed, "b": goneRefused, "d": "depleted"}}
	assert.Equal(t, "every candidate was probed: 2 carried what their routes can; 1 refused by the target, "+
		"1 skipped (depleted)", a.probeRanOut())
}
