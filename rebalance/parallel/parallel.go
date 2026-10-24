package parallel

import (
	"circular/graph"
	"circular/node"
	rebalance2 "circular/rebalance"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/gammazero/deque"
	"sync"
	"sync/atomic"
)

type RebalanceMethods interface {
	IsGoodCandidate(peerChannel *glightning.PeerChannel) bool
	CanUseChannel(channel *glightning.PeerChannel) error
	Fire(candidate *graph.Channel, amount uint64)
	EnqueueCandidate(result *rebalance2.Result)
	GetCandidateDirection(id string) string
	AddSuccess(result *rebalance2.Result)
}

type AbstractRebalance struct {
	TotalAttempts       uint64
	Node                *node.Node
	TargetChannel       *graph.Channel
	Candidates          *deque.Deque[*graph.Channel]
	AmountRebalanced    uint64
	InFlightAmount      uint64
	AmountLock          *sync.Mutex
	QueueLock           *sync.Mutex
	RebalanceResultChan chan *rebalance2.Result
	CandidatesList      []string
	Result              *Result
	amount              uint64
	maxPPM              uint64
	splits              int
	splitAmount         uint64
	minSplit            uint64 // msat: a split whose route was short, with no other route left, sends less over it, down to this
	attempts            int
	maxNodeFails        int // see Rebalance.MaxNodeFails
	// maxFailStreak: after this many failed payments in a row, across
	// splits, with no split succeeding in between, no more splits are fired
	// (0: DEFAULT_MAX_FAIL_STREAK; below 0: never). failStreak counts them.
	maxFailStreak int
	failStreak    int
	streakStopped bool
	// halt is read by the splits in flight before each payment: set when the
	// run stops firing splits, it ends them too
	halt atomic.Bool
	// riskPPM: see Rebalance.RiskPPM
	riskPPM int
	// movedMsat is what the successful splits delivered, for Result.AvgPPM
	movedMsat      uint64
	maxHops        int
	splitsInFlight int
	// routes holds, for each candidate's short channel id, the channel ids of
	// the last route that worked for it: its next split tries that route
	// before searching.
	routes map[string][]string
	// candidateSide is the side of the route our candidates are on: the out
	// channel for circular-pull, the in channel for circular-push
	candidateSide rebalance2.Side
	// launched holds the candidates fired, by id, and retried those given
	// another try after a stale policy was read again
	launched map[string]*graph.Channel
	retried  map[string]bool
	// targetCap, if not 0, is how many splits the target channel takes at
	// once, learned when it refused one. capSuccesses counts the splits that
	// succeeded since it last changed, and capSetAt is the number of splits
	// fired by then. probing says it grew and the growth is not judged yet;
	// probeSeq is the number of the split that judges it, once fired.
	// capBackoff multiplies the successes needed before it grows (0 counts
	// as 1).
	targetCap    int
	capSuccesses int
	capSetAt     int
	probing      bool
	probeSeq     int
	capBackoff   int
	// fireSeq counts the splits fired, and firedAt holds, by candidate, the
	// number of the split in flight for it
	fireSeq int
	firedAt map[string]int
	// busyRefusals counts, by candidate, the busy refusals by the target its
	// limit does not explain
	busyRefusals map[string]int
	// targetRechecked: the target was read again after refusing with nothing
	// in flight, and nothing succeeded since; refusedAlone: the candidate it
	// refused alone after that; targetStopped: it takes no more
	targetRechecked bool
	refusedAlone    string
	targetStopped   bool
	// refresh reads our channels with peers again; tests replace
	// Node.RefreshPeerChannels with it
	refresh func(peerIds ...string) error
	RebalanceMethods
}

func (r *AbstractRebalance) Init(amount, maxppm, splitamount, minsplit uint64, splits, attempts, maxhops int) {
	r.Node = node.GetNode()
	r.AmountLock = &sync.Mutex{}
	r.QueueLock = &sync.Mutex{}
	r.TotalAttempts = 0
	r.Node.Logf(glightning.Debug, "%+v", r)
	r.amount = amount
	r.maxPPM = maxppm
	r.splitAmount = splitamount
	r.minSplit = minsplit
	r.splits = splits
	r.routes = make(map[string][]string)
	r.launched = make(map[string]*graph.Channel)
	r.retried = make(map[string]bool)
	r.attempts = attempts
	r.maxHops = maxhops
	r.setGenericDefaults()
	r.RebalanceResultChan = make(chan *rebalance2.Result, r.splits)
	r.Node.Logln(glightning.Debug, "AbstractRebalance initialized")
}

// launch runs one split of amount (msat) from out to in, through candidate,
// one of the two, and sends its result on RebalanceResultChan.
func (r *AbstractRebalance) launch(out, in, candidate *graph.Channel, amount uint64) {
	key := candidate.ShortChannelId
	r.candidateSide = rebalance2.SideIn
	if candidate == out {
		r.candidateSide = rebalance2.SideOut
	}
	r.launched[candidateId(candidate)] = candidate
	// our channels, as last read from lightningd: their fees may have changed
	// since the run started
	if fresh, err := r.Node.Graph.GetChannel(candidateId(out)); err == nil {
		out = fresh
	}
	if fresh, err := r.Node.Graph.GetChannel(candidateId(in)); err == nil {
		in = fresh
	}
	r.Node.Logln(glightning.Debug, "Firing candidate: ", key, " for ", amount/1000, " sats, attempts: ", r.attempts)
	rebalance := rebalance2.NewRebalance(out, in, amount, r.maxPPM, r.attempts, r.maxHops)
	rebalance.Node = r.Node
	rebalance.MaxNodeFails = r.maxNodeFails
	rebalance.RiskPPM = r.riskPPM
	rebalance.Halt = &r.halt
	rebalance.MinAmount = r.minSplit
	if rebalance.MinAmount > amount {
		rebalance.MinAmount = amount
	}
	rebalance.Route = r.routes[key]

	go func() {
		result := rebalance2.NewResult("failure", amount/1000, out.Destination, in.Source)
		defer func() {
			if rec := recover(); rec != nil {
				r.Node.Logf(glightning.Unusual, "panic in rebalance goroutine: %v", rec)
			}
			result.Requested = amount
			result.Candidate = candidateId(candidate)
			r.RebalanceResultChan <- result
		}()
		result = rebalance.Run()
	}()
}

// candidateId is the id ("scid/direction") of a candidate channel.
func candidateId(c *graph.Channel) string {
	return c.ShortChannelId + "/" + util.GetDirection(c.Source, c.Destination)
}
