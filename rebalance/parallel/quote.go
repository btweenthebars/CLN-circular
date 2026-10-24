package parallel

import (
	"circular/graph"
	"circular/node"
	rebalance2 "circular/rebalance"
	"circular/util"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/elementsproject/glightning/jrpc2"
)

// RebalanceQuote is circular-quote: it takes the parameters of
// circular-pull (inscid), circular-push (outscid) or circular (both), and
// says what a run would try first, without paying anything.
type RebalanceQuote struct {
	InScid             string   `json:"inscid,omitempty"`
	OutScid            string   `json:"outscid,omitempty"`
	OutList            []string `json:"outlist,omitempty"`
	InList             []string `json:"inlist,omitempty"`
	MaxOutPPM          uint64   `json:"maxoutppm,omitempty"`
	MinOutPPM          uint64   `json:"minoutppm,omitempty"`
	Amount             uint64   `json:"amount,omitempty"`
	MaxPPM             uint64   `json:"maxppm,omitempty"`
	Splits             int      `json:"splits,omitempty"`
	SplitAmount        uint64   `json:"splitamount,omitempty"`
	MinSplit           uint64   `json:"minsplit,omitempty"`
	DepleteUpToPercent float64  `json:"depleteuptopercent,omitempty"`
	DepleteUpToAmount  uint64   `json:"depleteuptoamount,omitempty"`
	FillUpToPercent    float64  `json:"filluptopercent,omitempty"`
	FillUpToAmount     uint64   `json:"filluptoamount,omitempty"`
	Attempts           int      `json:"attempts,omitempty"`
	MaxHops            int      `json:"maxhops,omitempty"`
	MaxNodeFails       int      `json:"maxnodefails,omitempty"`
	MaxFailStreak      int      `json:"maxfailstreak,omitempty"` // accepted, so a run's call can be quoted; no payment is made
	RiskPPM            int      `json:"riskppm,omitempty"`
	MaxTime            int      `json:"maxtime,omitempty"` // accepted, like maxfailstreak
}

func (q *RebalanceQuote) Name() string {
	return "circular-quote"
}

func (q *RebalanceQuote) New() interface{} {
	return &RebalanceQuote{}
}

func (q *RebalanceQuote) Call() (jrpc2.Result, error) {
	switch {
	case q.InScid != "" && q.OutScid != "":
		return q.quotePair(node.GetNode())
	case q.InScid != "":
		pull := &RebalancePull{InScid: q.InScid, OutList: q.OutList, MaxOutPPM: q.MaxOutPPM, Amount: q.Amount,
			MaxPPM: q.MaxPPM, Splits: q.Splits, SplitAmount: q.SplitAmount, MinSplit: q.MinSplit,
			DepleteUpToPercent: q.DepleteUpToPercent, DepleteUpToAmount: q.DepleteUpToAmount,
			Attempts: q.Attempts, MaxHops: q.MaxHops, MaxNodeFails: q.MaxNodeFails, RiskPPM: q.RiskPPM}
		if err := pull.prepare(); err != nil {
			return nil, err
		}
		return pull.quote("pull", true), nil
	case q.OutScid != "":
		push := &RebalancePush{OutScid: q.OutScid, InList: q.InList, MinOutPPM: q.MinOutPPM, Amount: q.Amount,
			MaxPPM: q.MaxPPM, Splits: q.Splits, SplitAmount: q.SplitAmount, MinSplit: q.MinSplit,
			FillUpToPercent: q.FillUpToPercent, FillUpToAmount: q.FillUpToAmount,
			Attempts: q.Attempts, MaxHops: q.MaxHops, MaxNodeFails: q.MaxNodeFails, RiskPPM: q.RiskPPM}
		if err := push.prepare(); err != nil {
			return nil, err
		}
		return push.quote("push", false), nil
	}
	return nil, util.ErrNoRequiredParameter
}

// quotePair quotes one rebalance from OutScid to InScid, as circular would
// send it: amount is the payment.
func (q *RebalanceQuote) quotePair(n *node.Node) (*Quote, error) {
	out, err := n.GetOutgoingChannelFromScid(q.OutScid)
	if err != nil {
		return nil, err
	}
	in, err := n.GetIncomingChannelFromScid(q.InScid)
	if err != nil {
		return nil, err
	}
	amount := q.Amount * 1000
	if amount == 0 {
		amount = rebalance2.DEFAULT_AMOUNT
	}
	maxPPM, maxHops := q.MaxPPM, q.MaxHops
	if maxPPM == 0 {
		maxPPM = rebalance2.DEFAULT_MAXPPM
	}
	if maxHops <= 0 {
		maxHops = rebalance2.DEFAULT_MAXHOPS
	}
	result := &Quote{Mode: "pair", Target: q.InScid, TargetPeer: n.Graph.GetAlias(in.Source),
		Amount: amount / 1000, MaxPPM: maxPPM}
	rebalance := rebalance2.NewRebalance(out, in, amount, maxPPM, q.Attempts, maxHops)
	rebalance.Node = n
	rebalance.RiskPPM = q.RiskPPM
	result.add(quoteCandidate(n.Graph, out, out.Destination, rebalance))
	result.summarize()
	return result, nil
}

// Quote is what circular-quote returns: for each candidate, in the order a
// run would try them, the route its first payment would take, or why it would
// not be sent.
type Quote struct {
	Mode       string            `json:"mode"`        // pull, push or pair
	Target     string            `json:"target"`      // the target channel
	TargetPeer string            `json:"target_peer"` // its peer's alias
	Amount     uint64            `json:"amount"`      // sats per payment
	MaxPPM     uint64            `json:"maxppm"`
	Summary    string            `json:"summary"`
	Candidates []*QuoteCandidate `json:"candidates"`
}

// QuoteCandidate is one candidate channel's quote. Estimate is a rough
// chance that the route can carry the payment (see graph.Graph.Estimate),
// counting GuessedHops hops whose liquidity no payment has told us about.
type QuoteCandidate struct {
	Scid        string   `json:"scid"`
	Alias       string   `json:"alias"`
	NodeId      string   `json:"node_id"`
	PPM         *uint64  `json:"ppm,omitempty"`
	FeeMsat     *uint64  `json:"fee_msat,omitempty"`
	Hops        int      `json:"hops,omitempty"`
	Path        []string `json:"path,omitempty"` // from our out-peer to our in-peer
	Estimate    *float64 `json:"estimate,omitempty"`
	GuessedHops *int     `json:"guessed_hops,omitempty"`
	CheapestPPM uint64   `json:"cheapest_ppm,omitempty"` // when every route costs more than maxppm
	Error       string   `json:"error,omitempty"`        // why its first payment would not be sent
	Skipped     string   `json:"skipped,omitempty"`      // why a run would not use it
}

// quote quotes the candidates found by prepare, in the order a run tries
// them; pull says whether they are out channels (circular-pull) or in
// channels (circular-push).
func (r *AbstractRebalance) quote(mode string, pull bool) *Quote {
	targetPeer := r.TargetChannel.Destination
	if pull {
		targetPeer = r.TargetChannel.Source
	}
	result := &Quote{Mode: mode, Target: r.TargetChannel.ShortChannelId, TargetPeer: r.Node.Graph.GetAlias(targetPeer),
		Amount: r.splitAmount / 1000, MaxPPM: r.maxPPM}
	r.QueueLock.Lock()
	candidates := make([]*graph.Channel, r.Candidates.Len())
	for i := range candidates {
		candidates[i] = r.Candidates.At(i)
	}
	r.QueueLock.Unlock()

	for _, candidate := range candidates {
		peer := candidate.Source
		out, in := r.TargetChannel, candidate
		if pull {
			peer = candidate.Destination
			out, in = candidate, r.TargetChannel
		}
		peerChannel, err := r.Node.GetPeerChannelFromGraphChannel(candidate)
		if err == nil {
			err = r.CanUseChannel(peerChannel)
		}
		if err != nil {
			result.add(&QuoteCandidate{Scid: candidate.ShortChannelId, Alias: r.Node.Graph.GetAlias(peer),
				NodeId: peer, Skipped: err.Error()})
			continue
		}
		// our channels, as last read from lightningd, as a run would take them
		if fresh, err := r.Node.Graph.GetChannel(candidateId(out)); err == nil {
			out = fresh
		}
		if fresh, err := r.Node.Graph.GetChannel(candidateId(in)); err == nil {
			in = fresh
		}
		rebalance := rebalance2.NewRebalance(out, in, r.splitAmount, r.maxPPM, r.attempts, r.maxHops)
		rebalance.Node = r.Node
		rebalance.RiskPPM = r.riskPPM
		result.add(quoteCandidate(r.Node.Graph, candidate, peer, rebalance))
	}
	result.summarize()
	return result
}

// quoteCandidate quotes rebalance's first payment, for candidate, whose
// peer is peer.
func quoteCandidate(g *graph.Graph, candidate *graph.Channel, peer string, rebalance *rebalance2.Rebalance) *QuoteCandidate {
	c := &QuoteCandidate{Scid: candidate.ShortChannelId, Alias: g.GetAlias(peer), NodeId: peer}
	route, err := rebalance.Quote()
	if route != nil {
		ppm, fee := route.FeePPM(), route.Fee()
		c.PPM, c.FeeMsat, c.Hops = &ppm, &fee, len(route.Hops)
		for _, hop := range route.Hops[1:] {
			c.Path = append(c.Path, g.GetAlias(hop.Source))
		}
		chance, guessed := g.Estimate(route)
		chance = math.Round(chance*100) / 100
		c.Estimate, c.GuessedHops = &chance, &guessed
	}
	if err != nil {
		c.Error = err.Error()
		var tooExpensive util.ErrRouteTooExpensive
		if errors.As(err, &tooExpensive) {
			c.CheapestPPM = tooExpensive.FeePPM
		}
	}
	return c
}

func (q *Quote) add(c *QuoteCandidate) {
	q.Candidates = append(q.Candidates, c)
}

// summarize says in one line how many candidates would send a payment, at
// what cost, and why the others would not.
func (q *Quote) summarize() {
	var routed, failed, skipped int
	var cheapest, cheapestAbove uint64
	for _, c := range q.Candidates {
		switch {
		case c.Skipped != "":
			skipped++
		case c.Error == "" && c.PPM != nil:
			routed++
			if routed == 1 || *c.PPM < cheapest {
				cheapest = *c.PPM
			}
		default:
			failed++
			if c.CheapestPPM != 0 && (cheapestAbove == 0 || c.CheapestPPM < cheapestAbove) {
				cheapestAbove = c.CheapestPPM
			}
		}
	}
	parts := []string{fmt.Sprintf("%d of %d candidates have a route within %d ppm", routed, len(q.Candidates), q.MaxPPM)}
	if routed > 0 {
		parts[0] += fmt.Sprintf(" (cheapest %d ppm)", cheapest)
	}
	if failed > 0 {
		part := fmt.Sprintf("%d would fail", failed)
		if cheapestAbove > 0 {
			part += fmt.Sprintf(" (the cheapest route above maxppm is %d ppm)", cheapestAbove)
		}
		parts = append(parts, part)
	}
	if skipped > 0 {
		parts = append(parts, fmt.Sprintf("%d skipped", skipped))
	}
	q.Summary = strings.Join(parts, "; ")
}
