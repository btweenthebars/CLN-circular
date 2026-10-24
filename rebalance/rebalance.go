package rebalance

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"errors"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"strconv"
	"strings"
	"sync/atomic"
)

type Rebalance struct {
	OutChannel *graph.Channel
	InChannel  *graph.Channel
	Amount     uint64
	MaxPPM     uint64
	Attempts   int
	MaxHops    int
	Node       *node.Node

	// MinAmount, when above 0 and below Amount, lets a rebalance whose route
	// failed for lack of liquidity, and which then finds no other route, send
	// half the amount over that route, down to MinAmount (msat). Each such
	// payment is one of the attempts. A success may then deliver less than
	// Amount.
	MinAmount uint64
	// Route, if set, is tried before searching: the channel ids of a route
	// that worked, as in Result.Channels.
	Route []string

	// pay sends a payment over a route; tests replace payInvoice with it
	pay func(route *graph.Route) (*graph.PrettyRoute, error)
	// refresh reads our channels with peers again; tests replace
	// Node.RefreshPeerChannels with it
	refresh func(peerIds ...string) error

	// MaxNodeFails bounds how many of a rebalance's payments one node may
	// fail to forward for lack of liquidity: each failure only rules out the
	// channel that failed, and a drained node with many channels would
	// otherwise take one payment per channel. The rest of the rebalance
	// routes around such a node, and ends if it is the out-peer, which every
	// route goes through. 0 means DEFAULT_MAX_NODE_FAILS.
	MaxNodeFails int

	// RiskPPM, when above 0, is how much a route's chance of success weighs
	// against its fees for this rebalance (see graph.GetLikelyCircularRoute);
	// below 0, routes are chosen by fees and hops alone; 0: the node's
	// circular-risk-ppm.
	RiskPPM int
	// Halt, if set, ends the rebalance before its next payment once it reads
	// true: the parallel commands set it when they stop, so that splits in
	// flight do not go on through their attempts.
	Halt *atomic.Bool
	// Sent, if set, counts each payment as it is sent: the parallel commands
	// share one between their splits, for their progress lines.
	Sent *atomic.Int64

	// nodeFailures counts, per node, the payments of this run that it could
	// not forward for lack of liquidity
	nodeFailures map[string]int
	// failedAt counts, per node, the payments of this run it failed, for
	// whatever reason, other than on our own channels (Result.FailedNodes)
	failedAt map[string]int
	// payments counts the payments this run sent
	payments int
}

// errRunStopped ends a split whose run stopped firing splits: neither of
// its channels is to blame.
var errRunStopped = errors.New("the run stopped: no more payments for this split")

// riskPPM is the risk rate for this rebalance's searches: r.RiskPPM, or the
// node's circular-risk-ppm.
func (r *Rebalance) riskPPM() uint64 {
	switch {
	case r.RiskPPM > 0:
		return uint64(r.RiskPPM)
	case r.RiskPPM < 0:
		return 0
	}
	return r.Node.RiskPPM
}

// maxNodeFails is r.MaxNodeFails, or its default.
func (r *Rebalance) maxNodeFails() int {
	if r.MaxNodeFails <= 0 {
		return DEFAULT_MAX_NODE_FAILS
	}
	return r.MaxNodeFails
}

func NewRebalance(outChannel, inChannel *graph.Channel, amount, maxppm uint64, attempts, maxHops int) *Rebalance {
	return &Rebalance{
		OutChannel: outChannel,
		InChannel:  inChannel,
		Amount:     amount,
		MaxPPM:     maxppm,
		Attempts:   attempts,
		MaxHops:    maxHops,
		Node:       node.GetNode(),
	}
}

func (r *Rebalance) Setup() error {
	r.setDefaults()

	if err := r.validateLiquidityParameters(r.OutChannel, r.InChannel); err != nil {
		return err
	}

	return nil
}

func (r *Rebalance) Run() *Result {
	var (
		i         = 1
		lastError = ""
		// exclude accumulates failing channels across attempts so the pathfinder
		// skips them on subsequent tries within the same Run() call. It starts
		// with what recent rebalances learned to avoid.
		exclude = r.startExclude()
		// the last route that failed for lack of liquidity: sent again with
		// less once no other route is found
		short *graph.Route
		// the channels this run excluded because short ran out on them
		shortIds []string
		// the error that ended the run, to tell where it failed
		lastErr error
	)
	requested := r.Amount
	defer func() { r.Amount = requested }()
	r.nodeFailures = map[string]int{}
	r.failedAt = map[string]int{}
	r.payments = 0

	// a route that worked before needs no search
	known := r.knownRoute(exclude)
	for i <= r.Attempts {
		r.Node.Logln(glightning.Debug, "===================== ATTEMPT ", i, " =====================")

		// One search returns the route with the fewest hops within maxppm,
		// and the cheapest of those.
		reused := known != nil
		result, sent, err := r.runAttempt(exclude, known)
		known = nil
		if reused && err != nil && errors.Is(err, util.ErrTemporaryFailure) {
			// The route that worked no longer does: what failed is learned and
			// excluded. Search in the same attempt, without shrinking a route
			// that was not searched for.
			r.Node.Logln(glightning.Debug, "the route that worked before failed: searching")
			continue
		}
		if errors.Is(err, errShortOfLiquidity) && sent != nil {
			// the route works, but for less: other routes are searched first
			short, shortIds = sent, excludedChannels(sent, exclude)
		}
		if short != nil && (errors.Is(err, util.ErrNoRoute) || errors.As(err, &util.ErrRouteTooExpensive{})) {
			// no other route: this attempt sends less over the short one, and
			// later attempts search for the smaller amount
			if shrunk, ok := r.shrink(short, exclude, shortIds); ok {
				r.Node.Logf(glightning.Debug, "no other route: retrying the route that was short with %d sats", r.Amount/1000)
				result, sent, err = r.runAttempt(exclude, shrunk)
				if errors.Is(err, errShortOfLiquidity) && sent != nil {
					shortIds = append(shortIds, excludedChannels(sent, exclude)...)
				} else {
					// it failed for another reason: the route is not tried again
					short = nil
				}
			}
		}
		lastErr = err
		if errors.Is(err, util.ErrNoRoute) || errors.As(err, &util.ErrRouteTooExpensive{}) {
			r.Node.Logln(glightning.Debug, err)
			lastError = err.Error()
		}

		// Success
		if err == nil && result != nil {
			result.Attempts = uint64(r.payments)
			result.FailedNodes = r.failedAt
			r.Node.Logln(glightning.Info, result.Message)
			if result.Route != nil {
				r.Node.Logln(glightning.Info, "\n"+result.Route.String())
			}
			r.Node.Logln(glightning.Debug, result)
			return result
		}

		// Payment attempt was consumed
		i++

		if errors.Is(err, util.ErrNoRoute) || errors.As(err, &util.ErrRouteTooExpensive{}) {
			lastError = " Unable to find a route with at most " +
				strconv.Itoa(r.MaxHops) + " hops. " + lastError
			break
		}

		// sendpay timeout
		if errors.Is(err, util.ErrSendPayTimeout) {
			lastError = "rebalancing timed out after " +
				strconv.Itoa(node.SENDPAY_TIMEOUT) +
				"s."
			break
		}

		// wire fee insufficient. Most likely someone in the route has updated their fees, and gossip didn't reach us yet.
		if errors.Is(err, util.ErrWireFeeInsufficient) {
			lastError = "wire fee insufficient. Most likely someone in the route has updated their fees, and gossip didn't reach us yet."
			break
		}

		if !errors.Is(err, util.ErrTemporaryFailure) {
			lastError = err.Error()
			break
		}
	}

	r.Amount = requested
	failure := NewResult("failure", r.Amount/1000, r.OutChannel.Destination, r.InChannel.Source)
	failure.Attempts = uint64(r.payments)
	failure.FailedNodes = r.failedAt
	failure.FailedAt, failure.Retry = failureSide(lastErr)
	failure.Precheck = isPrecheck(lastErr)
	failure.Halted = errors.Is(lastErr, errRunStopped)
	var tooExpensive util.ErrRouteTooExpensive
	if errors.As(lastErr, &tooExpensive) {
		failure.CheapestPPM = tooExpensive.FeePPM
	}
	failure.Message = "rebalance failed after " + strconv.Itoa(int(failure.Attempts)) + " payments."
	if lastError == "" && lastErr != nil {
		// the attempts ran out: say how the last one failed
		lastError = "The last one failed: " + lastErr.Error()
	}
	if lastError = strings.TrimSpace(lastError); lastError != "" {
		failure.Message += " " + lastError
	}

	return failure
}

// knownRoute prices r.Route for r.Amount, or returns nil if it cannot be
// used: a channel is gone or disabled, it now costs more than maxppm, the
// graph believes a channel along it can no longer carry the amount (for
// example because the last payment used it up), or it passes through a node
// or channel to avoid.
func (r *Rebalance) knownRoute(exclude map[string]bool) *graph.Route {
	if len(r.Route) == 0 {
		return nil
	}
	route, err := r.Node.Graph.GetRouteOverChannels(r.Route, r.Amount)
	if err != nil || route.FeePPM() > r.MaxPPM || !r.Node.Graph.CanCarry(route, exclude) {
		r.Node.Logln(glightning.Debug, "the route that worked before cannot be used again: searching")
		return nil
	}
	return route
}

// excludedChannels returns the channels of route that exclude holds: right
// after a payment over it failed for lack of liquidity, the channel that ran
// short.
func excludedChannels(route *graph.Route, exclude map[string]bool) []string {
	var ids []string
	for _, id := range route.ChannelIds() {
		if exclude[id] {
			ids = append(ids, id)
		}
	}
	return ids
}

// shrink halves r.Amount, in whole sats, and prices route for it. It returns
// false, leaving r.Amount as it was, when shrinking is off, the half is below
// r.MinAmount, or the route can no longer be used: a channel is gone or
// disabled, it costs more than maxppm for less (base fees weigh more on
// smaller amounts), or what was learned since it ran short rules it out. Its
// channels in shortIds were excluded only for being short of the larger
// amounts, so the graph's beliefs about them decide.
func (r *Rebalance) shrink(route *graph.Route, exclude map[string]bool, shortIds []string) (*graph.Route, bool) {
	if r.MinAmount == 0 {
		return nil, false
	}
	amount := r.Amount / 2 / 1000 * 1000
	if amount < r.MinAmount || amount == 0 {
		r.Node.Logf(glightning.Debug, "the route that was short is not tried with %d sats: below the minimum of %d sats", amount/1000, r.MinAmount/1000)
		return nil, false
	}
	shrunk, err := r.Node.Graph.GetRouteOverChannels(route.ChannelIds(), amount)
	if err != nil {
		r.Node.Logln(glightning.Debug, "the route that was short cannot be used again: ", err)
		return nil, false
	}
	if shrunk.Fee() > graph.MaxFeeForPPM(amount, r.MaxPPM) {
		r.Node.Logf(glightning.Debug, "the route that was short charges %d msat for %d sats, more than maxppm %d allows", shrunk.Fee(), amount/1000, r.MaxPPM)
		return nil, false
	}
	others := make(map[string]bool, len(exclude))
	for id := range exclude {
		others[id] = true
	}
	for _, id := range shortIds {
		delete(others, id)
	}
	if !r.Node.Graph.CanCarry(shrunk, others) {
		r.Node.Logf(glightning.Debug, "the route that was short is not tried with %d sats: what was learned since rules it out", amount/1000)
		return nil, false
	}
	r.Amount = amount
	return shrunk, true
}

// runAttempt sends one payment: over route if it is not nil, otherwise over
// the route a search finds. It returns the route it sent, if any.
func (r *Rebalance) runAttempt(exclude map[string]bool, route *graph.Route) (*Result, *graph.Route, error) {
	if r.Node.Stopped.Load() {
		return nil, nil, util.ErrCircularStopped
	}
	if r.Halt != nil && r.Halt.Load() {
		return nil, nil, errRunStopped
	}

	if err := r.validateLiquidityParameters(r.OutChannel, r.InChannel); err != nil {
		return nil, nil, err
	}

	if route == nil {
		var err error
		if route, err = r.getRoute(exclude); err != nil {
			return nil, nil, err
		}
	}
	prettyRoute, err := r.tryRoute(route, exclude)
	if err != nil {
		return nil, route, err
	}

	result := NewResult("success", route.Amount/1000,
		r.OutChannel.Destination, r.InChannel.Source)
	result.AmountMsat = route.Amount
	result.Channels = route.ChannelIds()

	result.Fee = prettyRoute.Fee
	result.PPM = prettyRoute.FeePPM
	result.Route = prettyRoute
	result.Message = fmt.Sprintf("successfully rebalanced %d sats from %s to %s at %d ppm. Total fees paid: %.3f sats",
		result.Amount, r.Node.Graph.GetAlias(r.OutChannel.Destination), r.Node.Graph.GetAlias(r.InChannel.Source),
		result.PPM, float64(result.Fee)/1000)

	if prettyRoute.InboundSavingsMSat != 0 {
		if prettyRoute.InboundSavingsMSat > 0 {
			result.Message += fmt.Sprintf(" (saved %d msat / %d ppm due to inbound discounts)", prettyRoute.InboundSavingsMSat, prettyRoute.InboundSavingsPPM)
		} else {
			result.Message += fmt.Sprintf(" (extra %d msat / %d ppm due to inbound surcharges)", -prettyRoute.InboundSavingsMSat, -prettyRoute.InboundSavingsPPM)
		}
	}

	return result, nil, nil
}
