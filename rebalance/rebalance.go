package rebalance

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"errors"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"strconv"
	"time"
)

type Rebalance struct {
	OutChannel *graph.Channel
	InChannel  *graph.Channel
	Amount     uint64
	MaxPPM     uint64
	Attempts   int
	MaxHops    int
	Node       *node.Node

	// MinAmount, when above 0 and below Amount, lets a payment that fails for
	// lack of liquidity along the route try the same route again with half the
	// amount, down to MinAmount (msat). A success may then deliver less than
	// Amount.
	MinAmount uint64
	// Route, if set, is tried before searching: the channel ids of a route
	// that worked, as in Result.Channels.
	Route []string

	// pay sends a payment over a route; tests replace payInvoice with it
	pay func(route *graph.Route) (*graph.PrettyRoute, error)
}

// maxShrinks bounds how many times a route is retried with less.
const maxShrinks = 8

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
		exclude = r.Node.Graph.Avoided(time.Now().Unix())
		// payments made besides the attempts: retries of a route with less
		extra = 0
		// the error that ended the run, to tell where it failed
		lastErr error
	)
	exclude[r.Node.Id] = true
	// Our own peers may be avoided as middle hops of other routes, but here
	// they are the ends of ours: excluding them would leave no route.
	delete(exclude, r.OutChannel.Destination)
	delete(exclude, r.InChannel.Source)
	requested := r.Amount
	defer func() { r.Amount = requested }()

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
			extra++
			continue
		}
		if errors.Is(err, errShortOfLiquidity) && sent != nil {
			// the route works, but for less: retry it before searching again
			var shrinks int
			result, shrinks, err = r.shrinkOnRoute(sent, exclude, err)
			extra += shrinks
			r.Amount = requested
		}
		lastErr = err
		if errors.Is(err, util.ErrNoRoute) || errors.As(err, &util.ErrRouteTooExpensive{}) {
			r.Node.Logln(glightning.Debug, err)
			lastError = err.Error()
		}

		// Success
		if err == nil && result != nil {
			result.Attempts = uint64(i + extra)
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

	failure := NewResult("failure", r.Amount/1000, r.OutChannel.Destination, r.InChannel.Source)
	failure.Attempts = uint64(i - 1 + extra)
	failure.FailedAt, failure.Transient = failureSide(lastErr)
	failure.Message = "rebalance failed after " + strconv.Itoa(int(failure.Attempts)) + " attempts."
	failure.Message += lastError

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

// shrinkOnRoute retries the route that failed for lack of liquidity with half
// the amount each time, down to r.MinAmount, while it keeps failing that way.
// It returns the result of the last payment and the number of payments made,
// or failed, unchanged, when shrinking is off or the route cannot be used.
func (r *Rebalance) shrinkOnRoute(sent *graph.Route, exclude map[string]bool, failed error) (*Result, int, error) {
	if r.MinAmount == 0 || r.MinAmount >= r.Amount {
		return nil, 0, failed
	}
	ids := sent.ChannelIds()
	amount := r.Amount
	payments := 0
	for payments < maxShrinks {
		amount = amount / 2 / 1000 * 1000 // whole sats
		if amount < r.MinAmount || amount == 0 {
			break
		}
		route, err := r.Node.Graph.GetRouteOverChannels(ids, amount)
		if err != nil || route.FeePPM() > r.MaxPPM {
			break // base fees weigh more on smaller amounts
		}
		r.Node.Logf(glightning.Debug, "retrying the same route with %d sats", amount/1000)
		r.Amount = amount
		payments++
		result, _, err := r.runAttempt(exclude, route)
		if !errors.Is(err, errShortOfLiquidity) {
			return result, payments, err
		}
		failed = err
	}
	return nil, payments, failed
}

// runAttempt sends one payment: over route if it is not nil, otherwise over
// the route a search finds. It returns the route it sent, if any.
func (r *Rebalance) runAttempt(exclude map[string]bool, route *graph.Route) (*Result, *graph.Route, error) {
	if r.Node.Stopped.Load() {
		return nil, nil, util.ErrCircularStopped
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
