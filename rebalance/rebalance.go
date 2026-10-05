package rebalance

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"errors"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"strconv"
)

type Rebalance struct {
	OutChannel *graph.Channel
	InChannel  *graph.Channel
	Amount     uint64
	MaxPPM     uint64
	Attempts   int
	MaxHops    int
	Node       *node.Node
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
		// skips them on subsequent tries within the same Run() call.
		exclude = map[string]bool{r.Node.Id: true}
	)
	for i <= r.Attempts {
		r.Node.Logln(glightning.Debug, "===================== ATTEMPT ", i, " =====================")

		// One search returns the route with the fewest hops within maxppm,
		// and the cheapest of those.
		result, err := r.runAttempt(exclude)
		if err == util.ErrNoRoute || errors.As(err, &util.ErrRouteTooExpensive{}) {
			r.Node.Logln(glightning.Debug, err)
			lastError = err.Error()
		}

		// Success
		if err == nil && result != nil {
			result.Attempts = uint64(i)
			r.Node.Logln(glightning.Info, result.Message)
			if result.Route != nil {
				r.Node.Logln(glightning.Info, "\n"+result.Route.String())
			}
			r.Node.Logln(glightning.Debug, result)
			return result
		}

		// Payment attempt was consumed
		i++

		if err == util.ErrNoRoute || errors.As(err, &util.ErrRouteTooExpensive{}) {
			lastError = " Unable to find a route with at most " +
				strconv.Itoa(r.MaxHops) + " hops. " + lastError
			break
		}

		// sendpay timeout
		if err == util.ErrSendPayTimeout {
			lastError = "rebalancing timed out after " +
				strconv.Itoa(node.SENDPAY_TIMEOUT) +
				"s."
			break
		}

		// wire fee insufficient. Most likely someone in the route has updated their fees, and gossip didn't reach us yet.
		if err == util.ErrWireFeeInsufficient {
			lastError = "wire fee insufficient. Most likely someone in the route has updated their fees, and gossip didn't reach us yet."
			break
		}

		if err != util.ErrTemporaryFailure {
			lastError = err.Error()
			break
		}
	}

	failure := NewResult("failure", r.Amount/1000, r.OutChannel.Destination, r.InChannel.Source)
	failure.Attempts = uint64(i - 1)
	failure.Message = "rebalance failed after " + strconv.Itoa(int(failure.Attempts)) + " attempts."
	failure.Message += lastError

	return failure
}

func (r *Rebalance) runAttempt(exclude map[string]bool) (*Result, error) {
	if r.Node.Stopped.Load() {
		return nil, util.ErrCircularStopped
	}

	if err := r.validateLiquidityParameters(r.OutChannel, r.InChannel); err != nil {
		return nil, err
	}

	route, err := r.tryRoute(exclude)
	if err != nil {
		return nil, err
	}

	result := NewResult("success", r.Amount/1000,
		r.OutChannel.Destination, r.InChannel.Source)

	result.Fee = route.Fee
	result.PPM = route.FeePPM
	result.Route = route
	result.Message = fmt.Sprintf("successfully rebalanced %d sats from %s to %s at %d ppm. Total fees paid: %.3f sats",
		result.Amount, r.Node.Graph.GetAlias(r.OutChannel.Destination), r.Node.Graph.GetAlias(r.InChannel.Source),
		result.PPM, float64(result.Fee)/1000)

	if route.InboundSavingsMSat != 0 {
		if route.InboundSavingsMSat > 0 {
			result.Message += fmt.Sprintf(" (saved %d msat / %d ppm due to inbound discounts)", route.InboundSavingsMSat, route.InboundSavingsPPM)
		} else {
			result.Message += fmt.Sprintf(" (extra %d msat / %d ppm due to inbound surcharges)", -route.InboundSavingsMSat, -route.InboundSavingsPPM)
		}
	}

	return result, nil
}
