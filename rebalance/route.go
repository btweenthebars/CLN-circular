package rebalance

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"errors"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"math"
	"math/bits"
	"time"
)

func (r *Rebalance) getRoute(exclude map[string]bool) (*graph.Route, error) {
	defer util.TimeTrack(time.Now(), "rebalance.getRoute", r.Node.Logf)

	src := r.OutChannel.Destination
	dst := r.InChannel.Source

	r.Node.Logln(glightning.Debug, "looking for a route from ", r.Node.Graph.GetAlias(src), " to ", r.Node.Graph.GetAlias(dst))
	maxFee := graph.MaxFeeForPPM(r.Amount, r.MaxPPM)
	if inFee := r.Node.Graph.LeastForwardFee(r.InChannel, r.OutChannel, r.Amount); inFee > maxFee {
		// what the in-peer charges to send to us, with the best of its inbound
		// discounts, is more than maxppm allows: no route can do, whatever the
		// out channel
		// above a limit of this split's own, but within the run's: the split
		// is not worth it, which says nothing of either channel
		own := r.RunMaxPPM > r.MaxPPM && inFee <= graph.MaxFeeForPPM(r.Amount, r.RunMaxPPM)
		limit := r.MaxPPM
		if r.RunMaxPPM > r.MaxPPM && !own {
			limit = r.RunMaxPPM // the one it is above
		}
		err := fmt.Errorf("%s charges at least %d ppm to send %d sats to us over %s, more than maxppm %d",
			r.Node.Graph.GetAlias(dst), feePPM(inFee, r.Amount), r.Amount/1000, r.InChannel.ShortChannelId, limit)
		if own {
			return nil, fmt.Errorf("%w (this split's own limit)", err)
		}
		return nil, onIn(err, RetryNo)
	}
	route, err := r.Node.Graph.GetLikelyCircularRoute(r.OutChannel, r.InChannel, r.Amount, exclude, r.MaxHops, maxFee, r.riskPPM())
	if err == util.ErrNoRoute {
		// Say how expensive the cheapest route is, if there is one, so maxppm can be tuned.
		if cheapest, cerr := r.Node.Graph.GetCheapestCircularRoute(r.OutChannel, r.InChannel, r.Amount, exclude, r.MaxHops); cerr == nil {
			return nil, util.NewRouteTooExpensiveError(cheapest.FeePPM(), r.MaxPPM)
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	if route.FeePPM() > r.MaxPPM {
		return nil, util.NewRouteTooExpensiveError(route.FeePPM(), r.MaxPPM)
	}

	return route, nil
}

// feePPM is fee (msat) in parts per million of amount, rounded down.
func feePPM(fee, amount uint64) uint64 {
	if amount == 0 {
		return 0
	}
	hi, lo := bits.Mul64(fee, 1000000)
	if hi >= amount {
		return math.MaxUint64
	}
	ppm, _ := bits.Div64(hi, lo, amount)
	return ppm
}

// tryRoute pays route, and learns from the outcome.
func (r *Rebalance) tryRoute(route *graph.Route, exclude map[string]bool) (*graph.PrettyRoute, error) {
	if err := r.checkFirstHop(route); err != nil {
		return nil, err
	}

	pay := r.pay
	if pay == nil {
		pay = r.payInvoice
	}
	r.payments++
	if r.Sent != nil {
		r.Sent.Add(1)
	}
	prettyRoute, err := pay(route)
	if err != nil {
		// Learn from the failure now, so the next attempt avoids what failed.
		var paymentError *glightning.PaymentError
		if errors.As(err, &paymentError) && paymentError.Data != nil {
			return nil, r.learnFromFailure(route, paymentError.Data, exclude)
		}

		// a timeout, or an RPC error: retrying the same call would not help
		return nil, err
	}

	r.learnFromSuccess(route)
	return prettyRoute, nil
}

// payInvoice pays route to ourselves through a short-lived invoice, so that
// lightningd checks the incoming HTLC (payment secret, amount, CLTV) before
// releasing the preimage.
func (r *Rebalance) payInvoice(route *graph.Route) (*graph.PrettyRoute, error) {
	invoice, err := r.Node.CreateRebalanceInvoice(route.Amount)
	if err != nil {
		r.Node.Logln(glightning.Unusual, "unable to create rebalance invoice: ", err)
		return nil, err
	}
	paymentHash := invoice.PaymentHash

	prettyRoute := graph.NewPrettyRoute(route, paymentHash)

	// save route to DB
	if err := r.Node.SaveToDb(node.ROUTE_PREFIX+paymentHash, prettyRoute); err != nil {
		r.Node.Logln(glightning.Unusual, "unable to save route to db: ", err)
	}
	r.Node.Logln(glightning.Debug, prettyRoute)
	r.Node.Logln(glightning.Debug, prettyRoute.Simple())

	if _, err := r.Node.SendPay(route, invoice); err != nil {
		return nil, err
	}
	return prettyRoute, nil
}
