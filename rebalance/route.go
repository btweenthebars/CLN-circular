package rebalance

import (
	"circular/graph"
	"circular/node"
	"circular/util"
	"errors"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"time"
)

func (r *Rebalance) getRoute(exclude map[string]bool) (*graph.Route, error) {
	defer util.TimeTrack(time.Now(), "rebalance.getRoute", r.Node.Logf)

	src := r.OutChannel.Destination
	dst := r.InChannel.Source

	r.Node.Logln(glightning.Debug, "looking for a route from ", r.Node.Graph.GetAlias(src), " to ", r.Node.Graph.GetAlias(dst))
	maxFee := graph.MaxFeeForPPM(r.Amount, r.MaxPPM)
	route, err := r.Node.Graph.GetCircularRoute(r.OutChannel, r.InChannel, r.Amount, exclude, r.MaxHops, maxFee)
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

func (r *Rebalance) tryRoute(exclude map[string]bool) (*graph.PrettyRoute, error) {
	r.Node.Logln(glightning.Debug, "generating route")
	route, err := r.getRoute(exclude)
	if err != nil {
		return nil, err
	}

	// Pay ourselves through a short-lived invoice, so that lightningd checks the
	// incoming HTLC (payment secret, amount, CLTV) before releasing the preimage.
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

	_, err = r.Node.SendPay(route, invoice)
	if err != nil {
		// Extract erring channel from payment error and add it to exclude for
		// future attempts so the pathfinder skips the known-bad channel.
		var paymentError *glightning.PaymentError
		if errors.As(err, &paymentError) && paymentError.Data.ErringChannel != "" {
			key := fmt.Sprintf("%s/%d", paymentError.Data.ErringChannel, paymentError.Data.ErringDirection)
			exclude[key] = true
			r.Node.Logln(glightning.Debug, "excluded failing channel for next attempt: ", key)
		}

		if err == util.ErrSendPayTimeout {
			return nil, err
		}
		if err == util.ErrWireFeeInsufficient {
			return nil, err
		}
		if err == util.ErrFirstPeerNotReady {
			return nil, err
		}
		return nil, util.ErrTemporaryFailure
	}

	return prettyRoute, nil
}
