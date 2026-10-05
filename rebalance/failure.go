package rebalance

import (
	"circular/graph"
	"circular/util"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
)

// BOLT 4 failure codes that a rebalance reacts to.
const (
	failNodeBit                 = 0x2000
	failTemporaryChannelFailure = 0x1007
	failAmountBelowMinimum      = 0x100b
	failFeeInsufficient         = 0x100c
	failIncorrectCltvExpiry     = 0x100d
	failExpiryTooSoon           = 0x100e
	failChannelDisabled         = 0x1014
	failPermanentChannelFailure = 0x4008
	failUnknownNextPeer         = 0x400a
)

func channelId(c *graph.Channel) string {
	return c.ShortChannelId + "/" + util.GetDirection(c.Source, c.Destination)
}

// learnFromFailure updates the graph and this run's exclusions from a failed
// payment, using the amount the route carried on each hop, before the next
// attempt searches again. It returns the error for Run:
// util.ErrTemporaryFailure when another attempt can help.
func (r *Rebalance) learnFromFailure(route *graph.Route, failure *glightning.PaymentErrorData, exclude map[string]bool) error {
	g := r.Node.Graph
	r.Node.Logf(glightning.Debug, "payment failed at %s, channel %s/%d: %s (0x%04x)",
		g.GetAlias(failure.ErringNode), failure.ErringChannel, failure.ErringDirection,
		failure.FailCodeName, failure.FailCode)

	if failure.ErringNode == r.Node.Id {
		// our own node refused it, for example because it arrived after the invoice was deleted
		return fmt.Errorf("our node failed the payment: %s", failure.FailCodeName)
	}

	if failure.ErringChannel != "" {
		exclude[fmt.Sprintf("%s/%d", failure.ErringChannel, failure.ErringDirection)] = true
	}

	// The erring node could not forward over its channel in our route.
	k := -1
	for i, hop := range route.Hops {
		if hop.Source == failure.ErringNode {
			k = i
			break
		}
	}
	if k < 0 {
		exclude[failure.ErringNode] = true
		return util.ErrTemporaryFailure
	}
	hop := route.Hops[k]
	id := channelId(hop.Channel)
	inChannel := k == len(route.Hops)-1 // the in-peer's channel to us

	// The channels before it carried the payment, so they hold at least that much.
	// Our own channel (hop 0) is kept up to date from listpeerchannels instead.
	for i := 1; i < k; i++ {
		g.LearnLowerBound(channelId(route.Hops[i].Channel), route.Hops[i].MilliSatoshi)
	}

	switch failure.FailCode {
	case failTemporaryChannelFailure:
		if inChannel {
			return fmt.Errorf("%s cannot send %d sats to us over %s right now",
				g.GetAlias(failure.ErringNode), hop.MilliSatoshi/1000, hop.ShortChannelId)
		}
		g.LearnUpperBound(id, hop.MilliSatoshi)
		exclude[id] = true

	case failFeeInsufficient, failIncorrectCltvExpiry, failAmountBelowMinimum, failExpiryTooSoon:
		// Our copy of the channel's policy is out of date.
		if inChannel {
			if failure.FailCode == failFeeInsufficient {
				return util.ErrWireFeeInsufficient
			}
			return fmt.Errorf("%s rejected the payment on our channel %s: %s",
				g.GetAlias(failure.ErringNode), hop.ShortChannelId, failure.FailCodeName)
		}
		if !r.refreshPolicy(hop.Channel, id) {
			// the update has not reached us yet: avoid the channel for this run
			exclude[id] = true
		}

	case failChannelDisabled, failPermanentChannelFailure, failUnknownNextPeer:
		if inChannel {
			return fmt.Errorf("%s cannot use our channel %s: %s",
				g.GetAlias(failure.ErringNode), hop.ShortChannelId, failure.FailCodeName)
		}
		g.MarkUnusable(id)
		exclude[id] = true

	default:
		if failure.FailCode&failNodeBit != 0 {
			exclude[failure.ErringNode] = true
		} else {
			exclude[id] = true
		}
	}

	return util.ErrTemporaryFailure
}

// refreshPolicy reloads a channel's policy from lightningd, which applies the
// channel_update carried in the failure, and reports whether it changed.
func (r *Rebalance) refreshPolicy(old *graph.Channel, id string) bool {
	r.Node.RefreshChannel(old)
	updated, err := r.Node.Graph.GetChannel(id)
	if err != nil {
		return false
	}
	return updated.BaseFeeMillisatoshi != old.BaseFeeMillisatoshi ||
		updated.FeePerMillionth != old.FeePerMillionth ||
		updated.Delay != old.Delay ||
		updated.HtlcMinimumMilliSatoshis.MSat() != old.HtlcMinimumMilliSatoshis.MSat() ||
		updated.HtlcMaximumMilliSatoshis.MSat() != old.HtlcMaximumMilliSatoshis.MSat()
}
