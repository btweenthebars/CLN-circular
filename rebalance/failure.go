package rebalance

import (
	"circular/graph"
	"circular/util"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
	"time"
)

// AVOID_DURATION is how long later rebalances avoid a node or channel that
// failed a payment for a reason liquidity beliefs cannot express.
const AVOID_DURATION = 30 * time.Minute

// avoid excludes key for the rest of this run, and from later runs for
// AVOID_DURATION. Our out-peer and in-peer are excluded from this run only:
// avoiding them would block every route through our own channels with them.
func (r *Rebalance) avoid(exclude map[string]bool, key string) {
	exclude[key] = true
	if key == r.OutChannel.Destination || key == r.InChannel.Source {
		return
	}
	r.Node.Graph.Avoid(key, time.Now().Add(AVOID_DURATION).Unix())
}

func retryWhenFreeIf(busy bool) Retry {
	if busy {
		return RetryWhenFree
	}
	return RetryNo
}

// errShortOfLiquidity is util.ErrTemporaryFailure for a channel along the route,
// not one of ours, that could not carry the amount: the same route may carry
// less.
var errShortOfLiquidity = fmt.Errorf("%w: a channel on the route could not carry the amount", util.ErrTemporaryFailure)

// BOLT 4 failure codes that a rebalance reacts to.
const (
	failNodeBit                 = 0x2000
	failTemporaryNodeFailure    = 0x2002
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
// attempt searches again. It returns the error for Run: one that is
// util.ErrTemporaryFailure when another attempt can help, and
// errShortOfLiquidity when a channel along the route lacked liquidity.
func (r *Rebalance) learnFromFailure(route *graph.Route, failure *glightning.PaymentErrorData, exclude map[string]bool) error {
	g := r.Node.Graph
	r.Node.Logf(glightning.Debug, "payment failed at %s, channel %s/%d: %s (0x%04x)",
		g.GetAlias(failure.ErringNode), failure.ErringChannel, failure.ErringDirection,
		failure.FailCodeName, failure.FailCode)

	if failure.ErringNode == r.Node.Id && failure.ErringIndex == 0 && len(route.Hops) > 0 {
		// lightningd would not send it over our out channel, for example
		// because the peer is not connected
		hop := route.Hops[0]
		return onOut(fmt.Errorf("our channel %s to %s could not send the payment: %s",
			hop.ShortChannelId, g.GetAlias(hop.Destination), failure.FailCodeName),
			retryWhenFreeIf(failure.FailCode == failTemporaryChannelFailure))
	}
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
		r.avoid(exclude, failure.ErringNode)
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
			return onIn(fmt.Errorf("%s cannot send %d sats to us over %s right now",
				g.GetAlias(failure.ErringNode), hop.MilliSatoshi/1000, hop.ShortChannelId), RetryWhenFree)
		}
		g.LearnUpperBound(id, hop.MilliSatoshi)
		exclude[id] = true
		return errShortOfLiquidity

	case failFeeInsufficient, failIncorrectCltvExpiry, failAmountBelowMinimum, failExpiryTooSoon:
		// Our copy of the channel's policy is out of date.
		if inChannel {
			if failure.FailCode == failFeeInsufficient {
				// our copy of the in-peer's fee may be stale: read it again, so
				// that the next try pays the new fee. If it did not change, the
				// cause is elsewhere (lightningd has not applied the update yet,
				// or an inbound fee on the channel before it): a try now would
				// fail the same way.
				if err := r.refreshPeers(r.InChannel.Source); err != nil {
					return onIn(util.ErrWireFeeInsufficient, RetryNo)
				}
				updated, err := g.GetChannel(id)
				if err != nil || !policyChanged(hop.Channel, updated) {
					return onIn(util.ErrWireFeeInsufficient, RetryNo)
				}
				return onIn(util.ErrWireFeeInsufficient, RetryNow)
			}
			return onIn(fmt.Errorf("%s rejected the payment on our channel %s: %s",
				g.GetAlias(failure.ErringNode), hop.ShortChannelId, failure.FailCodeName), RetryNo)
		}
		if !r.refreshPolicy(hop.Channel, id) {
			// the update has not reached us yet: avoid the channel until it does
			r.avoid(exclude, id)
		}

	case failChannelDisabled, failPermanentChannelFailure, failUnknownNextPeer:
		if inChannel {
			return onIn(fmt.Errorf("%s cannot use our channel %s: %s",
				g.GetAlias(failure.ErringNode), hop.ShortChannelId, failure.FailCodeName), RetryNo)
		}
		g.MarkUnusable(id)
		exclude[id] = true

	default:
		// a temporary node failure from one of our peers may pass
		retry := retryWhenFreeIf(failure.FailCode == failTemporaryNodeFailure)
		if inChannel {
			// the in-peer, or its channel to us, failed it: not the route
			exclude[failure.ErringNode] = true
			return onIn(fmt.Errorf("%s failed the payment on our channel %s: %s",
				g.GetAlias(failure.ErringNode), hop.ShortChannelId, failure.FailCodeName), retry)
		}
		if k == 1 && failure.FailCode&failNodeBit != 0 {
			// the out-peer itself failed: not the route
			exclude[failure.ErringNode] = true
			return onOut(fmt.Errorf("%s failed the payment: %s",
				g.GetAlias(failure.ErringNode), failure.FailCodeName), retry)
		}
		if failure.FailCode&failNodeBit != 0 {
			r.avoid(exclude, failure.ErringNode)
		} else {
			r.avoid(exclude, id)
		}
	}

	return util.ErrTemporaryFailure
}

// learnFromSuccess records that each channel along the route, other than our
// own (kept up to date from listpeerchannels), forwarded its hop's amount.
func (r *Rebalance) learnFromSuccess(route *graph.Route) {
	for i := 1; i < len(route.Hops)-1; i++ {
		r.Node.Graph.LearnPayment(channelId(route.Hops[i].Channel), route.Hops[i].MilliSatoshi)
	}
}

// refreshPolicy reloads a channel's policy from lightningd, which applies the
// channel_update carried in the failure, and reports whether it changed.
func (r *Rebalance) refreshPolicy(old *graph.Channel, id string) bool {
	r.Node.RefreshChannel(old)
	updated, err := r.Node.Graph.GetChannel(id)
	if err != nil {
		return false
	}
	return policyChanged(old, updated)
}

// refreshPeers reads our channels with the given peers again.
func (r *Rebalance) refreshPeers(peerIds ...string) error {
	if r.refresh != nil {
		return r.refresh(peerIds...)
	}
	return r.Node.RefreshPeerChannels(peerIds...)
}

// policyChanged reports whether a channel's fees, CLTV delta or HTLC limits
// differ between two reads.
func policyChanged(old, updated *graph.Channel) bool {
	return updated.BaseFeeMillisatoshi != old.BaseFeeMillisatoshi ||
		updated.FeePerMillionth != old.FeePerMillionth ||
		updated.Delay != old.Delay ||
		updated.HtlcMinimumMilliSatoshis.MSat() != old.HtlcMinimumMilliSatoshis.MSat() ||
		updated.HtlcMaximumMilliSatoshis.MSat() != old.HtlcMaximumMilliSatoshis.MSat()
}
