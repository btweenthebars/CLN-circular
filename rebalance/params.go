package rebalance

import (
	"circular/graph"
	"circular/util"
	"fmt"
	"github.com/elementsproject/glightning/glightning"
)

const (
	NORMAL           = "CHANNELD_NORMAL"
	DEFAULT_AMOUNT   = 200000000
	DEFAULT_MAXPPM   = 10
	DEFAULT_ATTEMPTS = 1
	DEFAULT_MAXHOPS  = 8
)

func (r *Rebalance) checkConnections(inChannel, outChannel *glightning.PeerChannel) error {
	//validate that the channels are in normal state
	if inChannel.State != NORMAL {
		return util.ErrIncomingChannelNotInNormalState
	}
	if outChannel.State != NORMAL {
		return util.ErrOutgoingChannelNotInNormalState
	}

	// validate that the peers are connected
	if !r.Node.IsPeerConnected(inChannel) {
		return util.ErrIncomingPeerDisconnected
	}
	if !r.Node.IsPeerConnected(outChannel) {
		return util.ErrOutgoingPeerDisconnected
	}
	return nil
}

// checkLiquidity checks that the channels can carry the amount. lightningd's
// spendable and receivable amounts account for channel reserves, commitment
// fees and HTLCs in flight, which our balance alone does not.
func (r *Rebalance) checkLiquidity(inChannel, outChannel *glightning.PeerChannel) error {
	if inChannel.ReceivableMsat.MSat() < r.Amount {
		return util.ErrIncomingChannelDepleted
	}
	// the out channel also carries the fees: checkFirstHop checks the route's amount
	if outChannel.SpendableMsat.MSat() < r.Amount {
		return util.ErrOutgoingChannelDepleted
	}
	return nil
}

// checkFirstHop checks that our out channel can send what the route's first
// hop carries: the amount plus the fees of the hops after it.
func (r *Rebalance) checkFirstHop(route *graph.Route) error {
	outChannel, err := r.Node.GetPeerChannelFromGraphChannel(r.OutChannel)
	if err != nil {
		return err
	}
	if spendable := outChannel.SpendableMsat.MSat(); spendable < route.Hops[0].MilliSatoshi {
		return fmt.Errorf("%w: it can send %d msat, the route needs %d msat including fees",
			util.ErrOutgoingChannelDepleted, spendable, route.Hops[0].MilliSatoshi)
	}
	return nil
}

func (r *Rebalance) validateLiquidityParameters(out, in *graph.Channel) error {
	r.Node.Logln(glightning.Debug, "validating liquidity parameters")

	inChannel, err := r.Node.GetPeerChannelFromGraphChannel(in)
	if err != nil {
		r.Node.Logln(glightning.Unusual, err)
		return err
	}
	outChannel, err := r.Node.GetPeerChannelFromGraphChannel(out)
	if err != nil {
		r.Node.Logln(glightning.Unusual, err)
		return err
	}

	if err := r.checkConnections(inChannel, outChannel); err != nil {
		return err
	}

	if err := r.checkLiquidity(inChannel, outChannel); err != nil {
		return err
	}

	r.Node.Logln(glightning.Debug, "liquidity parameters validated")
	return nil
}

func (r *Rebalance) setDefaults() {
	//convert to msatoshi
	r.Amount *= 1000
	if r.Amount == 0 {
		r.Amount = DEFAULT_AMOUNT
		r.Node.Logln(glightning.Debug, "amount not provided, using default value", r.Amount)
	}
	if r.MaxPPM == 0 {
		r.MaxPPM = DEFAULT_MAXPPM
		r.Node.Logln(glightning.Debug, "maxPPM not provided, using default value", r.MaxPPM)
	}
	if r.Attempts <= 0 {
		r.Attempts = DEFAULT_ATTEMPTS
		r.Node.Logln(glightning.Debug, "attempts not provided, using default value", r.Attempts)
	}
	if r.MaxHops <= 0 {
		r.MaxHops = DEFAULT_MAXHOPS
		r.Node.Logln(glightning.Debug, "maxHops not provided, using default value", r.MaxHops)
	}
}
