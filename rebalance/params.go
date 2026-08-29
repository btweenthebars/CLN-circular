package rebalance

import (
	"circular/graph"
	"circular/util"
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
		r.Node.Logf(glightning.Info, "checkConnections: inChannel %s not in normal state (state=%s, expected %s)", inChannel.ShortChannelId, inChannel.State, NORMAL)
		return util.ErrIncomingChannelNotInNormalState
	}
	if outChannel.State != NORMAL {
		r.Node.Logf(glightning.Info, "checkConnections: outChannel %s not in normal state (state=%s, expected %s)", outChannel.ShortChannelId, outChannel.State, NORMAL)
		return util.ErrOutgoingChannelNotInNormalState
	}

	// validate that the peers are connected
	if !r.Node.IsPeerConnected(inChannel) {
		r.Node.Logf(glightning.Info, "checkConnections: inChannel %s peer disconnected (peer_connected=%v)", inChannel.ShortChannelId, inChannel.PeerConnected)
		return util.ErrIncomingPeerDisconnected
	}
	if !r.Node.IsPeerConnected(outChannel) {
		r.Node.Logf(glightning.Info, "checkConnections: outChannel %s peer disconnected (peer_connected=%v)", outChannel.ShortChannelId, outChannel.PeerConnected)
		return util.ErrOutgoingPeerDisconnected
	}
	return nil
}

func (r *Rebalance) checkLiquidity(inChannel, outChannel *glightning.PeerChannel) error {
	//validate that the amount is less than the liquidity of the channels
	inAvailable := inChannel.TotalMsat.MSat() - inChannel.ToUsMsat.MSat()
	if inAvailable < r.Amount {
		r.Node.Logf(glightning.Info, "checkLiquidity: inChannel %s depleted (inAvailable=%d msat < required=%d msat)", inChannel.ShortChannelId, inAvailable, r.Amount)
		return util.ErrIncomingChannelDepleted
	}
	if outChannel.ToUsMsat.MSat() < r.Amount {
		r.Node.Logf(glightning.Info, "checkLiquidity: outChannel %s depleted (to_us=%d msat < required=%d msat)", outChannel.ShortChannelId, outChannel.ToUsMsat.MSat(), r.Amount)
		return util.ErrOutgoingChannelDepleted
	}
	return nil
}

func (r *Rebalance) validateLiquidityParameters(out, in *graph.Channel) error {
	inChannel, err := r.Node.GetPeerChannelFromGraphChannel(in)
	if err != nil {
		r.Node.Logf(glightning.Unusual, "validateLiquidityParameters: GetPeerChannelFromGraphChannel(in=%s) failed: %v", in.ShortChannelId, err)
		return err
	}
	outChannel, err := r.Node.GetPeerChannelFromGraphChannel(out)
	if err != nil {
		r.Node.Logf(glightning.Unusual, "validateLiquidityParameters: GetPeerChannelFromGraphChannel(out=%s) failed: %v", out.ShortChannelId, err)
		return err
	}

	if err := r.checkConnections(inChannel, outChannel); err != nil {
		return err
	}

	if err := r.checkLiquidity(inChannel, outChannel); err != nil {
		return err
	}

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
