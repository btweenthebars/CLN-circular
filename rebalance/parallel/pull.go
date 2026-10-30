package parallel

import (
	"circular/graph"
	rebalance2 "circular/rebalance"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/elementsproject/glightning/jrpc2"
)

const (
	DEFAULT_MAX_OUT_PPM           = 50
	DEFAULT_DEPLETE_UP_TO_PERCENT = 0.2
	DEFAULT_DEPLETE_UP_TO_AMOUNT  = 1000000
)

type RebalancePull struct {
	InScid             string   `json:"inscid"`
	OutList            []string `json:"outlist,omitempty"`
	MaxOutPPM          uint64   `json:"maxoutppm,omitempty"`
	Amount             uint64   `json:"amount,omitempty"`
	MaxPPM             uint64   `json:"maxppm,omitempty"`
	Splits             int      `json:"splits,omitempty"`
	SplitAmount        uint64   `json:"splitamount,omitempty"`
	MinSplit           uint64   `json:"minsplit,omitempty"`
	DepleteUpToPercent float64  `json:"depleteuptopercent,omitempty"`
	DepleteUpToAmount  uint64   `json:"depleteuptoamount,omitempty"`
	Attempts           int      `json:"attempts,omitempty"`
	MaxHops            int      `json:"maxhops,omitempty"`
	MaxNodeFails       int      `json:"maxnodefails,omitempty"`
	MaxFailStreak      int      `json:"maxfailstreak,omitempty"`
	RiskPPM            int      `json:"riskppm,omitempty"`
	MaxTime            int      `json:"maxtime,omitempty"`    // seconds; 0: the node's default (circular-max-time); below 0: no limit
	MaxFee             int      `json:"maxfee,omitempty"`     // sats for the whole run; 0: the node's default (circular-max-fee); below 0: no budget
	FeeRatio           float64  `json:"feeratio,omitempty"`   // splits pay at most this share of what the filled channel earns, less our fee on the drained one; 0: circular-fee-ratio; below 0: off
	RetryEvery         int      `json:"retryevery,omitempty"` // seconds; slow mode: when a round falls short, wait this long and try again (min 60; maxtime 6h if none)
	AbstractRebalance
}

func (r *RebalancePull) Name() string {
	return "circular-pull"
}

func (r *RebalancePull) New() interface{} {
	return &RebalancePull{}
}

func (r *RebalancePull) Call() (jrpc2.Result, error) {
	if err := r.prepare(); err != nil {
		return nil, err
	}
	r.FireCandidates()
	return r.WaitForResult()
}

// prepare reads the parameters and finds the candidates, in the order a run
// tries them.
func (r *RebalancePull) prepare() error {
	r.AbstractRebalance.RebalanceMethods = r
	if r.InScid == "" {
		return util.ErrNoRequiredParameter
	}
	r.Init(r.Amount, r.MaxPPM, r.SplitAmount, r.MinSplit, r.Splits, r.Attempts, r.MaxHops)
	r.maxNodeFails = r.MaxNodeFails
	r.maxFailStreak = r.MaxFailStreak
	r.riskPPM = r.RiskPPM
	r.setMaxTime(r.MaxTime)
	r.setMaxFee(r.MaxFee)
	r.setFeeRatio(r.FeeRatio)
	r.setRetryEvery(r.RetryEvery)

	r.CandidatesList = r.OutList
	if r.CandidatesList != nil {
		r.Node.Logln(glightning.Info, "Using outlist:", r.CandidatesList)
		// if an outlist was supplied, ignore maxoutppm. To do this we put it to "infinity"
		r.MaxOutPPM = 1 << 63
	}

	r.setDefaults()

	if err := r.validateParameters(); err != nil {
		r.Node.Logf(glightning.Unusual, "RebalancePull validateParameters failed: %v", err)
		return err
	}
	r.Node.Logf(glightning.Debug, "RebalancePull parameters validated: %+v", r)

	incomingChannel, err := r.Node.GetIncomingChannelFromScid(r.InScid)
	if err != nil {
		r.Node.Logf(glightning.Unusual, "GetIncomingChannelFromScid(%s) failed: %v", r.InScid, err)
		return err
	}
	r.TargetChannel = incomingChannel

	r.excludePeer = r.TargetChannel.Source
	if err = r.FindCandidates(r.excludePeer); err != nil {
		r.Node.Logf(glightning.Unusual, "FindCandidates failed: %v", err)
		return err
	}
	r.prefetchEarnings()
	return nil
}

func (r *RebalancePull) IsGoodCandidate(peerChannel *glightning.PeerChannel) bool {
	if peerChannel.State != rebalance2.NORMAL || !r.Node.IsPeerConnected(peerChannel) {
		return false
	}

	// we need to get the outgoing channel from the peer to compute outgoing PPM and check it's below the maxoutppm
	outgoingChannel, err := r.Node.GetOutgoingChannelFromScid(peerChannel.ShortChannelId)
	if err != nil {
		r.Node.Logf(glightning.Unusual, "IsGoodCandidate[%s]: GetOutgoingChannel failed: %v", peerChannel.ShortChannelId, err)
		return false
	}

	effectivePPM := outgoingChannel.ComputeFeePPM(r.splitAmount)
	return effectivePPM <= r.MaxOutPPM
}

// CanUseChannel checks that the channel stays above the deplete threshold
// after sending a split, and that its peer is connected.
func (r *RebalancePull) CanUseChannel(channel *glightning.PeerChannel) error {
	depleteAmount := util.Min(r.DepleteUpToAmount,
		uint64(float64(channel.TotalMsat.MSat())*r.DepleteUpToPercent))
	r.Node.Logln(glightning.Debug, "depleteAmount:", depleteAmount)

	// The split takes the split amount and its fees out of our balance, less
	// what our HTLCs in flight already take. Spendable is only a limit on a
	// single HTLC: lightningd caps it at 2^32-1 msat on channels without
	// option_support_large_channel, so it cannot stand for the balance.
	split := addSaturating(r.splitAmount, graph.MaxFeeForPPM(r.splitAmount, r.maxPPM))
	if localBalance(channel) < addSaturating(depleteAmount, split) || channel.SpendableMsat.MSat() < split {
		return util.ErrChannelDepleted
	}

	if channel.State != rebalance2.NORMAL {
		return util.ErrChannelNotInNormalState
	}

	if !r.Node.IsPeerConnected(channel) {
		return util.ErrOutgoingPeerDisconnected
	}

	return nil
}

// Fire drains candidate, our out channel, into the target channel.
func (r *RebalancePull) Fire(candidate *graph.Channel, amount uint64) {
	r.launch(candidate, r.TargetChannel, candidate, amount)
}

func (r *RebalancePull) setDefaults() {
	if r.DepleteUpToPercent <= 0 {
		r.DepleteUpToPercent = DEFAULT_DEPLETE_UP_TO_PERCENT
	}
	if r.DepleteUpToAmount == 0 {
		r.DepleteUpToAmount = DEFAULT_DEPLETE_UP_TO_AMOUNT
	}
	if r.MaxOutPPM == 0 {
		r.MaxOutPPM = DEFAULT_MAX_OUT_PPM
	}

	// convert to msat
	r.DepleteUpToAmount *= 1000
}

func (r *RebalancePull) validateParameters() error {
	if err := r.validateGenericParameters(); err != nil {
		return err
	}
	if r.DepleteUpToPercent > 1 || r.DepleteUpToPercent < 0 {
		return util.ErrDepleteUpToPercentInvalid
	}
	return nil
}

func (r *RebalancePull) GetCandidateDirection(id string) string {
	return util.GetDirection(r.Node.Id, id)
}

// EnqueueCandidate puts a candidate at the front of the queue
func (r *RebalancePull) EnqueueCandidate(result *rebalance2.Result) {
	scid := result.Route.Hops[0].ShortChannelId
	candidate, err := r.Node.GetOutgoingChannelFromScid(scid)
	if err != nil {
		r.Node.Logln(glightning.Unusual, err)
		r.leftQueue(result.Candidate, err.Error())
		return
	}

	r.routes[scid] = result.Channels

	r.QueueLock.Lock()
	r.Candidates.PushFront(candidate)
	r.QueueLock.Unlock()
}

// AddSuccess records the success under the out channel it drained.
func (r *RebalancePull) AddSuccess(result *rebalance2.Result) {
	scid := ""
	if result.Route != nil && len(result.Route.Hops) > 0 {
		scid = result.Route.Hops[0].ShortChannelId
	}
	r.AddSuccessGeneric(scid, result.Out, result.PPM, result.Amount)
	r.addInvolving(scid, result.Out, result.Route)
}
