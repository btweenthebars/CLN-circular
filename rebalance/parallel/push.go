package parallel

import (
	"circular/graph"
	rebalance2 "circular/rebalance"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"github.com/elementsproject/glightning/jrpc2"
)

const (
	DEFAULT_MIN_OUT_PPM        = 50
	DEFAULT_FILL_UP_TO_PERCENT = 0.8
	DEFAULT_FILL_UP_TO_AMOUNT  = 10000000
)

type RebalancePush struct {
	OutScid         string   `json:"outscid"`
	InList          []string `json:"inlist,omitempty"`
	MinOutPPM       uint64   `json:"minoutppm,omitempty"`
	Amount          uint64   `json:"amount,omitempty"`
	MaxPPM          uint64   `json:"maxppm,omitempty"`
	Splits          int      `json:"splits,omitempty"`
	SplitAmount     uint64   `json:"splitamount,omitempty"`
	MinSplit        uint64   `json:"minsplit,omitempty"`
	Attempts        int      `json:"attempts,omitempty"`
	MaxHops         int      `json:"maxhops,omitempty"`
	FillUpToPercent float64  `json:"filluptopercent,omitempty"`
	FillUpToAmount  uint64   `json:"filluptoamount,omitempty"`
	MaxNodeFails    int      `json:"maxnodefails,omitempty"`
	MaxFailStreak   int      `json:"maxfailstreak,omitempty"`
	RiskPPM         int      `json:"riskppm,omitempty"`
	MaxTime         int      `json:"maxtime,omitempty"` // seconds; 0: the node's default (circular-max-time); below 0: no limit
	MaxFee          int      `json:"maxfee,omitempty"`  // sats for the whole run; 0: the node's default (circular-max-fee); below 0: no budget
	AbstractRebalance
}

func (r *RebalancePush) Name() string {
	return "circular-push"
}

func (r *RebalancePush) New() interface{} {
	return &RebalancePush{}
}

func (r *RebalancePush) Call() (jrpc2.Result, error) {
	if err := r.prepare(); err != nil {
		return nil, err
	}
	r.FireCandidates()
	return r.WaitForResult()
}

// prepare reads the parameters and finds the candidates, in the order a run
// tries them.
func (r *RebalancePush) prepare() error {
	r.AbstractRebalance.RebalanceMethods = r
	if r.OutScid == "" {
		return util.ErrNoRequiredParameter
	}
	r.Init(r.Amount, r.MaxPPM, r.SplitAmount, r.MinSplit, r.Splits, r.Attempts, r.MaxHops)
	r.maxNodeFails = r.MaxNodeFails
	r.maxFailStreak = r.MaxFailStreak
	r.riskPPM = r.RiskPPM
	r.setMaxTime(r.MaxTime)
	r.setMaxFee(r.MaxFee)

	r.CandidatesList = r.InList
	if r.CandidatesList != nil {
		r.Node.Logln(glightning.Debug, "Using inlist:", r.CandidatesList)
		// if an inlist was supplied, ignore minoutppm. To do this we put it to zero
		r.MinOutPPM = 0
	}

	r.setDefaults()

	if err := r.validateParameters(); err != nil {
		return err
	}

	outgoingChannel, err := r.Node.GetOutgoingChannelFromScid(r.OutScid)
	if err != nil {
		return err
	}
	r.TargetChannel = outgoingChannel

	if err = r.FindCandidates(r.TargetChannel.Destination); err != nil {
		return err
	}
	return nil
}

func (r *RebalancePush) IsGoodCandidate(peerChannel *glightning.PeerChannel) bool {
	// first of all, if the peer charges more than maxppm to send to us, even
	// with the best of its inbound discounts, there's no point in trying to
	// use it
	incomingChannel, err := r.Node.GetIncomingChannelFromScid(peerChannel.ShortChannelId)
	if err != nil {
		r.Node.Logln(glightning.Unusual, err)
		return false
	}
	if !r.inPeerFeeFits(incomingChannel) {
		return false
	}

	// we need to get the outgoing channel from the peer to compute outgoing PPM and check it's above the minoutppm
	outgoingChannel, err := r.Node.GetOutgoingChannelFromScid(peerChannel.ShortChannelId)
	if err != nil {
		r.Node.Logln(glightning.Unusual, err)
		return false
	}

	return outgoingChannel.ComputeFeePPM(r.splitAmount) > r.MinOutPPM
}

// inPeerFeeFits reports whether what the peer charges to send a split to us
// over in, from the target channel's peer, with the best of its inbound
// discounts, is within maxppm. Its fee for in alone used to rule out
// candidates whose discounts made them fit.
func (r *RebalancePush) inPeerFeeFits(in *graph.Channel) bool {
	return r.Node.Graph.LeastForwardFee(in, r.TargetChannel, r.splitAmount) <= graph.MaxFeeForPPM(r.splitAmount, r.maxPPM)
}

// CanUseChannel checks that the channel keeps at least the fill threshold as
// remote balance after receiving a split, and that its peer is connected.
func (r *RebalancePush) CanUseChannel(channel *glightning.PeerChannel) error {
	fillAmount := util.Min(r.FillUpToAmount,
		uint64(float64(channel.TotalMsat.MSat())*r.FillUpToPercent))
	r.Node.Logln(glightning.Debug, "fillAmount:", fillAmount)

	// The split moves the split amount out of the peer's balance, less what
	// its HTLCs in flight already take. Receivable is only a limit on a
	// single HTLC (see RebalancePull.CanUseChannel).
	if remoteBalance(channel) < addSaturating(fillAmount, r.splitAmount) || channel.ReceivableMsat.MSat() < r.splitAmount {
		return util.ErrChannelFilled
	}

	if channel.State != rebalance2.NORMAL {
		return util.ErrChannelNotInNormalState
	}

	if r.Node.IsPeerConnected(channel) == false {
		return util.ErrIncomingPeerDisconnected
	}

	return nil
}

// Fire drains the target channel into candidate, our in channel.
func (r *RebalancePush) Fire(candidate *graph.Channel, amount uint64) {
	r.launch(r.TargetChannel, candidate, candidate, amount)
}

func (r *RebalancePush) validateParameters() error {
	if err := r.validateGenericParameters(); err != nil {
		return err
	}
	if r.FillUpToPercent > 1 || r.FillUpToPercent < 0 {
		return util.ErrDepleteUpToPercentInvalid
	}
	return nil
}

func (r *RebalancePush) setDefaults() {
	if r.MinOutPPM == 0 {
		r.MinOutPPM = DEFAULT_MIN_OUT_PPM
	}
	if r.FillUpToPercent <= 0 {
		r.FillUpToPercent = DEFAULT_FILL_UP_TO_PERCENT
	}
	if r.FillUpToAmount == 0 {
		r.FillUpToAmount = DEFAULT_FILL_UP_TO_AMOUNT
	}

	// convert to msat
	r.FillUpToAmount *= 1000
}

func (r *RebalancePush) GetCandidateDirection(id string) string {
	return util.GetDirection(id, r.Node.Id)
}

// EnqueueCandidate puts a candidate at the front of the queue
func (r *RebalancePush) EnqueueCandidate(result *rebalance2.Result) {
	scid := result.Route.Hops[len(result.Route.Hops)-1].ShortChannelId
	candidate, err := r.Node.GetIncomingChannelFromScid(scid)
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

// AddSuccess records the success under the in channel it filled.
func (r *RebalancePush) AddSuccess(result *rebalance2.Result) {
	scid := ""
	if result.Route != nil && len(result.Route.Hops) > 0 {
		scid = result.Route.Hops[len(result.Route.Hops)-1].ShortChannelId
	}
	r.AddSuccessGeneric(scid, result.In, result.PPM, result.Amount)
	r.addInvolving(scid, result.In, result.Route)
}
