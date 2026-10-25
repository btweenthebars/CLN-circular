package node

import (
	"errors"
	"math"
	"math/bits"
	"time"

	"github.com/elementsproject/glightning/glightning"
)

const (
	// DEFAULT_EARN_DAYS is how far back the forwards of a channel count for
	// what its liquidity earns (circular-earn-days).
	DEFAULT_EARN_DAYS = 30
	// EARN_MIN_VOLUME_MSAT is how much the forwards out through a channel
	// must have carried in that time for their rate to count; below it, our
	// current fee on the channel stands in.
	EARN_MIN_VOLUME_MSAT = 100000000 // 100k sats
)

type listForwardsRequest struct {
	Status     string `json:"status"`
	OutChannel string `json:"out_channel"`
}

func (r *listForwardsRequest) Name() string {
	return "listforwards"
}

type listForwardsResponse struct {
	Forwards []glightning.Forwarding `json:"forwards"`
}

// Earned reads the settled forwards out through our channel scid, and returns
// what those resolved since since earned, as ppm of what they carried, and
// how much they carried (msat).
func (n *Node) Earned(scid string, since time.Time) (uint64, uint64, error) {
	if n.lightning == nil {
		return 0, 0, errors.New("not connected to lightningd")
	}
	var resp listForwardsResponse
	if err := n.lightning.Request(&listForwardsRequest{Status: "settled", OutChannel: scid}, &resp); err != nil {
		return 0, 0, err
	}
	ppm, volume := EarnedPPM(resp.Forwards, since)
	return ppm, volume, nil
}

// EarnedPPM is what the settled forwards resolved since since earned, as ppm
// of what they carried out, and how much they carried (msat).
func EarnedPPM(forwards []glightning.Forwarding, since time.Time) (uint64, uint64) {
	var fees, volume uint64
	cut := float64(since.Unix())
	for _, f := range forwards {
		if f.Status != "settled" {
			continue
		}
		at := f.ResolvedTime
		if at == 0 {
			at = f.ReceivedTime
		}
		if at < cut {
			continue
		}
		fees += f.FeeMsat.MSat()
		volume += f.OutMsat.MSat()
	}
	if volume == 0 {
		return 0, 0
	}
	hi, lo := bits.Mul64(fees, 1000000)
	if hi >= volume {
		return math.MaxUint64, volume
	}
	ppm, _ := bits.Div64(hi, lo, volume)
	return ppm, volume
}
