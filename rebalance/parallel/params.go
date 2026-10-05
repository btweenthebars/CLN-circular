package parallel

import (
	"circular/rebalance"
	"circular/util"
	"github.com/elementsproject/glightning/glightning"
	"math"
)

const (
	DEFAULT_AMOUNT       = 400000
	DEFAULT_SPLITS       = 4
	DEFAULT_SPLIT_AMOUNT = 100000
)

func (r *AbstractRebalance) setGenericDefaults() {
	if r.amount == 0 {
		r.amount = DEFAULT_AMOUNT
	}
	if r.splits == 0 {
		r.splits = DEFAULT_SPLITS
	}
	if r.splitAmount == 0 {
		r.splitAmount = DEFAULT_SPLIT_AMOUNT
	}
	if r.maxPPM == 0 {
		r.maxPPM = rebalance.DEFAULT_MAXPPM
	}
	if r.attempts <= 0 {
		r.attempts = rebalance.DEFAULT_ATTEMPTS
	}
	if r.maxHops <= 0 {
		r.maxHops = rebalance.DEFAULT_MAXHOPS
	}

	r.AmountRebalanced = 0
	r.InFlightAmount = 0

	// convert to msat
	r.amount *= 1000
	r.splitAmount *= 1000
}

func (r *AbstractRebalance) validateGenericParameters() error {
	if r.amount < r.splitAmount {
		return util.ErrAmountLessThanSplitAmount
	}
	if r.amount%r.splitAmount != 0 {
		return util.ErrAmountNotMultipleOfSplitAmount
	}
	return nil
}

// localBalance is our balance in channel, less the HTLCs we are offering.
func localBalance(channel *glightning.PeerChannel) uint64 {
	return subSaturating(channel.ToUsMsat.MSat(), htlcsInFlight(channel, "out"))
}

// remoteBalance is the peer's balance in channel, less the HTLCs it is
// offering us.
func remoteBalance(channel *glightning.PeerChannel) uint64 {
	remote := subSaturating(channel.TotalMsat.MSat(), channel.ToUsMsat.MSat())
	return subSaturating(remote, htlcsInFlight(channel, "in"))
}

// htlcsInFlight sums the HTLCs in channel going in direction ("in" or "out"):
// lightningd counts them in the balances until they settle.
func htlcsInFlight(channel *glightning.PeerChannel, direction string) uint64 {
	var total uint64
	for _, htlc := range channel.Htlcs {
		if htlc != nil && htlc.Direction == direction {
			total = addSaturating(total, htlc.AmountMsat.MSat())
		}
	}
	return total
}

func addSaturating(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}
	return a + b
}

func subSaturating(a, b uint64) uint64 {
	if b > a {
		return 0
	}
	return a - b
}
