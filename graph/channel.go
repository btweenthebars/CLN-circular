package graph

import (
	"github.com/elementsproject/glightning/glightning"
	"math"
	"math/bits"
	"time"
)

// Channel is one direction of a channel in the graph. Its policy (the
// embedded glightning.Channel) never changes once the channel is in the
// graph: an update replaces the whole Channel, so routes keep the policy they
// were priced with and can read it without a lock. Liquidity and Timestamp
// change in place, only with the graph's channel lock held.
type Channel struct {
	*glightning.Channel `json:"channel"`
	Liquidity           uint64 `json:"liquidity"`
	Timestamp           int64  `json:"timestamp"`
	// Upper: a failed payment showed the channel holds at most Liquidity, so
	// it is a bound rather than an estimate (see SuccessChance)
	Upper bool `json:"upper,omitempty"`
	// Dear: its fee rate is at least DearFactor times its node's median, a
	// sign its side is drained (see MarkDearChannels)
	Dear        bool `json:"-"`
	maxHtlcMsat uint64
	minHtlcMsat uint64
}

func NewChannel(channel *glightning.Channel, liquidity uint64, timestamp int64) *Channel {
	return &Channel{
		Channel:     channel,
		Liquidity:   liquidity,
		Timestamp:   timestamp,
		maxHtlcMsat: channel.HtlcMaximumMilliSatoshis.MSat(),
		minHtlcMsat: channel.HtlcMinimumMilliSatoshis.MSat(),
	}
}

// ComputeFee returns the channel's outbound fee for forwarding amount: the
// base fee plus the proportional fee, rounded up. It saturates at
// math.MaxUint64 rather than wrapping: amount x ppm overflows 64 bits from
// about 4.3M sats at the highest ppm (2^32-1), which some nodes set to keep
// payments off a channel, and a wrapped fee made such a channel look cheap.
func (c *Channel) ComputeFee(amount uint64) uint64 {
	hi, lo := bits.Mul64(amount, c.FeePerMillionth)
	// ceiling division: (amount x ppm + 999,999) / 1,000,000
	lo, carry := bits.Add64(lo, 999999, 0)
	hi += carry
	if hi >= 1000000 {
		return math.MaxUint64 // the quotient does not fit in 64 bits
	}
	proportionalFee, _ := bits.Div64(hi, lo, 1000000)
	return addSaturating(c.BaseFeeMillisatoshi, proportionalFee)
}

func (c *Channel) ComputeFeePPM(amount uint64) uint64 {
	return feePPM(c.ComputeFee(amount), amount)
}

// feePPM returns fee as parts per million of amount, rounded down: 0 for a
// zero amount, math.MaxUint64 for a saturated fee or one that does not fit.
func feePPM(fee, amount uint64) uint64 {
	if amount == 0 {
		return 0
	}
	hi, lo := bits.Mul64(fee, 1000000)
	if fee == math.MaxUint64 || hi >= amount {
		return math.MaxUint64
	}
	ppm, _ := bits.Div64(hi, lo, amount)
	return ppm
}

// addSaturating returns a + b, or math.MaxUint64 if that overflows.
func addSaturating(a, b uint64) uint64 {
	sum, carry := bits.Add64(a, b, 0)
	if carry != 0 {
		return math.MaxUint64
	}
	return sum
}

func (c *Channel) GetHop(amount uint64, delay uint32) glightning.RouteHop {
	return glightning.RouteHop{
		Id:             c.Destination,
		ShortChannelId: c.ShortChannelId,
		AmountMsat:     glightning.AmountFromMSat(amount),
		Delay:          delay,
		Direction:      c.GetDirection(),
	}
}

func (c *Channel) GetDirection() uint32 {
	if c.Source < c.Destination {
		return 0
	}
	return 1
}

func (c *Channel) CanForward(amount uint64) bool {
	limit := c.Liquidity
	if c.Upper && limit > c.AmountMsat.MSat()/2 {
		// a bound loosening after a failure never admits more than a
		// channel nothing is known about
		limit = c.AmountMsat.MSat() / 2
	}
	return c.IsActive &&
		limit >= amount &&
		c.maxHtlcMsat >= amount &&
		c.minHtlcMsat <= amount
}

// SuccessChance is the chance that c can forward amount, taking its
// liquidity belief as the middle of an even spread of balances: over the whole
// capacity for the default belief (half of it), narrower once payments have
// moved the belief toward empty or full. After a failure (Upper), the belief
// is a bound: any balance up to it is as likely. It is 0 for a channel
// believed unusable.
func (c *Channel) SuccessChance(amount uint64) float64 {
	capacity := c.AmountMsat.MSat()
	liquidity := c.Liquidity
	if liquidity > capacity {
		liquidity = capacity
	}
	if liquidity == 0 || capacity == 0 {
		return 0
	}
	if c.Upper && c.learned() {
		if amount > liquidity {
			return 0
		}
		return float64(liquidity+1-amount) / float64(liquidity+1)
	}
	if c.Dear && !c.learned() {
		// priced well above its node's other channels: likely drained
		liquidity = uint64(float64(capacity) * DearShare)
	}
	low, high := uint64(0), 2*liquidity
	if liquidity > capacity/2 {
		low, high = 2*liquidity-capacity, capacity
	}
	if amount <= low {
		return 1
	}
	if amount >= high {
		return 0
	}
	return float64(high-amount) / float64(high-low)
}

func (c *Channel) ResetLiquidity() {
	c.Liquidity = c.AmountMsat.MSat() / 2
	c.Upper = false
	c.Timestamp = time.Now().Unix()
}
