package graph

import (
	"github.com/elementsproject/glightning/glightning"
	"math"
	"math/bits"
	"time"
)

type Channel struct {
	*glightning.Channel `json:"channel"`
	Liquidity           uint64 `json:"liquidity"`
	Timestamp           int64  `json:"timestamp"`
	maxHtlcMsat         uint64
	minHtlcMsat         uint64
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
	if amount == 0 {
		return 0
	}
	fee := c.ComputeFee(amount)
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
	return c.IsActive &&
		c.Liquidity >= amount &&
		c.maxHtlcMsat >= amount &&
		c.minHtlcMsat <= amount
}

func (c *Channel) ResetLiquidity() {
	c.Liquidity = c.AmountMsat.MSat() / 2
	c.Timestamp = time.Now().Unix()
}
