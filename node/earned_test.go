package node

import (
	"testing"
	"time"

	"github.com/elementsproject/glightning/glightning"
	"github.com/stretchr/testify/assert"
)

// What a channel's forwards earned: settled ones resolved since the cut,
// weighted by what they carried.
func TestEarnedPPM(t *testing.T) {
	now := time.Now()
	at := func(daysAgo int) float64 { return float64(now.Add(-time.Duration(daysAgo) * 24 * time.Hour).Unix()) }
	forward := func(status string, daysAgo int, outSats, feeMsat uint64) glightning.Forwarding {
		return glightning.Forwarding{Status: status, ResolvedTime: at(daysAgo),
			OutMsat: glightning.AmountFromMSat(outSats * 1000), FeeMsat: glightning.AmountFromMSat(feeMsat)}
	}
	forwards := []glightning.Forwarding{
		forward("settled", 1, 100000, 50000),   // 500 ppm
		forward("settled", 10, 300000, 30000),  // 100 ppm
		forward("failed", 2, 900000, 900000),   // not settled
		forward("settled", 40, 500000, 500000), // too old
	}
	ppm, volume := EarnedPPM(forwards, now.Add(-30*24*time.Hour))
	assert.Equal(t, uint64(200), ppm, "80,000 msat on 400k sats")
	assert.Equal(t, uint64(400000000), volume)

	ppm, volume = EarnedPPM(nil, now)
	assert.Equal(t, uint64(0), ppm)
	assert.Equal(t, uint64(0), volume)

	// received_time stands in for a missing resolved_time
	f := forward("settled", 0, 100000, 10000)
	f.ResolvedTime, f.ReceivedTime = 0, at(1)
	ppm, _ = EarnedPPM([]glightning.Forwarding{f}, now.Add(-2*24*time.Hour))
	assert.Equal(t, uint64(100), ppm)
}
