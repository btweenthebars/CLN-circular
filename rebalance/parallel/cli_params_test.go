package parallel

import (
	"testing"

	"github.com/elementsproject/glightning/jrpc2"
	"github.com/stretchr/testify/assert"
)

// lightning-cli -k sends a value unquoted only when it is all digits and
// dots: feeratio=-1 reaches the plugin as the string "-1". Every param that
// means something below 0 takes it, through glightning's own parser.
func TestNegativeParamsFromLightningCli(t *testing.T) {
	negatives := func(extra map[string]interface{}) map[string]interface{} {
		p := map[string]interface{}{"feeratio": "-1", "maxtime": "-1", "maxfee": "-1", "riskppm": "-1",
			"maxfailstreak": "-1", "maxidlerounds": "-1", "probefailstreak": "-1"}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}

	pull := &RebalancePull{}
	if assert.NoError(t, jrpc2.ParseNamedParams(pull, negatives(map[string]interface{}{"inscid": "1x1x1"}))) {
		assert.Equal(t, -1.0, pull.FeeRatio.V)
		assert.Equal(t, []int{-1, -1, -1, -1, -1, -1}, []int{int(pull.MaxTime), int(pull.MaxFee),
			int(pull.RiskPPM), int(pull.MaxFailStreak), int(pull.MaxIdleRounds), int(pull.ProbeFailStreak)})
	}
	push := &RebalancePush{}
	if assert.NoError(t, jrpc2.ParseNamedParams(push, negatives(map[string]interface{}{"outscid": "1x1x1"}))) {
		assert.Equal(t, -1.0, push.FeeRatio.V)
		assert.Equal(t, []int{-1, -1, -1, -1, -1, -1}, []int{int(push.MaxTime), int(push.MaxFee),
			int(push.RiskPPM), int(push.MaxFailStreak), int(push.MaxIdleRounds), int(push.ProbeFailStreak)})
	}
	quote := &RebalanceQuote{}
	if assert.NoError(t, jrpc2.ParseNamedParams(quote, negatives(map[string]interface{}{"inscid": "1x1x1"}))) {
		assert.Equal(t, -1.0, quote.FeeRatio.V)
		assert.Equal(t, []int{-1, -1, -1, -1, -1, -1}, []int{int(quote.MaxTime), int(quote.MaxFee),
			int(quote.RiskPPM), int(quote.MaxFailStreak), int(quote.MaxIdleRounds), int(quote.ProbeFailStreak)})
	}

	// what lightning-cli sends unquoted, and JSON clients (the bridge's
	// callers) send: numbers
	pull = &RebalancePull{}
	assert.NoError(t, jrpc2.ParseNamedParams(pull, map[string]interface{}{"inscid": "1x1x1", "feeratio": 0.9,
		"maxfee": 400.0, "maxtime": -1.0, "maxppm": 1600.0, "attempts": 200.0}))
	assert.Equal(t, 0.9, pull.FeeRatio.V)
	assert.Equal(t, 400, int(pull.MaxFee))
	assert.Equal(t, -1, int(pull.MaxTime))
	assert.Equal(t, uint64(1600), pull.MaxPPM, "the other params as before")
	assert.Equal(t, 200, pull.Attempts)
	pull = &RebalancePull{}
	assert.NoError(t, jrpc2.ParseNamedParams(pull, map[string]interface{}{"inscid": "1x1x1", "feeratio": " 0.5 "}))
	assert.Equal(t, 0.5, pull.FeeRatio.V)

	// not a number, or not a whole one where one is needed: said
	for _, bad := range []map[string]interface{}{{"feeratio": "off"}, {"maxfee": "-1.5"}, {"maxtime": "1e30"},
		{"feeratio": "NaN"}, {"riskppm": "Inf"}, {"maxfee": true}} {
		bad["inscid"] = "1x1x1"
		assert.Error(t, jrpc2.ParseNamedParams(&RebalancePull{}, bad), "%v", bad)
	}
}

// The quote hands its params on to the pull or push it quotes.
func TestQuoteHandsOnNegativeParams(t *testing.T) {
	q := &RebalanceQuote{}
	assert.NoError(t, jrpc2.ParseNamedParams(q, map[string]interface{}{"inscid": "1x1x1", "feeratio": "-1",
		"riskppm": "-1"}))
	pull := q.pull()
	assert.Equal(t, -1.0, pull.FeeRatio.V)
	assert.Equal(t, -1, int(pull.RiskPPM))
	q = &RebalanceQuote{}
	assert.NoError(t, jrpc2.ParseNamedParams(q, map[string]interface{}{"outscid": "1x1x1", "feeratio": "-1",
		"riskppm": "-1"}))
	push := q.push()
	assert.Equal(t, -1.0, push.FeeRatio.V)
	assert.Equal(t, -1, int(push.RiskPPM))
}
