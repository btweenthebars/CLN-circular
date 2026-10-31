package rebalance

import (
	"testing"

	"github.com/elementsproject/glightning/jrpc2"
	"github.com/stretchr/testify/assert"
)

// lightning-cli -k sends riskppm=-1 as the string "-1" (see
// util.Int): circular and circular-node take it.
func TestRiskPPMFromLightningCli(t *testing.T) {
	byScid := &RebalanceByScid{}
	assert.NoError(t, jrpc2.ParseNamedParams(byScid, map[string]interface{}{"outscid": "1x1x1", "inscid": "2x1x1",
		"riskppm": "-1", "maxppm": 500.0}))
	assert.Equal(t, -1, int(byScid.RiskPPM))
	assert.Equal(t, uint64(500), byScid.MaxPPM)
	byNode := &RebalanceByNode{}
	assert.NoError(t, jrpc2.ParseNamedParams(byNode, map[string]interface{}{"outnode": "02a", "innode": "02b",
		"riskppm": "-1"}))
	assert.Equal(t, -1, int(byNode.RiskPPM))
	byNode = &RebalanceByNode{}
	assert.NoError(t, jrpc2.ParseNamedParams(byNode, map[string]interface{}{"outnode": "02a", "innode": "02b",
		"riskppm": 300.0}))
	assert.Equal(t, 300, int(byNode.RiskPPM))
}
