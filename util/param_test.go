package util

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamNumbers(t *testing.T) {
	var p struct {
		I Int   `json:"i"`
		F Float `json:"f"`
	}
	assert.NoError(t, json.Unmarshal([]byte(`{"i": "-1", "f": "-0.5"}`), &p))
	assert.Equal(t, Int(-1), p.I)
	assert.Equal(t, -0.5, p.F.V)
	assert.NoError(t, json.Unmarshal([]byte(`{"i": 1e6, "f": 0.9}`), &p))
	assert.Equal(t, Int(1000000), p.I, "a JSON number glightning marshals back as 1e+06")
	assert.Equal(t, 0.9, p.F.V)
	assert.NoError(t, json.Unmarshal([]byte(`{"i": null, "f": null}`), &p))
	assert.Zero(t, p.I, "null: not given")
	assert.Zero(t, p.F.V)
	for _, bad := range []string{`{"i": "x"}`, `{"i": "2.5"}`, `{"i": 1e300}`, `{"f": "inf"}`, `{"f": "nan"}`,
		`{"f": ""}`, `{"f": [1]}`} {
		assert.Error(t, json.Unmarshal([]byte(bad), &p), bad)
	}
	out, err := json.Marshal(Float{V: 0.9})
	assert.NoError(t, err)
	assert.Equal(t, "0.9", string(out), "written as a number")
}
