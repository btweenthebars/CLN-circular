package node

import (
	"encoding/json"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionFromBuildInfo(t *testing.T) {
	info := &debug.BuildInfo{GoVersion: "go1.22.1", Settings: []debug.BuildSetting{
		{Key: "-compiler", Value: "gc"},
		{Key: "vcs", Value: "git"},
		{Key: "vcs.revision", Value: "14f4b66c0ffee5d7a1b2c3d4e5f60718293a4b5c"},
		{Key: "vcs.time", Value: "2026-10-31T12:05:00Z"},
		{Key: "vcs.modified", Value: "false"},
	}}
	v := versionFrom(info, true)
	assert.Equal(t, Version{Commit: "14f4b66", Time: "2026-10-31T12:05:00Z", Go: "go1.22.1"}, v)
	assert.Equal(t, "14f4b66 (2026-10-31T12:05:00Z, go1.22.1)", v.String())
	out, err := json.Marshal(v)
	assert.NoError(t, err)
	assert.Equal(t, `{"commit":"14f4b66","time":"2026-10-31T12:05:00Z","go":"go1.22.1"}`, string(out))

	info.Settings[4].Value = "true"
	v = versionFrom(info, true)
	assert.True(t, v.Modified)
	assert.Equal(t, "14f4b66+modified (2026-10-31T12:05:00Z, go1.22.1)", v.String(), "built from changed files")

	// no git information: built outside a checkout, or with -buildvcs=false
	v = versionFrom(&debug.BuildInfo{GoVersion: "go1.22.1"}, true)
	assert.Equal(t, "unknown (go1.22.1)", v.String())
	assert.Equal(t, "unknown", versionFrom(nil, false).String())
	assert.Equal(t, "abc (go1.20)", versionFrom(&debug.BuildInfo{GoVersion: "go1.20",
		Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}}}, true).String(), "a short revision as it is")
	assert.Equal(t, "1234567 (2026-10-31T12:05:00Z)", Version{Commit: "1234567", Time: "2026-10-31T12:05:00Z"}.String())
}
