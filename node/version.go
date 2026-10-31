package node

import (
	"fmt"
	"runtime/debug"
)

// Version is which build of circular is running, from what go build stamps
// into the binary when it builds ./cmd/circular in a git checkout: the
// commit, its time, whether files were changed from it, and the Go version.
// Commit is "unknown" when the build had no git information (built outside
// a checkout, or with -buildvcs=false).
type Version struct {
	Commit   string `json:"commit"`             // the first 7 characters, as git log --oneline shows them
	Time     string `json:"time,omitempty"`     // the commit's, UTC
	Modified bool   `json:"modified,omitempty"` // built from files changed since the commit
	Go       string `json:"go,omitempty"`
}

// BuildVersion is the running binary's Version.
func BuildVersion() Version {
	info, ok := debug.ReadBuildInfo()
	return versionFrom(info, ok)
}

func versionFrom(info *debug.BuildInfo, ok bool) Version {
	v := Version{Commit: "unknown"}
	if !ok || info == nil {
		return v
	}
	v.Go = info.GoVersion
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			v.Commit = s.Value
			if len(v.Commit) > 7 {
				v.Commit = v.Commit[:7]
			}
		case "vcs.time":
			v.Time = s.Value
		case "vcs.modified":
			v.Modified = s.Value == "true"
		}
	}
	return v
}

// String is the Version for the log: "14f4b66 (2026-10-31T12:05:00Z, go1.22.1)",
// with "+modified" after the commit when files were changed from it.
func (v Version) String() string {
	s := v.Commit
	if v.Modified {
		s += "+modified"
	}
	var about []string
	if v.Time != "" {
		about = append(about, v.Time)
	}
	if v.Go != "" {
		about = append(about, v.Go)
	}
	switch len(about) {
	case 1:
		s += fmt.Sprintf(" (%s)", about[0])
	case 2:
		s += fmt.Sprintf(" (%s, %s)", about[0], about[1])
	}
	return s
}
