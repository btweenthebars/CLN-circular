package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/elementsproject/glightning/jrpc2"
)

// AVOID_FILE holds the nodes and channels the user never wants rebalances to
// route through (circular-avoid). Unlike the avoid list rebalances learn
// (graph.Avoid, 30 minutes), it lasts until removed.
const AVOID_FILE = "avoid.json"

var (
	reNodeId  = regexp.MustCompile(`^0[23][0-9a-f]{64}$`)
	reScid    = regexp.MustCompile(`^[0-9]+x[0-9]+x[0-9]+$`)
	reScidDir = regexp.MustCompile(`^[0-9]+x[0-9]+x[0-9]+/[01]$`)
)

// AvoidKey checks key: a node id, a short channel id (both directions) or
// "scid/direction" (one). -> it, lowercased.
func AvoidKey(key string) (string, error) {
	key = strings.ToLower(strings.TrimSpace(key))
	if reNodeId.MatchString(key) || reScid.MatchString(key) || reScidDir.MatchString(key) {
		return key, nil
	}
	return "", fmt.Errorf("%q is not a node id, a short channel id or scid/direction", key)
}

// ExcludeKeys turns avoided keys into the search's exclusions: a node id as
// it is, a short channel id as both its directions.
func ExcludeKeys(keys []string) []string {
	out := make([]string, 0, len(keys)+2)
	for _, k := range keys {
		if reScid.MatchString(k) {
			out = append(out, k+"/0", k+"/1")
		} else {
			out = append(out, k)
		}
	}
	return out
}

// AvoidEntry is one entry of the saved list.
type AvoidEntry struct {
	Key   string `json:"key"`
	Alias string `json:"alias,omitempty"`
	Note  string `json:"note,omitempty"`
	Added int64  `json:"added"`
}

type avoidList struct {
	sync.Mutex
	entries map[string]AvoidEntry
	broken  error // the file could not be read: nothing is routed, nothing written over it
}

// AvoidListError says why rebalances are refused: the saved list could not
// be read, so what it holds cannot be avoided. nil when it was read (or
// there is none).
func (n *Node) AvoidListError() error {
	n.avoidList.Lock()
	defer n.avoidList.Unlock()
	if n.avoidList.broken != nil {
		return fmt.Errorf("the avoid list cannot be read (%v): fix or remove circular/%s, then restart circular",
			n.avoidList.broken, AVOID_FILE)
	}
	return nil
}

// AvoidedKeys is the saved list's keys, sorted.
func (n *Node) AvoidedKeys() []string {
	n.avoidList.Lock()
	defer n.avoidList.Unlock()
	keys := make([]string, 0, len(n.avoidList.entries))
	for k := range n.avoidList.entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// LoadAvoidList reads dir/AVOID_FILE; a missing file is an empty list. One
// that cannot be read refuses rebalances (AvoidListError) until it is read.
func (n *Node) LoadAvoidList(dir string) error {
	data, err := os.ReadFile(filepath.Join(dir, AVOID_FILE))
	entries := map[string]AvoidEntry{}
	if err == nil {
		var list []AvoidEntry
		if err = json.Unmarshal(data, &list); err != nil {
			err = fmt.Errorf("%s: %w", AVOID_FILE, err)
		}
		for _, e := range list {
			key, kerr := AvoidKey(e.Key)
			if kerr != nil {
				if err == nil {
					err = fmt.Errorf("%s: %w", AVOID_FILE, kerr)
				}
				continue
			}
			e.Key = key
			entries[key] = e
		}
	} else if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	n.avoidList.Lock()
	defer n.avoidList.Unlock()
	n.avoidList.entries, n.avoidList.broken = entries, err
	return err
}

// saveAvoidList writes the list to dir/AVOID_FILE (a temporary file renamed
// over it). Called with the lock held.
func (n *Node) saveAvoidList(dir string, entries map[string]AvoidEntry) error {
	list := make([]AvoidEntry, 0, len(entries))
	for _, e := range entries {
		list = append(list, e)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Key < list[j].Key })
	data, err := json.MarshalIndent(list, "", " ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, AVOID_FILE)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync() // on disk before it replaces the list: a power loss leaves the old one, or this
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// the rename itself on disk too; not every file system can sync a
	// directory, and the list is saved either way
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// changeAvoidList adds key (with note) or removes it, and saves the list;
// nothing changes if it cannot be saved.
func (n *Node) changeAvoidList(dir, key, note string, add bool) error {
	n.avoidList.Lock()
	defer n.avoidList.Unlock()
	if n.avoidList.broken != nil {
		return fmt.Errorf("the avoid list cannot be read (%v): not written over; fix or remove circular/%s, "+
			"then restart circular", n.avoidList.broken, AVOID_FILE)
	}
	entries := make(map[string]AvoidEntry, len(n.avoidList.entries)+1)
	for k, e := range n.avoidList.entries {
		entries[k] = e
	}
	if add {
		e := AvoidEntry{Key: key, Note: note, Added: time.Now().Unix()}
		if old, ok := entries[key]; ok {
			e.Added = old.Added
			if note == "" {
				e.Note = old.Note
			}
		}
		entries[key] = e
	} else {
		if _, ok := entries[key]; !ok {
			return fmt.Errorf("%s is not on the list (remove an entry by its key as the list shows it)", key)
		}
		delete(entries, key)
	}
	if dir == "" {
		return errors.New("circular has no data directory yet")
	}
	if err := n.saveAvoidList(dir, entries); err != nil {
		return err
	}
	n.avoidList.entries = entries
	return nil
}

// AvoidCmd is circular-avoid: the saved list of nodes and channels
// rebalances never route through. add or remove one (a node id, a short
// channel id, or scid/direction), with a note; without either, the list.
type AvoidCmd struct {
	Add    string `json:"add,omitempty"`
	Remove string `json:"remove,omitempty"`
	Note   string `json:"note,omitempty"`
}

type AvoidResult struct {
	Changed string       `json:"changed,omitempty"`
	Count   int          `json:"count"`
	Avoid   []AvoidEntry `json:"avoid"`
}

func (c *AvoidCmd) Name() string {
	return "circular-avoid"
}

func (c *AvoidCmd) New() interface{} {
	return &AvoidCmd{}
}

func (c *AvoidCmd) Call() (jrpc2.Result, error) {
	n := GetNode()
	return n.AvoidCall(n.dataDir, c)
}

func (n *Node) AvoidCall(dir string, c *AvoidCmd) (*AvoidResult, error) {
	if c.Add != "" && c.Remove != "" {
		return nil, errors.New("add or remove, not both")
	}
	out := &AvoidResult{}
	for _, change := range []struct {
		key string
		add bool
	}{{c.Add, true}, {c.Remove, false}} {
		if change.key == "" {
			continue
		}
		key, err := AvoidKey(change.key)
		if err != nil {
			return nil, err
		}
		if err := n.changeAvoidList(dir, key, strings.TrimSpace(c.Note), change.add); err != nil {
			return nil, err
		}
		out.Changed = map[bool]string{true: "added ", false: "removed "}[change.add] + key
	}
	n.avoidList.Lock()
	for _, e := range n.avoidList.entries {
		if reNodeId.MatchString(e.Key) && n.Graph != nil {
			e.Alias = n.Graph.GetAlias(e.Key)
		}
		out.Avoid = append(out.Avoid, e)
	}
	n.avoidList.Unlock()
	sort.Slice(out.Avoid, func(i, j int) bool { return out.Avoid[i].Key < out.Avoid[j].Key })
	if out.Avoid == nil {
		out.Avoid = []AvoidEntry{}
	}
	out.Count = len(out.Avoid)
	return out, nil
}
