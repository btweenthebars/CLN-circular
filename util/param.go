package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Int and Float are command params that also take a number written as a
// string. lightning-cli -k sends a value unquoted only when it is all digits
// and dots, so maxfee=-1 or feeratio=-1 reaches the plugin as the string
// "-1", which glightning refuses for an int or float64 field. Their kinds
// differ from that of every value encoding/json decodes to, so glightning
// hands them to UnmarshalJSON. A named float64 would not do: glightning sets
// a float64-kind field straight from a JSON number, and panics on the type.

// Int is an int param that also takes a whole number in a string.
type Int int

func (i *Int) UnmarshalJSON(data []byte) error {
	f, err := parseNumber(data)
	if err != nil {
		return err
	}
	if f != math.Trunc(f) || math.Abs(f) > 1<<53 {
		return fmt.Errorf("%s is not a whole number", data)
	}
	*i = Int(f)
	return nil
}

// Float is a float64 param that also takes a number in a string. It is a
// struct, not a named float64: see Int.
type Float struct{ V float64 }

func (f *Float) UnmarshalJSON(data []byte) error {
	v, err := parseNumber(data)
	if err != nil {
		return err
	}
	f.V = v
	return nil
}

func (f Float) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.V)
}

// parseNumber reads a JSON number, a number in a JSON string, or null (0).
func parseNumber(data []byte) (float64, error) {
	s := string(bytes.TrimSpace(data))
	if s == "null" {
		return 0, nil
	}
	if strings.HasPrefix(s, `"`) {
		if err := json.Unmarshal(data, &s); err != nil {
			return 0, err
		}
		s = strings.TrimSpace(s)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("%s is not a number", data)
	}
	return f, nil
}
