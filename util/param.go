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

// Keys is a list of strings a parameter takes as a JSON array or as one
// string, whose items are separated by commas or spaces: lightning-cli -k
// passes `avoid=02ab...` as a string and `avoid=["02ab...","1x2x3"]` as an
// array. It is a struct, so that glightning, which reads an array into a
// slice itself and refuses a string for one, always calls UnmarshalJSON.
type Keys struct{ V []string }

func (k *Keys) UnmarshalJSON(data []byte) error {
	var list []string
	if err := json.Unmarshal(data, &list); err == nil {
		k.V = list
		return nil
	}
	var one *string
	if err := json.Unmarshal(data, &one); err != nil {
		return fmt.Errorf("expected a list of strings, or one string: %s", data)
	}
	k.V = nil
	if one != nil {
		k.V = strings.FieldsFunc(*one, func(r rune) bool { return r == ',' || r == ' ' })
	}
	return nil
}
