package configuration

import (
	"strings"
	"testing"
)

func TestStrictJSONLimits(t *testing.T) {
	var decoded struct {
		Value string `json:"value"`
	}
	data := []byte(`{"value":"` + strings.Repeat("x", 256<<10) + `"}`)
	if err := StrictJSON(data, &decoded); err == nil {
		t.Fatal("default configuration limit was bypassed")
	}
	if err := StrictJSONLimit(data, &decoded, len(data)); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Value) != 256<<10 {
		t.Fatal("decoded value was truncated")
	}
	for _, limit := range []int{-1, 0, len(data) - 1} {
		if err := StrictJSONLimit(data, &decoded, limit); err == nil {
			t.Errorf("accepted document with limit %d", limit)
		}
	}
}

func TestStrictJSONCustomLimitKeepsValidation(t *testing.T) {
	for name, data := range map[string]string{
		"duplicate": `{"value":"a","value":"b"}`,
		"unknown":   `{"other":"a"}`,
		"trailing":  `{"value":"a"} {}`,
		"utf8":      "{\"value\":\"\xff\"}",
	} {
		t.Run(name, func(t *testing.T) {
			var decoded struct {
				Value string `json:"value"`
			}
			if err := StrictJSONLimit([]byte(data), &decoded, 16<<20); err == nil {
				t.Fatal("accepted invalid document")
			}
		})
	}
}
