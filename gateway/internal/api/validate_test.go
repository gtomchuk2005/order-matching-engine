package api

import (
	"encoding/json"
	"testing"
)

func TestValidateSymbol(t *testing.T) {
	cases := map[string]bool{
		"AAPL":              true,
		"A":                 true,
		"BRK.A":             true,
		"A1234567890123456": false, // 17 chars, too long
		"aapl":              false,
		"":                  false,
		"1AAPL":             false,
		"AAPL!":             false,
	}
	for sym, want := range cases {
		got := validateSymbol(sym) == nil
		if got != want {
			t.Errorf("validateSymbol(%q) valid=%v want %v", sym, got, want)
		}
	}
}

func TestValidateSide(t *testing.T) {
	if validateSide("buy") != nil {
		t.Error("buy should be valid")
	}
	if validateSide("sell") != nil {
		t.Error("sell should be valid")
	}
	if validateSide("BUY") == nil {
		t.Error("BUY should be invalid (case-sensitive)")
	}
	if validateSide("hold") == nil {
		t.Error("hold should be invalid")
	}
}

func TestValidatePrice(t *testing.T) {
	if _, verr := validatePrice(json.Number("100")); verr != nil {
		t.Errorf("100 should be valid: %v", verr)
	}
	if _, verr := validatePrice(json.Number("0")); verr == nil {
		t.Error("0 should be invalid")
	}
	if _, verr := validatePrice(json.Number("-5")); verr == nil {
		t.Error("-5 should be invalid")
	}
	if _, verr := validatePrice(json.Number("100.7")); verr == nil {
		t.Error("100.7 should be invalid (not an integer)")
	}
}

func TestValidateQty(t *testing.T) {
	if _, verr := validateQty(json.Number("10")); verr != nil {
		t.Errorf("10 should be valid: %v", verr)
	}
	if _, verr := validateQty(json.Number("0")); verr == nil {
		t.Error("0 should be invalid")
	}
	if _, verr := validateQty(json.Number("3.5")); verr == nil {
		t.Error("3.5 should be invalid")
	}
}

func TestValidateClientOrderID(t *testing.T) {
	if validateClientOrderID("") == nil {
		t.Error("empty should be invalid")
	}
	if validateClientOrderID("abc") != nil {
		t.Error("abc should be valid")
	}
	long := make([]byte, 65)
	for i := range long {
		long[i] = 'a'
	}
	if validateClientOrderID(string(long)) == nil {
		t.Error("65 bytes should be invalid")
	}
	ok := make([]byte, 64)
	for i := range ok {
		ok[i] = 'a'
	}
	if validateClientOrderID(string(ok)) != nil {
		t.Error("64 bytes should be valid")
	}
}
