package api

import (
	"encoding/json"
	"fmt"
	"regexp"
)

var symbolRe = regexp.MustCompile(`^[A-Z][A-Z0-9.]{0,15}$`)

// ValidationError names the offending field so handlers can report it in the 422 body.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

func validationErr(field, msg string) *ValidationError {
	return &ValidationError{Field: field, Message: msg}
}

func validateSymbol(symbol string) *ValidationError {
	if !symbolRe.MatchString(symbol) {
		return validationErr("symbol", "invalid symbol")
	}
	return nil
}

func validateSide(side string) *ValidationError {
	if side != "buy" && side != "sell" {
		return validationErr("side", "side must be \"buy\" or \"sell\"")
	}
	return nil
}

// jsonInt64 rejects non-integers explicitly: UseNumber gives us the string form,
// so "100.7" must fail here rather than truncate on conversion.
func jsonInt64(field string, n json.Number) (int64, *ValidationError) {
	v, err := n.Int64()
	if err != nil {
		return 0, validationErr(field, fmt.Sprintf("%s must be an integer", field))
	}
	return v, nil
}

func validatePrice(n json.Number) (int64, *ValidationError) {
	v, verr := jsonInt64("price", n)
	if verr != nil {
		return 0, verr
	}
	if v <= 0 {
		return 0, validationErr("price", "price must be greater than 0")
	}
	return v, nil
}

func validateQty(n json.Number) (int64, *ValidationError) {
	v, verr := jsonInt64("qty", n)
	if verr != nil {
		return 0, verr
	}
	if v <= 0 {
		return 0, validationErr("qty", "qty must be greater than 0")
	}
	return v, nil
}

func validateClientOrderID(id string) *ValidationError {
	if len(id) == 0 {
		return validationErr("client_order_id", "client_order_id must not be empty")
	}
	if len(id) > 64 {
		return validationErr("client_order_id", "client_order_id must be at most 64 bytes")
	}
	return nil
}
