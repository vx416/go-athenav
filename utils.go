package athena

import (
	"github.com/spf13/cast"
)

type Stringer interface {
	String() string
}

func convertAnyToString(anyValue any) (string, error) {
	return cast.ToStringE(anyValue)
}
