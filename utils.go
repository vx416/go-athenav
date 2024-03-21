package athena

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cast"
)

type Stringer interface {
	String() string
}

func convertAnyToString(anyValue any) (string, error) {
	return cast.ToStringE(anyValue)
}

type AthenaDate time.Time

func (t AthenaDate) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t AthenaDate) String() string {
	return time.Time(t).Format(DateLayout)
}

func (t AthenaDate) Equal(t2 AthenaDate) bool {
	return time.Time(t).Equal(time.Time(t2))
}

func (t AthenaDate) ToQueryValue() string {
	return fmt.Sprintf("date '%s'", t.String())
}
