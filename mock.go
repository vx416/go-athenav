package athena

import "github.com/aws/aws-sdk-go/service/athena/athenaiface"

// AthenaAPI is an interface that represents the AthenaAPI. It's useful for mocking.
type AthenaAPI interface {
	athenaiface.AthenaAPI
}
