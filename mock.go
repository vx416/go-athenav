package athena

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"github.com/stretchr/testify/mock"
)

// AthenaAPI is an interface that represents the AthenaAPI. It's useful for mocking.
type AthenaAPI interface {
	athenaiface.AthenaAPI
}

type Mocker interface {
	On(methodName string, arguments ...interface{}) *mock.Call
}

// MockQuery mocks the AthenaAPI to return the given columns and data rows.
func MockQuery(mocker Mocker, mockColumnsType []string, mockDataRows []*athena.Row) {
	queryID := fmt.Sprintf("query-%d", time.Now().UnixNano())
	state := athena.QueryExecutionStateSucceeded
	mocker.On("StartQueryExecution", mock.Anything, mock.Anything).Return(&athena.StartQueryExecutionOutput{QueryExecutionId: &queryID}, nil)
	mocker.On("GetQueryExecutionWithContext", mock.Anything, mock.Anything).Return(&athena.GetQueryExecutionOutput{QueryExecution: &athena.QueryExecution{Status: &athena.QueryExecutionStatus{
		State: &state,
	}}}, nil)
	columnInfos := make([]*athena.ColumnInfo, len(mockColumnsType))
	for i, colType := range mockColumnsType {
		columnInfos[i] = &athena.ColumnInfo{
			Type: &colType,
		}
	}
	mocker.On("GetQueryResults", mock.Anything, mock.Anything).Return(&athena.GetQueryResultsOutput{
		ResultSet: &athena.ResultSet{
			ResultSetMetadata: &athena.ResultSetMetadata{
				ColumnInfo: columnInfos,
			},
			Rows: mockDataRows,
		},
	}, nil)
}
