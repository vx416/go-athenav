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
func MockQuery(mocker Mocker, mockColumnsName []string, mockColumnsType []string, mockDataRows [][]string) {
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
			Name: &mockColumnsName[i],
		}
	}
	athenaRows := make([]*athena.Row, 0, len(mockDataRows))
	for _, rowData := range mockDataRows {
		datum := make([]*athena.Datum, len(rowData))
		for i, val := range rowData {
			datum[i] = &athena.Datum{
				VarCharValue: &val,
			}
		}
		athenaRows = append(athenaRows, &athena.Row{
			Data: datum,
		})
	}
	mocker.On("GetQueryResults", mock.Anything, mock.Anything).Return(&athena.GetQueryResultsOutput{
		ResultSet: &athena.ResultSet{
			ResultSetMetadata: &athena.ResultSetMetadata{
				ColumnInfo: columnInfos,
			},
			Rows: athenaRows,
		},
	}, nil)
}
