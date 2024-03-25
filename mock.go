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
// e.g MockQuery(&mockAPI, map[string]string{"id": "string"}, [][]string{{"1"}})
func MockQuery(mocker Mocker, columnNameTypeMap map[string]string, mockDataRows [][]string) {
	queryID := fmt.Sprintf("query-%d", time.Now().UnixNano())
	state := athena.QueryExecutionStateSucceeded
	mocker.On("StartQueryExecution", mock.Anything, mock.Anything).Return(&athena.StartQueryExecutionOutput{QueryExecutionId: &queryID}, nil)
	mocker.On("GetQueryExecutionWithContext", mock.Anything, mock.Anything).Return(&athena.GetQueryExecutionOutput{QueryExecution: &athena.QueryExecution{Status: &athena.QueryExecutionStatus{
		State: &state,
	}}}, nil)
	columnInfos := make([]*athena.ColumnInfo, 0, len(columnNameTypeMap))
	for colName, colType := range columnNameTypeMap {
		colNameTemp := colName
		colTypeTemp := colType
		columnInfos = append(columnInfos, &athena.ColumnInfo{
			Type: &colTypeTemp,
			Name: &colNameTemp,
		})
	}

	athenaRows := make([]*athena.Row, 1, len(mockDataRows))
	for _, rowData := range mockDataRows {
		datum := make([]*athena.Datum, len(rowData))
		for i, val := range rowData {
			valTemp := val
			datum[i] = &athena.Datum{
				VarCharValue: &valTemp,
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
