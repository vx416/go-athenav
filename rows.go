package athena

import (
	"database/sql/driver"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
)

const (
	maxResultCnt = 1000
)

type rows struct {
	athena  athenaiface.AthenaAPI
	queryID string

	done          bool
	skipHeaderRow bool
	out           *athena.GetQueryResultsOutput
}

type rowsConfig struct {
	Athena     athenaiface.AthenaAPI
	QueryID    string
	SkipHeader bool
}

func newRows(cfg rowsConfig) (*rows, error) {
	r := rows{
		athena:        cfg.Athena,
		queryID:       cfg.QueryID,
		skipHeaderRow: cfg.SkipHeader,
	}

	shouldContinue, err := r.fetchNextPage(nil)
	if err != nil {
		return nil, err
	}

	r.done = !shouldContinue
	return &r, nil
}

func (r *rows) Columns() []string {
	var columns []string
	for _, colInfo := range r.out.ResultSet.ResultSetMetadata.ColumnInfo {
		columns = append(columns, *colInfo.Name)
	}

	return columns
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	colInfo := r.out.ResultSet.ResultSetMetadata.ColumnInfo[index]
	if colInfo.Type != nil {
		return *colInfo.Type
	}
	return ""
}

func (r *rows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	if r.out == nil || r.out.ResultSet == nil {
		return io.EOF
	}
	// If nothing left to iterate...
	if len(r.out.ResultSet.Rows) == 0 {
		// And if nothing more to paginate...
		if r.out.NextToken == nil || *r.out.NextToken == "" {
			return io.EOF
		}

		cont, err := r.fetchNextPage(r.out.NextToken)
		if err != nil {
			return err
		}

		if !cont {
			return io.EOF
		}
	}
	currentRow := r.popRowInResultSet()
	if currentRow == nil {
		return io.EOF
	}
	columns := r.out.ResultSet.ResultSetMetadata.ColumnInfo
	if err := convertRow(columns, currentRow.Data, dest); err != nil {
		return err
	}
	return nil
}

func (r *rows) fetchNextPage(token *string) (bool, error) {
	// if there are rows left in the current page, return true, else fetch next page
	if r.out != nil && r.out.ResultSet != nil && len(r.out.ResultSet.Rows) > 0 {
		return true, nil
	}
	var err error
	r.out, err = r.athena.GetQueryResults(&athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(r.queryID),
		NextToken:        token,
		MaxResults:       aws.Int64(maxResultCnt),
	})
	if err != nil {
		return false, err
	}

	//  If there are no rows in the result set, return false
	if r.out == nil || r.out.ResultSet == nil || len(r.out.ResultSet.Rows) == 0 {
		return false, nil
	}
	// First row of the first page contains header if the query is not DDL.
	// These are also available in *athena.Row.ResultSetMetadata.
	if r.skipHeaderRow {
		r.out.ResultSet.Rows = r.out.ResultSet.Rows[1:]
	}
	return true, nil
}

func (r *rows) popRowInResultSet() *athena.Row {
	if r.out == nil {
		return nil
	}
	if len(r.out.ResultSet.Rows) == 0 {
		return nil
	}
	row := r.out.ResultSet.Rows[0]
	r.out.ResultSet.Rows = r.out.ResultSet.Rows[1:]
	return row
}

func (r *rows) Close() error {
	r.done = true
	return nil
}
