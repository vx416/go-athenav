package athena

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
)

type conn struct {
	athena         athenaiface.AthenaAPI
	db             string
	OutputLocation string

	pollRetryIncrement time.Duration
	maxRetryDuration   time.Duration
	pollFrequency      time.Duration
	workGroup          *string
	dataCataLog        *string
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// if len(args) > 0 {
	// 	panic("Athena doesn't support prepared statements. Format your own arguments.")
	// }

	rows, err := c.runQuery(ctx, query, args)
	return rows, err
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// if len(args) > 0 {
	// 	panic("Athena doesn't support prepared statements. Format your own arguments.")
	// }

	_, err := c.runQuery(ctx, query, args)
	return nil, err
}

func (c *conn) runQuery(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryID, err := c.startQuery(query, args)
	if err != nil {
		return nil, err
	}

	if err := c.waitOnQuery(ctx, queryID); err != nil {
		return nil, err
	}

	return newRows(rowsConfig{
		Athena:  c.athena,
		QueryID: queryID,
		// todo add check for ddl queries to not skip header(#10)
		SkipHeader: true,
	})
}

// startQuery starts an Athena query and returns its ID.
func (c *conn) startQuery(query string, args []driver.NamedValue) (string, error) {
	input := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String(c.db),
			Catalog:  c.dataCataLog,
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(c.OutputLocation),
		},
		WorkGroup: c.workGroup,
	}
	executeParams := make([]*string, 0, len(args))
	for _, arg := range args {
		valStr, err := convertAnyToString(arg.Value)
		if err != nil {
			return "", fmt.Errorf("convert %s any type to string type failed, real type: %T", arg.Name, arg.Value)
		}
		executeParams = append(executeParams, &valStr)
	}
	if len(executeParams) > 0 {
		input.ExecutionParameters = executeParams
	}
	resp, err := c.athena.StartQueryExecution(input)
	if err != nil {
		return "", err
	}

	return *resp.QueryExecutionId, nil
}

// waitOnQuery blocks until a query finishes, returning an error if it failed.
func (c *conn) waitOnQuery(ctx context.Context, queryID string) error {
	pollFreq := c.pollFrequency
	for {
		statusResp, err := c.athena.GetQueryExecutionWithContext(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			return err
		}

		switch *statusResp.QueryExecution.Status.State {
		case athena.QueryExecutionStateCancelled:
			return context.Canceled
		case athena.QueryExecutionStateFailed:
			reason := *statusResp.QueryExecution.Status.StateChangeReason
			return errors.New(reason)
		case athena.QueryExecutionStateSucceeded:
			return nil
		case athena.QueryExecutionStateQueued:
		case athena.QueryExecutionStateRunning:
		}

		select {
		case <-ctx.Done():
			c.athena.StopQueryExecution(&athena.StopQueryExecutionInput{
				QueryExecutionId: aws.String(queryID),
			})

			return ctx.Err()
		case <-time.After(pollFreq):
			pollFreq += c.pollRetryIncrement
			if pollFreq > c.maxRetryDuration {
				pollFreq = c.maxRetryDuration
			}
			continue
		}
	}
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	panic("Athena doesn't support prepared statements")
}

func (c *conn) Begin() (driver.Tx, error) {
	panic("Athena doesn't support transactions")
}

func (c *conn) Close() error {
	return nil
}

var _ driver.QueryerContext = (*conn)(nil)
var _ driver.ExecerContext = (*conn)(nil)

// HACK(tejasmanohar): database/sql calls Prepare() if your driver doesn't implement
// Queryer. Regardless, db.Query/Exec* calls Query/Exec-Context so I've filed a bug--
// https://github.com/golang/go/issues/22980.
func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	panic("Query() is noop")
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	panic("Exec() is noop")
}

var _ driver.Queryer = (*conn)(nil)
var _ driver.Execer = (*conn)(nil)
