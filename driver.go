package athena

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
)

var (
	openFromSessionMutex          sync.Mutex
	openFromSessionCount          int
	defaultPollFrequency          = 1 * time.Second
	defaultMaxRetryDuration       = 3 * time.Second
	defaultRetryDurationIncrement = 300 * time.Millisecond
	mockEnabled                   = false
	mockAthenaClientImpl          athenaiface.AthenaAPI
)

// EnableMockMode allows you to use a mock implementation of the Athena API.
func EnableMockMode(mockAthenaClient athenaiface.AthenaAPI) {
	mockEnabled = true
	mockAthenaClientImpl = mockAthenaClient
}

// Driver is a sql.Driver. It's intended for db/sql.Open().
type Driver struct {
	cfg *Config
}

// NewDriver allows you to register your own driver with `sql.Register`.
// It's useful for more complex use cases. Read more in PR #3.
// https://github.com/segmentio/go-athena/pull/3
//
// Generally, sql.Open() or athena.Open() should suffice.
func NewDriver(cfg *Config) *Driver {
	return &Driver{cfg}
}

func init() {
	var drv driver.Driver = &Driver{}
	sql.Register("athena", drv)
}

// Open should be used via `db/sql.Open("athena", "<params>")`.
// The following parameters are supported in URI query format (k=v&k2=v2&...)
// example: "db=default&data_catalog=default&aws_access_key_id=default&aws_access_key_secrete=default&region=default&output_location=s3://results"
//
// - `db` (required) refer to https://docs.aws.amazon.com/athena/latest/ug/understanding-tables-databases-and-the-data-catalog.html
// This is the Athena database name. In the UI, this defaults to "default",
// but the driver requires it regardless.
//
// - `output_location` (required)
// This is the S3 location Athena will dump query results in the format
// "s3://bucket/and/so/forth". In the AWS UI, this defaults to
// "s3://aws-athena-query-results-<ACCOUNTID>-<REGION>", but the driver requires it.
//
// - `poll_frequency` (optional)
// Athena's API requires polling to retrieve query results. This is the frequency at
// which the driver will poll for results. It should be a time/Duration.String().
// A completely arbitrary default of "5s" was chosen.
//
// - `work_group` (optional)
// Athena's API allows you to specify a workgroup for queries. This is the name of
// the workgroup you want to use. If not specified, the default workgroup is used.
//
// - `data_catalog` (required) refer to https://docs.aws.amazon.com/athena/latest/ug/understanding-tables-databases-and-the-data-catalog.html
// Athena's API allows you to specify a data catalog for queries. This is the name of
// the data catalog you want to use. If not specified, the default data catalog is used.
//
// - `region` (required)
// Override AWS region. Useful if it is not set with environment variable.
//
// - `aws_access_key_id` (required)
// AWS access key id. Useful if it is not set with environment variable.
//
// - `aws_access_key_secret` (required)
// AWS access key secret. Useful if it is not set with environment variable.
// Credentials must be accessible via the SDK's Default Credential Provider Chain.
// For more advanced AWS credentials/session/config management, please supply
// a custom AWS session directly via `athena.Open()`.
func (d *Driver) Open(connStr string) (driver.Conn, error) {
	if mockEnabled {
		return &conn{
			athena: mockAthenaClientImpl,
		}, nil
	}

	cfg := d.cfg
	if cfg == nil {
		var err error
		cfg, err = configFromConnectionString(connStr)
		if err != nil {
			return nil, err
		}
	}

	if cfg.PollFrequency == 0 {
		cfg.PollFrequency = defaultPollFrequency
	}
	if cfg.PollRetryIncrement == 0 {
		cfg.PollRetryIncrement = defaultRetryDurationIncrement
	}
	if cfg.MaxRetryDuration == 0 {
		cfg.MaxRetryDuration = defaultMaxRetryDuration
	}

	return &conn{
		athena:             athena.New(cfg.Session),
		db:                 cfg.Database,
		OutputLocation:     cfg.OutputLocation,
		pollFrequency:      cfg.PollFrequency,
		pollRetryIncrement: cfg.PollRetryIncrement,
		maxRetryDuration:   cfg.MaxRetryDuration,
		workGroup:          cfg.WorkerGroup,
		dataCataLog:        cfg.DataCateLog,
	}, nil
}

// Open is a more robust version of `db.Open`, as it accepts a raw aws.Session.
// This is useful if you have a complex AWS session since the driver doesn't
// currently attempt to serialize all options into a string.
func Open(cfg Config) (*sql.DB, error) {
	if cfg.Database == "" {
		return nil, errors.New("db is required")
	}

	if cfg.OutputLocation == "" {
		return nil, errors.New("s3_staging_url is required")
	}

	if cfg.Session == nil {
		return nil, errors.New("session is required")
	}

	// This hack was copied from jackc/pgx. Sorry :(
	// https://github.com/jackc/pgx/blob/70a284f4f33a9cc28fd1223f6b83fb00deecfe33/stdlib/sql.go#L130-L136
	openFromSessionMutex.Lock()
	openFromSessionCount++
	name := fmt.Sprintf("athena-%d", openFromSessionCount)
	openFromSessionMutex.Unlock()

	sql.Register(name, &Driver{&cfg})
	return sql.Open(name, "")
}

// Config is the input to Open().
type Config struct {
	Session        *session.Session
	Database       string
	OutputLocation string

	PollFrequency      time.Duration
	PollRetryIncrement time.Duration
	MaxRetryDuration   time.Duration
	WorkerGroup        *string
	DataCateLog        *string
}

func configFromConnectionString(connStr string) (*Config, error) {
	args, err := url.ParseQuery(connStr)
	if err != nil {
		return nil, err
	}

	var cfg Config

	var acfg []*aws.Config
	region := args.Get("region")
	awsAccessKey := args.Get("aws_access_key_id")
	awsAccessKeySecret := args.Get("aws_access_key_secret")
	if region == "" || awsAccessKey == "" || awsAccessKeySecret == "" {
		return nil, fmt.Errorf("region, aws_access_key_id and aws_access_key_secret are required")
	}

	acfg = append(acfg, &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(awsAccessKey, awsAccessKeySecret, ""),
	})
	cfg.Session, err = session.NewSession(acfg...)
	if err != nil {
		return nil, err
	}

	cfg.Database = args.Get("db")
	cfg.OutputLocation = args.Get("output_location")

	frequencyStr := args.Get("poll_frequency")
	if frequencyStr != "" {
		cfg.PollFrequency, err = time.ParseDuration(frequencyStr)
		if err != nil {
			return nil, fmt.Errorf("invalid poll_frequency parameter: %s", frequencyStr)
		}
	}
	workerGroupStr := args.Get("work_group")
	if workerGroupStr != "" {
		cfg.WorkerGroup = aws.String(workerGroupStr)
	}
	dataCateLogStr := args.Get("data_catalog")
	if dataCateLogStr != "" {
		cfg.DataCateLog = aws.String(dataCateLogStr)
	}
	return &cfg, nil
}
