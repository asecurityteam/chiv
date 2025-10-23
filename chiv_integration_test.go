//go:build integration

package chiv_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/asecurityteam/chiv"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func TestArchiver_Archive(t *testing.T) {
	type call struct {
		expected string
		table    string
		key      string
		options  []chiv.Option
	}

	cases := []struct {
		name     string
		driver   string
		database string
		setup    string
		teardown string
		bucket   string
		options  []chiv.Option
		calls    []call
	}{
		{
			name:     "postgres to csv",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.csv",
					table:    "postgres_table",
					key:      "postgres_table.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to csv key override",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithKey("archive.csv"),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.csv",
					table:    "postgres_table",
					key:      "archive.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to csv null override",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithNull("custom_null"),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres_with_null.csv",
					table:    "postgres_table",
					key:      "postgres_table.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to json",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.JSON),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.json",
					table:    "postgres_table",
					key:      "postgres_table.json",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to yaml",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.yaml",
					table:    "postgres_table",
					key:      "postgres_table.yaml",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres two formats",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.json",
					table:    "postgres_table",
					key:      "postgres_table.json",
					options: []chiv.Option{
						chiv.WithFormat(chiv.JSON),
					},
				},
				{
					expected: "./testdata/postgres/postgres.yaml",
					table:    "postgres_table",
					key:      "postgres_table.yaml",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres one-off extension",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.yaml",
					table:    "postgres_table",
					key:      "postgres_table.not_yaml",
					options: []chiv.Option{
						chiv.WithExtension("not_yaml"),
					},
				},
				{
					expected: "./testdata/postgres/postgres.yaml",
					table:    "postgres_table",
					key:      "postgres_table.yaml",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres two tables",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/two_tables_setup.sql",
			teardown: "./testdata/postgres/two_tables_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.CSV),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/two_tables_first.csv",
					table:    "first_table",
					key:      "first_table.csv",
					options:  []chiv.Option{},
				},
				{
					expected: "./testdata/postgres/two_tables_second.csv",
					table:    "second_table",
					key:      "second_table.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "with columns",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres_subset.csv",
					table:    "postgres_table",
					key:      "postgres_table.csv",
					options: []chiv.Option{
						chiv.WithColumns("id", "text_column", "int_column"),
					},
				},
			},
		},
		{
			name:     "mariadb happy path csv",
			driver:   "mysql",
			database: os.Getenv("MARIADB_URL"),
			setup:    "./testdata/mariadb/setup.sql",
			teardown: "./testdata/mariadb/teardown.sql",
			bucket:   "test_bucket",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./testdata/mariadb/happy.csv",
					table:    "test_table",
					key:      "test_table.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "mariadb happy path yaml",
			driver:   "mysql",
			database: os.Getenv("MARIADB_URL"),
			setup:    "./testdata/mariadb/setup.sql",
			teardown: "./testdata/mariadb/teardown.sql",
			bucket:   "mariadb_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./testdata/mariadb/happy.yaml",
					table:    "test_table",
					key:      "test_table.yaml",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "mariadb happy path json",
			driver:   "mysql",
			database: os.Getenv("MARIADB_URL"),
			setup:    "./testdata/mariadb/setup.sql",
			teardown: "./testdata/mariadb/teardown.sql",
			bucket:   "mariadb_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.JSON),
			},
			calls: []call{
				{
					expected: "./testdata/mariadb/happy.json",
					table:    "test_table",
					key:      "test_table.json",
					options:  []chiv.Option{},
				},
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var (
				ctx        = context.Background()
				db         = newDB(t, test.driver, test.database)
				s3client   = newS3Client(ctx, t, os.Getenv("AWS_REGION"), os.Getenv("AWS_ENDPOINT"))
				uploader   = manager.NewUploader(s3client)
				downloader = manager.NewDownloader(s3client)
			)
			defer db.Close()

			exec(t, db, readFile(t, test.setup))
			defer exec(t, db, readFile(t, test.teardown))

			createBucket(ctx, t, s3client, test.bucket)
			defer deleteBucket(ctx, t, s3client, test.bucket)

			subject := chiv.NewArchiver(db, uploader, test.options...)
			require.NotNil(t, subject)

			for _, call := range test.calls {
				require.NoError(t, subject.Archive(call.table, test.bucket, call.options...))

				expected := readFile(t, call.expected)
				actual := download(ctx, t, downloader, test.bucket, call.key)
				require.Equal(t, expected, actual)
			}
		})
	}
}

func TestArchiveWithContext(t *testing.T) {
	var (
		database   = os.Getenv("POSTGRES_URL")
		driver     = "postgres"
		bucket     = "postgres_bucket"
		table      = "postgres_table"
		key        = "postgres_table.csv"
		setup      = "./testdata/postgres/postgres_setup.sql"
		teardown   = "./testdata/postgres/postgres_teardown.sql"
		expected   = "./testdata/postgres/postgres.csv"
		db         = newDB(t, driver, database)
		ctx        = context.Background()
		s3client   = newS3Client(ctx, t, os.Getenv("AWS_REGION"), os.Getenv("AWS_ENDPOINT"))
		uploader   = manager.NewUploader(s3client)
		downloader = manager.NewDownloader(s3client)
	)
	defer db.Close()

	exec(t, db, readFile(t, setup))
	defer exec(t, db, readFile(t, teardown))

	createBucket(ctx, t, s3client, bucket)
	defer deleteBucket(ctx, t, s3client, bucket)

	require.NoError(t, chiv.ArchiveWithContext(ctx, db, uploader, table, bucket))

	actual := download(ctx, t, downloader, bucket, key)
	require.Equal(t, readFile(t, expected), actual)
}

func TestArchiveRowsJoin(t *testing.T) {
	var (
		database   = os.Getenv("POSTGRES_URL")
		driver     = "postgres"
		bucket     = "join_bucket"
		key        = "join.csv"
		setup      = "./testdata/postgres/join_setup.sql"
		teardown   = "./testdata/postgres/join_teardown.sql"
		expected   = "./testdata/postgres/join.csv"
		db         = newDB(t, driver, database)
		ctx        = context.Background()
		s3client   = newS3Client(ctx, t, os.Getenv("AWS_REGION"), os.Getenv("AWS_ENDPOINT"))
		uploader   = manager.NewUploader(s3client)
		downloader = manager.NewDownloader(s3client)
	)
	defer db.Close()

	exec(t, db, readFile(t, setup))
	defer exec(t, db, readFile(t, teardown))

	createBucket(ctx, t, s3client, bucket)
	defer deleteBucket(ctx, t, s3client, bucket)

	rows, err := db.Query(`
		SELECT first_table.id, first_text, first_int, second_text, second_int
		FROM first_table
		JOIN second_table ON first_table.id = second_table.id;
	`)
	if err != nil {
		t.Fatal(err)
	}

	require.NoError(t, chiv.ArchiveRows(rows, uploader, bucket, chiv.WithKey("join.csv")))

	actual := download(ctx, t, downloader, bucket, key)
	require.Equal(t, readFile(t, expected), actual)
}

const (
	retryLimit    = 15
	retryInterval = 3 * time.Second
)

func newDB(t testing.TB, driver string, url string) *sql.DB {
	t.Helper()

	db, err := sql.Open(driver, url)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < retryLimit; i++ {
		if err := db.Ping(); err != nil {
			if i < retryLimit {
				time.Sleep(retryInterval)
				continue
			}
			t.Fatalf("connecting to %s: %s", driver, err)
		}
		break
	}

	return db
}

func newS3Client(ctx context.Context, t testing.TB, region string, endpoint string) *s3.Client {
	t.Helper()

	awsConfig, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(region),
		config.WithBaseEndpoint(endpoint),
	)
	if err != nil {
		t.Fatal(err)
	}

	return s3.NewFromConfig(awsConfig)
}

func exec(t testing.TB, db *sql.DB, statements string) {
	s := strings.Split(statements, ";\n\n")
	for _, statement := range s {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
}

func createBucket(ctx context.Context, t testing.TB, client *s3.Client, name string) {
	t.Helper()

	if _, err := client.CreateBucket(ctx,
		&s3.CreateBucketInput{
			Bucket: aws.String(name),
		},
	); err != nil {
		t.Fatal(err)
	}
}

func deleteBucket(ctx context.Context, t testing.TB, client *s3.Client, name string) {
	t.Helper()

	// we could do this more cleanly with BatchDeleteIterator, but localstack doesn't like batch deletes :shrug:
	out, err := client.ListObjects(ctx,
		&s3.ListObjectsInput{
			Bucket: aws.String(name),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, o := range out.Contents {
		if _, err := client.DeleteObject(ctx,
			&s3.DeleteObjectInput{
				Key:    o.Key,
				Bucket: aws.String(name),
			},
		); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := client.DeleteBucket(ctx,
		&s3.DeleteBucketInput{
			Bucket: aws.String(name),
		},
	); err != nil {
		t.Fatal(err)
	}
}

func readFile(t testing.TB, path string) string {
	t.Helper()

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return string(contents)
}

func download(ctx context.Context, t testing.TB, downloader *manager.Downloader, bucket string, key string) string {
	t.Helper()

	b := &manager.WriteAtBuffer{}
	_, err := downloader.Download(ctx, b, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		fmt.Println("error:", bucket, key)
		t.Fatal(err)
	}

	return string(b.Bytes())
}
