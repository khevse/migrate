package cockroachdb

// error codes https://github.com/lib/pq/blob/master/error.go

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

const defaultPort = 26257

var (
	opts = dktest.Options{Cmd: []string{"start", "--insecure"}, PortRequired: true, ReadyFunc: isReady}
	// Released versions: https://www.cockroachlabs.com/docs/releases/
	specs = []dktesting.ContainerSpec{
		{ImageName: "cockroachdb/cockroach:v1.0.7", Options: opts},
		{ImageName: "cockroachdb/cockroach:v1.1.9", Options: opts},
		{ImageName: "cockroachdb/cockroach:v2.0.7", Options: opts},
		{ImageName: "cockroachdb/cockroach:v2.1.3", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {

	db, err := createDbConnection(ctx, c)
	if err != nil {
		log.Println("is not ready: ", err)
		return false
	}

	if err := db.Close(); err != nil {
		log.Println("close error:", err)
	}

	return true
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {

		for _, testcase := range []struct {
			Name string
			Func func(_ *testing.T, _ database.Driver, addr, dbName string)
		}{
			{Name: "base", Func: testBase},
			{Name: "migrate", Func: testMigrate},
			{Name: "multiStatement", Func: testMultiStatement},
			{Name: "filterCustomQuery", Func: testFilterCustomQuery},
		} {

			func() {

				dbName := fmt.Sprintf("test%d", time.Now().UnixNano())
				{
					// create test database
					db, err := createDbConnection(context.Background(), c)
					require.NoError(t, err)
					defer func() { require.NoError(t, db.Close()) }()

					defer func() {
						_, err = db.Exec("DROP DATABASE IF EXISTS " + dbName)
						require.NoError(t, err)
					}()

					_, err = db.Exec("CREATE DATABASE " + dbName)
					require.NoError(t, err)
				}

				ip, port, err := c.Port(defaultPort)
				require.NoError(t, err, "Unable to get mapped port")

				addr := fmt.Sprintf("cockroach://root@%v:%v/%s?sslmode=disable", ip, port, dbName)

				c := &CockroachDb{}
				d, err := c.Open(addr)
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()

				t.Run(testcase.Name, func(*testing.T) { testcase.Func(t, d, addr, dbName) })
			}()
		}
	})
}

func testBase(t *testing.T, d database.Driver, addr, dbName string) {
	dt.Test(t, d, []byte("SELECT table_name from information_schema.tables"))
}

func testMigrate(t *testing.T, d database.Driver, addr, dbName string) {
	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", dbName, d)
	require.NoError(t, err)
	dt.TestMigrate(t, m)
}

func testMultiStatement(t *testing.T, d database.Driver, addr, dbName string) {

	err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLE bar (bar text);"))
	require.NoError(t, err)

	// make sure second table exists
	var exists bool
	err = d.(*CockroachDb).db.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bar' AND table_schema = (SELECT current_schema()))").Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists)
}

func testFilterCustomQuery(t *testing.T, d database.Driver, addr, dbName string) {

	u, err := url.Parse(addr)
	require.NoError(t, err)

	q := u.Query()
	q.Set("x-custom", "foobar")
	u.RawQuery = q.Encode()

	c := &CockroachDb{}
	_, err = c.Open(u.String())
	require.NoError(t, err)
}

func createDbConnection(ctx context.Context, c dktest.ContainerInfo) (*sql.DB, error) {

	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return nil, fmt.Errorf("port error: %w", err)
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://root@%v:%v?sslmode=disable", ip, port))
	if err != nil {
		return nil, fmt.Errorf("open error: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping error: %w", err)
	}

	return db, nil
}
