package cassandra

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/gocql/gocql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/require"
)

var (
	opts = dktest.Options{PortRequired: true, ReadyFunc: isReady}
	// Supported versions: http://cassandra.apache.org/download/
	// Although Cassandra 2.x is supported by the Apache Foundation,
	// the migrate db driver only supports Cassandra 3.x since it uses
	// the system_schema keyspace.
	specs = []dktesting.ContainerSpec{
		{ImageName: "cassandra:3.0", Options: opts},
		{ImageName: "cassandra:3.11", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {

	s, err := createSession(c)
	if err != nil {
		log.Println("is not ready: ", err)
		return false
	}
	defer s.Close()

	return true
}

func Test(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {

		for _, testcase := range []struct {
			Name string
			Func func(*testing.T, database.Driver, string)
		}{
			{Name: "base", Func: testBase},
			{Name: "migrate", Func: testMigrate},
		} {

			func() {

				keySpace := fmt.Sprintf("testks%d", time.Now().UnixNano())
				{
					// create test keyspace
					s, err := createSession(c)
					require.NoError(t, err)
					defer s.Close()

					query := fmt.Sprintf("CREATE KEYSPACE %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1}", keySpace)
					err = s.Query(query).Exec()
					require.NoError(t, err)
					defer func() { s.Query("DROP KEYSPACE IF EXISTS " + keySpace).Exec() }()
				}

				ip, port, err := c.Port(9042)
				require.NoError(t, err, "Unable to get mapped port")

				addr := fmt.Sprintf("cassandra://%v:%v/"+keySpace, ip, port)
				p := &Cassandra{}
				d, err := p.Open(addr)
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()

				t.Run(testcase.Name, func(*testing.T) { testcase.Func(t, d, keySpace) })
			}()
		}
	})
}

func testBase(t *testing.T, d database.Driver, keySpace string) {
	dt.Test(t, d, []byte("SELECT table_name from system_schema.tables"))
}

func testMigrate(t *testing.T, d database.Driver, keySpace string) {
	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", keySpace, d)
	require.NoError(t, err)
	dt.TestMigrate(t, m)
}

func createSession(c dktest.ContainerInfo) (*gocql.Session, error) {
	// Cassandra exposes 5 ports (7000, 7001, 7199, 9042 & 9160)
	// We only need the port bound to 9042
	ip, portStr, err := c.Port(9042)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}

	cluster := gocql.NewCluster(ip)
	cluster.Port = port
	cluster.Consistency = gocql.All
	p, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return p, nil
}
