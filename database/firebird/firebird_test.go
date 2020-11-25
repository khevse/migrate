package firebird

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"testing"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/nakagami/firebirdsql"
	"github.com/stretchr/testify/require"
)

const (
	user     = "test_user"
	password = "123456"
	dbName   = "test.fdb"
)

var (
	opts = dktest.Options{
		PortRequired: true,
		ReadyFunc:    isReady,
		Env: map[string]string{
			"FIREBIRD_DATABASE": dbName,
			"FIREBIRD_USER":     user,
			"FIREBIRD_PASSWORD": password,
		},
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "jacobalberty/firebird:2.5-ss", Options: opts},
		{ImageName: "jacobalberty/firebird:3.0", Options: opts},
	}
)

func fbConnectionString(host, port string) string {
	//firebird://user:password@servername[:port_number]/database_name_or_file[?params1=value1[&param2=value2]...]
	return fmt.Sprintf("firebird://%s:%s@%s:%s//firebird/data/%s", user, password, host, port, dbName)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {

	db, err := createDbConnection(ctx, c)
	if err != nil {
		if !(errors.Is(err, sqldriver.ErrBadConn) || errors.Is(err, io.EOF)) {
			log.Println("is not ready: ", err)
		}
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
			{Name: "lock", Func: testLock},
			{Name: "migrate", Func: testMigrate},
			{Name: "errorParsing", Func: testErrorParsing},
			{Name: "filterCustomQuery", Func: testFilterCustomQuery},
		} {

			func() {
				ip, port, err := c.FirstPort()
				require.NoError(t, err, "Unable to get mapped port")

				addr := fbConnectionString(ip, port)

				p := &Firebird{}
				d, err := p.Open(addr)
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()

				t.Run(testcase.Name, func(*testing.T) { testcase.Func(t, d, addr, dbName) })
			}()
		}
	})
}

func testBase(t *testing.T, d database.Driver, addr, dbName string) {
	dt.Test(t, d, []byte("SELECT Count(*) FROM rdb$relations"))
}

func testMigrate(t *testing.T, d database.Driver, addr, dbName string) {
	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", dbName, d)
	require.NoError(t, err)
	dt.TestMigrate(t, m)
}

func testErrorParsing(t *testing.T, d database.Driver, addr, dbName string) {

	wantErr := `migration failed in line 0: CREATE TABLEE foo (foo varchar(40)); (details: Dynamic SQL Error
SQL error code = -104
Token unknown - line 1, column 8
TABLEE
)`

	err := d.Run(strings.NewReader("CREATE TABLEE foo (foo varchar(40));"))
	require.EqualError(t, err, wantErr)
}

func testFilterCustomQuery(t *testing.T, d database.Driver, addr, dbName string) {

	u, err := url.Parse(addr)
	require.NoError(t, err)

	q := u.Query()
	q.Set("x-custom", "foobar")
	u.RawQuery = q.Encode()

	c := &Firebird{}
	_, err = c.Open(u.String())
	require.NoError(t, err)
}

func testLock(t *testing.T, d database.Driver, addr, dbName string) {

	dt.Test(t, d, []byte("SELECT Count(*) FROM rdb$relations"))

	ps := d.(*Firebird)

	require.NoError(t, ps.Lock())
	require.NoError(t, ps.Unlock())

	require.NoError(t, ps.Lock())
	require.NoError(t, ps.Unlock())
}

func createDbConnection(ctx context.Context, c dktest.ContainerInfo) (*sql.DB, error) {

	ip, port, err := c.FirstPort()
	if err != nil {
		return nil, fmt.Errorf("port error: %w", err)
	}

	db, err := sql.Open("firebirdsql", fbConnectionString(ip, port))
	if err != nil {
		return nil, fmt.Errorf("open error: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping error: %w", err)
	}

	return db, nil
}
