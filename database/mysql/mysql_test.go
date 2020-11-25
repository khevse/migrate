package mysql

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/go-sql-driver/mysql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/require"
)

const defaultPort = 3306

var (
	opts = dktest.Options{
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "root", "MYSQL_DATABASE": "public"},
		PortRequired: true, ReadyFunc: isReady,
	}
	// Supported versions: https://www.mysql.com/support/supportedplatforms/database.html
	specs = []dktesting.ContainerSpec{
		{ImageName: "mysql:5.5", Options: opts},
		{ImageName: "mysql:5.6", Options: opts},
		{ImageName: "mysql:5.7", Options: opts},
		{ImageName: "mysql:8", Options: opts},
	}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {

	db, err := createDbConnection(ctx, c)
	if err != nil {
		if !(errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, sqldriver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn)) {
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
	// mysql.SetLogger(mysql.Logger(log.New(ioutil.Discard, "", log.Ltime)))

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {

		for _, testcase := range []struct {
			Name string
			Func func(_ *testing.T, _ database.Driver, addr, dbName string)
		}{
			{Name: "base", Func: testBase},
			{Name: "migrate", Func: testMigrate},
			{Name: "lockWorks", Func: testLockWorks},
			{Name: "noLockParamValidation", Func: testNoLockParamValidation},
			{Name: "noLockWorks", Func: testNoLockWorks},
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

				addr := fmt.Sprintf("mysql://root:root@tcp(%v:%v)/%s", ip, port, dbName)

				c := &Mysql{}
				d, err := c.Open(addr)
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()

				t.Run(testcase.Name, func(*testing.T) { testcase.Func(t, d, addr, dbName) })
			}()
		}
	})
}

func testBase(t *testing.T, d database.Driver, addr, dbName string) {

	dt.Test(t, d, []byte("SELECT 1"))

	// check ensureVersionTable
	require.NoError(t, d.(*Mysql).ensureVersionTable())
	// check again
	require.NoError(t, d.(*Mysql).ensureVersionTable())
}

func testMigrate(t *testing.T, d database.Driver, addr, dbName string) {
	// mysql.SetLogger(mysql.Logger(log.New(ioutil.Discard, "", log.Ltime)))

	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", dbName, d)
	require.NoError(t, err)
	dt.TestMigrate(t, m)

	// check ensureVersionTable
	require.NoError(t, d.(*Mysql).ensureVersionTable())
	// check again
	require.NoError(t, d.(*Mysql).ensureVersionTable())
}

func testLockWorks(t *testing.T, d database.Driver, addr, dbName string) {

	dt.Test(t, d, []byte("SELECT 1"))

	ms := d.(*Mysql)

	require.NoError(t, ms.Lock())
	require.NoError(t, ms.Unlock())

	// make sure the 2nd lock works (RELEASE_LOCK is very finicky)
	require.NoError(t, ms.Lock())
	require.NoError(t, ms.Unlock())
}

func testNoLockParamValidation(t *testing.T, d database.Driver, addr, dbName string) {

	ip := "127.0.0.1"
	port := 3306
	addr = fmt.Sprintf("mysql://root:root@tcp(%v:%v)/%s", ip, port, dbName)
	p := &Mysql{}
	_, err := p.Open(addr + "?x-no-lock=not-a-bool")
	require.Truef(t,
		errors.Is(err, strconv.ErrSyntax),
		"Expected syntax error when passing a non-bool as x-no-lock parameter: %v", err)
}

func testNoLockWorks(t *testing.T, d database.Driver, addr, dbName string) {

	lock := d.(*Mysql)

	p := &Mysql{}
	d, err := p.Open(addr + "?x-no-lock=true")
	require.NoError(t, err)

	noLock := d.(*Mysql)

	// Should be possible to take real lock and no-lock at the same time
	require.NoError(t, lock.Lock())
	require.NoError(t, noLock.Lock())
	require.NoError(t, lock.Unlock())
	require.NoError(t, noLock.Unlock())
}

func TestExtractCustomQueryParams(t *testing.T) {
	testcases := []struct {
		name                 string
		config               *mysql.Config
		expectedParams       map[string]string
		expectedCustomParams map[string]string
		expectedErr          error
	}{
		{name: "nil config", expectedErr: ErrNilConfig},
		{
			name:                 "no params",
			config:               mysql.NewConfig(),
			expectedCustomParams: map[string]string{},
		},
		{
			name:                 "no custom params",
			config:               &mysql.Config{Params: map[string]string{"hello": "world"}},
			expectedParams:       map[string]string{"hello": "world"},
			expectedCustomParams: map[string]string{},
		},
		{
			name: "one param, one custom param",
			config: &mysql.Config{
				Params: map[string]string{"hello": "world", "x-foo": "bar"},
			},
			expectedParams:       map[string]string{"hello": "world"},
			expectedCustomParams: map[string]string{"x-foo": "bar"},
		},
		{
			name: "multiple params, multiple custom params",
			config: &mysql.Config{
				Params: map[string]string{
					"hello": "world",
					"x-foo": "bar",
					"dead":  "beef",
					"x-cat": "hat",
				},
			},
			expectedParams:       map[string]string{"hello": "world", "dead": "beef"},
			expectedCustomParams: map[string]string{"x-foo": "bar", "x-cat": "hat"},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			customParams, err := extractCustomQueryParams(tc.config)
			if tc.config != nil {
				require.Equal(t, tc.expectedParams, tc.config.Params,
					"Expected config params have custom params properly removed")
			}
			require.Equal(t, tc.expectedErr, err, "Expected errors to match")
			require.Equal(t, tc.expectedCustomParams, customParams,
				"Expected custom params to be properly extracted")
		})
	}
}

func createTmpCert(t *testing.T) string {
	tmpCertFile, err := ioutil.TempFile("", "migrate_test_cert")
	require.NoError(t, err, "Failed to create temp cert file")

	t.Cleanup(func() {
		require.NoError(t, err, os.Remove(tmpCertFile.Name()), "Failed to cleanup temp cert file")
	})

	r := rand.New(rand.NewSource(0))
	pub, priv, err := ed25519.GenerateKey(r)
	require.NoError(t, err, "Failed to generate ed25519 key for temp cert file")

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(0),
	}
	derBytes, err := x509.CreateCertificate(r, &tmpl, &tmpl, pub, priv)
	require.NoError(t, err, "Failed to generate temp cert file")

	err = pem.Encode(tmpCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	require.NoError(t, err, "Failed to encode")

	require.NoError(t, err, tmpCertFile.Close(), "Failed to close temp cert file")

	return tmpCertFile.Name()
}

func TestURLToMySQLConfig(t *testing.T) {
	tmpCertFilename := createTmpCert(t)
	tmpCertFilenameEscaped := url.PathEscape(tmpCertFilename)

	testcases := []struct {
		name        string
		urlStr      string
		expectedDSN string // empty string signifies that an error is expected
	}{
		{name: "no user/password", urlStr: "mysql://tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "only user", urlStr: "mysql://username@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "only user - with encoded :",
			urlStr:      "mysql://username%3A@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "only user - with encoded @",
			urlStr:      "mysql://username%40@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username@@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password", urlStr: "mysql://username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		// Not supported yet: https://github.com/go-sql-driver/mysql/issues/591
		// {name: "user/password - user with encoded :",
		// 	urlStr:      "mysql://username%3A:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
		// 	expectedDSN: "username::pasword@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - user with encoded @",
			urlStr:      "mysql://username%40:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username@:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - password with encoded :",
			urlStr:      "mysql://username:password%3A@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password:@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - password with encoded @",
			urlStr:      "mysql://username:password%40@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password@@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "custom tls",
			urlStr:      "mysql://username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true&tls=custom&x-tls-ca=" + tmpCertFilenameEscaped,
			expectedDSN: "username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true&tls=custom&x-tls-ca=" + tmpCertFilenameEscaped},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := urlToMySQLConfig(tc.urlStr)
			require.NoError(t, err)

			dsn := config.FormatDSN()
			require.Equal(t, tc.expectedDSN, dsn)
		})
	}
}

func createDbConnection(ctx context.Context, c dktest.ContainerInfo) (*sql.DB, error) {

	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return nil, fmt.Errorf("port error: %w", err)
	}

	db, err := sql.Open("mysql", fmt.Sprintf("root:root@tcp(%v:%v)/public", ip, port))
	if err != nil {
		return nil, fmt.Errorf("open error: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping error: %w", err)
	}

	return db, nil
}
