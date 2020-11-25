package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	opts = dktest.Options{PortRequired: true, ReadyFunc: isReady}
	// Supported versions: https://www.mongodb.com/support-policy
	specs = []dktesting.ContainerSpec{
		{ImageName: "mongo:3.4", Options: opts},
		{ImageName: "mongo:3.6", Options: opts},
		{ImageName: "mongo:4.0", Options: opts},
		{ImageName: "mongo:4.2", Options: opts},
	}
)

func mongoConnectionString(host, port, dbName string) string {
	// there is connect option for excluding serverConnection algorithm
	// it's let avoid errors with mongo replica set connection in docker container
	return fmt.Sprintf("mongodb://%s:%s/%s?connect=direct", host, port, dbName)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {

	db, err := createDbConnection(ctx, c)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Println("is not ready: ", err)
		}
		return false
	}

	if err := db.Disconnect(ctx); err != nil {
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
			{Name: "withAuthRight", Func: testWithAuthRight},
			{Name: "withAuthWrong", Func: testWithAuthWrong},
			{Name: "lockWorks", Func: testLockWorks},
		} {

			func() {

				dbName := fmt.Sprintf("test%d", time.Now().UnixNano())

				ip, port, err := c.FirstPort()
				require.NoError(t, err, "Unable to get mapped port")

				addr := mongoConnectionString(ip, port, dbName)

				c := &Mongo{}
				d, err := c.Open(addr)
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()

				t.Run(testcase.Name, func(*testing.T) { testcase.Func(t, d, addr, dbName) })
			}()
		}
	})
}

func testBase(t *testing.T, d database.Driver, addr, dbName string) {

	dt.TestNilVersion(t, d)
	dt.TestLockAndUnlock(t, d)
	dt.TestRun(t, d, bytes.NewReader([]byte(`[{"insert":"hello","documents":[{"wild":"world"}]}]`)))
	dt.TestSetVersion(t, d)
	dt.TestDrop(t, d)
}

func testMigrate(t *testing.T, d database.Driver, addr, dbName string) {

	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "", d)
	require.NoError(t, err)
	dt.TestMigrate(t, m)
}

func testWithAuthRight(t *testing.T, d database.Driver, addr, dbName string) {

	createUserCMD := []byte(`[{"createUser":"deminem","pwd":"gogo","roles":[{"role":"readWrite","db":"testMigration"}]}]`)
	err := d.Run(bytes.NewReader(createUserCMD))
	require.NoError(t, err)

	u, err := url.Parse(addr)
	require.NoError(t, err)

	u.User = url.UserPassword("deminem", "gogo")

	mc := &Mongo{}
	ld, err := mc.Open(u.String())
	require.NoError(t, err)
	defer func() { require.NoError(t, ld.Close()) }()
}

func testWithAuthWrong(t *testing.T, d database.Driver, addr, dbName string) {

	u, err := url.Parse(addr)
	require.NoError(t, err)

	u.User = url.UserPassword("wrong", "auth")

	mc := &Mongo{}
	ld, err := mc.Open(u.String())
	require.NotNil(t, err)
	require.Nil(t, ld)
}

func testLockWorks(t *testing.T, d database.Driver, addr, dbName string) {

	dt.TestRun(t, d, bytes.NewReader([]byte(`[{"insert":"hello","documents":[{"wild":"world"}]}]`)))

	mc := d.(*Mongo)

	require.NoError(t, mc.Lock())
	require.NoError(t, mc.Unlock())

	require.NoError(t, mc.Lock())
	require.NoError(t, mc.Unlock())

	// disable locking, validate wer can lock twice
	mc.config.Locking.Enabled = false
	require.NoError(t, mc.Lock())
	require.NoError(t, mc.Lock())

	// re-enable locking,
	//try to hit a lock conflict
	mc.config.Locking.Enabled = true
	mc.config.Locking.Timeout = 1
	require.NoError(t, mc.Lock())
	require.EqualError(t, mc.Lock(), "can't acquire lock")
}

func TestTransaction(t *testing.T) {
	transactionSpecs := []dktesting.ContainerSpec{
		{ImageName: "mongo:4", Options: dktest.Options{PortRequired: true, ReadyFunc: isReady,
			Cmd: []string{"mongod", "--bind_ip_all", "--replSet", "rs0"}}},
	}
	dktesting.ParallelTest(t, transactionSpecs, func(t *testing.T, c dktest.ContainerInfo) {

		client, err := createDbConnection(context.TODO(), c)
		require.NoError(t, err)

		//rs.initiate()
		err = client.Database("admin").RunCommand(context.TODO(), bson.D{bson.E{Key: "replSetInitiate", Value: bson.D{}}}).Err()
		require.NoError(t, err)

		err = waitForReplicaInit(client)
		require.NoError(t, err)

		dbName := fmt.Sprintf("test%d", time.Now().UnixNano())

		d, err := WithInstance(client, &Config{
			DatabaseName: dbName,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, d.Close()) }()

		//We have to create collection
		//transactions don't support operations with creating new dbs, collections
		//Unique index need for checking transaction aborting
		insertCMD := []byte(`[
				{"create":"hello"},
				{"createIndexes": "hello",
					"indexes": [{
						"key": {
							"wild": 1
						},
						"name": "unique_wild",
						"unique": true,
						"background": true
					}]
			}]`)
		err = d.Run(bytes.NewReader(insertCMD))
		require.NoError(t, err)

		testcases := []struct {
			name            string
			cmds            []byte
			documentsCount  int64
			isErrorExpected bool
		}{
			{
				name: "success transaction",
				cmds: []byte(`[{"insert":"hello","documents":[
										{"wild":"world"},
										{"wild":"west"},
										{"wild":"natural"}
									 ]
								  }]`),
				documentsCount:  3,
				isErrorExpected: false,
			},
			{
				name: "failure transaction",
				//transaction have to be failure - duplicate unique key wild:west
				//none of the documents should be added
				cmds: []byte(`[{"insert":"hello","documents":[{"wild":"flower"}]},
									{"insert":"hello","documents":[
										{"wild":"cat"},
										{"wild":"west"}
									 ]
								  }]`),
				documentsCount:  3,
				isErrorExpected: true,
			},
		}
		for _, tcase := range testcases {
			t.Run(tcase.name, func(t *testing.T) {

				client, err := createDbConnection(context.Background(), c)
				require.NoError(t, err)

				d, err := WithInstance(client, &Config{
					DatabaseName:    dbName,
					TransactionMode: true,
				})
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()

				runErr := d.Run(bytes.NewReader(tcase.cmds))
				if tcase.isErrorExpected {
					require.NotNil(t, runErr)
				} else {
					require.NoError(t, runErr)
				}

				documentsCount, err := client.Database(dbName).Collection("hello").CountDocuments(context.TODO(), bson.M{})
				require.NoError(t, err)

				require.Equal(t, tcase.documentsCount, documentsCount)
			})
		}
	})
}

type isMaster struct {
	IsMaster bool `bson:"ismaster"`
}

func waitForReplicaInit(client *mongo.Client) error {
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	timeout, err := strconv.Atoi(os.Getenv("MIGRATE_TEST_MONGO_REPLICA_SET_INIT_TIMEOUT"))
	if err != nil {
		timeout = 30
	}
	timeoutTimer := time.NewTimer(time.Duration(timeout) * time.Second)
	defer timeoutTimer.Stop()
	for {
		select {
		case <-ticker.C:
			var status isMaster
			//Check that node is primary because
			//during replica set initialization, the first node first becomes a secondary and then becomes the primary
			//should consider that initialization is completed only after the node has become the primary
			result := client.Database("admin").RunCommand(context.TODO(), bson.D{bson.E{Key: "isMaster", Value: 1}})
			r, err := result.DecodeBytes()
			if err != nil {
				return err
			}
			err = bson.Unmarshal(r, &status)
			if err != nil {
				return err
			}
			if status.IsMaster {
				return nil
			}
		case <-timeoutTimer.C:
			return fmt.Errorf("replica init timeout")
		}
	}

}

func createDbConnection(ctx context.Context, c dktest.ContainerInfo) (*mongo.Client, error) {

	ip, port, err := c.FirstPort()
	if err != nil {
		return nil, fmt.Errorf("port error: %w", err)
	}

	// there is connect option for excluding serverConnection algorithm
	// it's let avoid errors with mongo replica set connection in docker container
	addr := fmt.Sprintf("mongodb://%s/?connect=direct", net.JoinHostPort(ip, port))
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(addr))
	if err != nil {
		return nil, fmt.Errorf("connect error '%s': %w", addr, err)
	}

	if err = client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("ping error: %w", err)
	}

	return client, nil
}
