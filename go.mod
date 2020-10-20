module github.com/golang-migrate/migrate/v4

go 1.14

replace github.com/Sirupsen/logrus => github.com/sirupsen/logrus v1.7.0

require (
	cloud.google.com/go/spanner v1.11.0
	cloud.google.com/go/storage v1.12.0
	github.com/ClickHouse/clickhouse-go v1.4.3
	github.com/aws/aws-sdk-go v1.35.10
	github.com/cenkalti/backoff/v4 v4.1.0
	github.com/cockroachdb/cockroach-go v2.0.1+incompatible
	github.com/denisenkom/go-mssqldb v0.0.0-20200910202707-1e08a3fab204
	github.com/dhui/dktest v0.3.2
	github.com/docker/docker v1.4.2-0.20200213202729-31a86c4ab209
	github.com/fsouza/fake-gcs-server v1.21.1
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gobuffalo/here v0.6.2
	github.com/gocql/gocql v0.0.0-20200926162733-393f0c961220
	github.com/google/go-github v17.0.0+incompatible
	github.com/hashicorp/go-multierror v1.1.0
	github.com/ktrysmt/go-bitbucket v0.6.4
	github.com/lib/pq v1.8.0
	github.com/markbates/pkger v0.17.1
	github.com/mattn/go-sqlite3 v1.14.4
	github.com/mutecomm/go-sqlcipher/v4 v4.4.0
	github.com/nakagami/firebirdsql v0.9.0
	github.com/neo4j/neo4j-go-driver v1.8.3
	github.com/snowflakedb/gosnowflake v1.3.9
	github.com/stretchr/testify v1.6.1
	github.com/xanzy/go-gitlab v0.38.2
	go.mongodb.org/mongo-driver v1.4.2
	go.uber.org/atomic v1.7.0
	golang.org/x/tools v0.0.0-20201019175715-b894a3290fff
	google.golang.org/api v0.33.0
	google.golang.org/genproto v0.0.0-20201019141844-1ed22bb0c154
	modernc.org/ql v1.1.0
)
