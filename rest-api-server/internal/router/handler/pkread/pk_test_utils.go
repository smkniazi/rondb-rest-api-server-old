package pkread

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
)

func NewPKReadReqBody(t *testing.T) PKReadBody {
	t.Helper()
	param := PKReadBody{
		Filters:     NewFilters(t, "filter_col_", 3),
		ReadColumns: NewReadColumns(t, "read_col_", 5),
		OperationID: NewOperationID(t, 64),
	}
	return param
}

func NewOperationID(t *testing.T, size int) *string {
	opID := RandString(size)
	return &opID
}

func NewFilters(t *testing.T, prefix string, numFilters int) *[]Filter {
	t.Helper()

	filters := make([]Filter, numFilters)
	for i := 0; i < numFilters; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		val := col + "_data"
		filters[i] = Filter{Column: &col, Value: &val}
	}
	return &filters
}

func NewFilter(t *testing.T, column *string, value *string) *[]Filter {
	t.Helper()
	filter := make([]Filter, 1)
	filter[0] = Filter{Column: column, Value: value}
	return &filter
}

func NewFiltersKVs(t *testing.T, vals ...string) *[]Filter {
	t.Helper()
	if len(vals)%2 != 0 {
		t.Fatal("Expecting key value pairs")
	}

	filters := make([]Filter, len(vals)/2)
	fidx := 0
	for i := 0; i < len(vals); {
		filters[fidx] = Filter{Column: &vals[i], Value: &vals[i+1]}
		fidx++
		i += 2
	}
	return &filters
}

func NewReadColumns(t *testing.T, prefix string, numReadColumns int) *[]string {
	t.Helper()
	readColumns := make([]string, numReadColumns)
	for i := 0; i < numReadColumns; i++ {
		readColumns[i] = prefix + fmt.Sprintf("%d", i)
	}
	return &readColumns
}

func NewReadColumn(t *testing.T, col string) *[]string {
	t.Helper()
	readColumns := make([]string, 1)
	readColumns[0] = col
	return &readColumns
}

func NewPKReadURL(db string, table string) string {
	url := fmt.Sprintf("%s%s", DB_OPS_EP_GROUP, DB_OPERATION)
	url = strings.Replace(url, ":"+DB_PP, db, 1)
	url = strings.Replace(url, ":"+TABLE_PP, table, 1)
	return url
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func withDBs(t *testing.T, dbs [][][]string, fn func(router *gin.Engine)) {
	t.Helper()

	//user:password@tcp(IP:Port)/
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/", config.SqlUser(), config.SqlPassword(),
		config.SqlServerIP(), config.SqlServerPort())
	dbConnection, err := sql.Open("mysql", connectionString)
	if err != nil {
		t.Fatalf("failed to connect to db. %v", err)
	}

	for _, db := range dbs {
		if len(db) != 2 {
			t.Fatal("expecting the setup array to contain two sub arrays where the first " +
				"sub array contains commands to setup the DBs, " +
				"and the second sub array contains commands to clean up the DBs")
		}
		defer runSQLQueries(t, dbConnection, db[1])
		runSQLQueries(t, dbConnection, db[0])
	}

	router, err := initRouter(t)
	if err != nil {
		t.Fatalf("%v", err)
	}

	fn(router)
}

func runSQLQueries(t *testing.T, db *sql.DB, setup []string) {
	t.Helper()
	for _, command := range setup {
		_, err := db.Exec(command)
		if err != nil {
			t.Fatalf("failed to run command. %s. Error: %v", command, err)
		}
	}
}

func initRouter(t *testing.T) (*gin.Engine, error) {
	t.Helper()
	//router := gin.Default()
	router := gin.New()

	group := router.Group(DB_OPS_EP_GROUP)
	group.POST(DB_OPERATION, PkReadHandler)
	err := dal.InitRonDBConnection(config.ConnectionString())
	if err != nil {
		return nil, err
	}
	return router, nil
}
