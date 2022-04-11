package pkread

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	ds "hopsworks.ai/rdrs/internal/datastructs"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
)

func NewPKReadReqBody(t *testing.T) ds.PKReadBody {
	t.Helper()
	param := ds.PKReadBody{
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

// creates dummy filter columns of type string
func NewFilters(t *testing.T, prefix string, numFilters int) *[]ds.Filter {
	t.Helper()

	filters := make([]ds.Filter, numFilters)
	for i := 0; i < numFilters; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		val := col + "_data"
		v := tu.RawBytes(val)
		filters[i] = ds.Filter{Column: &col, Value: &v}
	}
	return &filters
}

func NewFilter(t *testing.T, column *string, a interface{}) *[]ds.Filter {
	t.Helper()
	filter := make([]ds.Filter, 1)

	filter[0] = ds.Filter{Column: column}
	v := tu.RawBytes(a)
	filter[0].Value = &v
	return &filter
}

func RawBytes(a interface{}) json.RawMessage {
	var value json.RawMessage
	if a == nil {
		return []byte("null")
	}

	switch a.(type) {
	case int8:
	case uint8:
	case int16:
	case uint16:
	case int32:
	case uint32:
	case int64:
	case uint64:
	case int:
	case uint:
	case *int8:
	case *uint8:
	case *int16:
	case *uint16:
	case *int32:
	case *uint32:
	case *int64:
	case *uint64:
	case *int:
	case *uint:
	case float32:
	case float64:
		value = []byte(fmt.Sprintf("%v", a))
	case string:
	case *string:
	case *float32:
	case *float64:
	default:
		panic(fmt.Errorf("Unsupported data type. Type: %v", reflect.TypeOf(a)))
	}
	return value
}

func NewFiltersKVs(t *testing.T, vals ...interface{}) *[]ds.Filter {
	t.Helper()
	if len(vals)%2 != 0 {
		t.Fatal("Expecting key value pairs")
	}

	filters := make([]ds.Filter, len(vals)/2)
	fidx := 0
	for i := 0; i < len(vals); {
		c := fmt.Sprintf("%v", vals[i])
		v := tu.RawBytes(vals[i+1])
		filters[fidx] = ds.Filter{Column: &c, Value: &v}
		fidx++
		i += 2
	}
	return &filters
}

func NewReadColumns(t *testing.T, prefix string, numReadColumns int) *[]ds.ReadColumn {
	t.Helper()
	readColumns := make([]ds.ReadColumn, numReadColumns)
	for i := 0; i < numReadColumns; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		drt := ds.DRT_DEFAULT
		readColumns[i].Column = &col
		readColumns[i].DataReturnType = &drt
	}
	return &readColumns
}

func NewReadColumn(t *testing.T, col string) *[]ds.ReadColumn {
	t.Helper()
	readColumns := make([]ds.ReadColumn, 1)
	drt := string(ds.DRT_DEFAULT)
	readColumns[0].Column = &col
	readColumns[0].DataReturnType = &drt
	return &readColumns
}

func NewPKReadURL(db string, table string) string {
	url := fmt.Sprintf("%s%s", ds.DB_OPS_EP_GROUP, ds.PK_DB_OPERATION)
	url = strings.Replace(url, ":"+ds.DB_PP, db, 1)
	url = strings.Replace(url, ":"+ds.TABLE_PP, table, 1)
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
	defer dbConnection.Close()
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
	defer shutDownRouter(t, router)

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

	group := router.Group(ds.DB_OPS_EP_GROUP)
	group.POST(ds.PK_DB_OPERATION, PkReadHandler)
	err := dal.InitRonDBConnection(config.ConnectionString(), true)
	if err != nil {
		return nil, err
	}
	return router, nil
}

func shutDownRouter(t *testing.T, router *gin.Engine) error {
	t.Helper()
	err := dal.ShutdownConnection()
	if err != nil {
		return err
	}
	return nil
}
