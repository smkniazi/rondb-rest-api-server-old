/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	ds "hopsworks.ai/rdrs/internal/datastructs"
)

type RegisterTestHandler func(*gin.Engine)

func ProcessRequest(t *testing.T, router *gin.Engine, httpVerb string,
	url string, body string, expectedStatus int, expectedMsg string) common.Response {

	t.Helper()
	req, _ := http.NewRequest(httpVerb, url, strings.NewReader(body))
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	fmt.Printf("Response Body. %v\n", resp.Body)
	if resp.Code != expectedStatus || !strings.Contains(resp.Body.String(), expectedMsg) {
		if resp.Code != expectedStatus {
			t.Fatalf("Test failed. Expected: %d, Got: %d. Complete Response Body: %v ", expectedStatus, resp.Code, resp.Body)
		}
		if !strings.Contains(resp.Body.String(), expectedMsg) {
			t.Fatalf("Test failed. Response body does not contain %s. Body: %s", expectedMsg, resp.Body)
		}
	}

	r := common.Response{}
	json.Unmarshal(resp.Body.Bytes(), &r)
	// fmt.Printf("Response Body: %v\n", r)
	return r
}

func ValidateResArrayData(t *testing.T, testInfo ds.PKTestInfo, resp common.Response, isBinaryData bool) {
	t.Helper()

	for i := 0; i < len(testInfo.RespKVs); i++ {
		key := string(testInfo.RespKVs[i].(string))

		jsonVal, found := getColumnDataFromJson(t, key, testInfo, resp)
		if !found {
			t.Fatalf("Key not found in the response. Key %s", key)
		}

		dbVal, err := getColumnDataFromDB(t, testInfo, key, isBinaryData)
		if err != nil {
			t.Fatalf("%v", err)
		}

		if string(jsonVal) != string(dbVal) {
			t.Fatalf("The read value for key %s does not match. Got from REST Server: %s, Got from MYSQL Server: %s", key, jsonVal, dbVal)
		}
	}
}

func getColumnDataFromJson(t *testing.T, colName string, testInfo ds.PKTestInfo, resp common.Response) (string, bool) {
	t.Helper()

	if colName[0:1] != "\"" && colName[len(colName)-1:] != "\"" {
		colName = "\"" + colName + "\""
	}

	kvMap := make(map[string]string)

	var result map[string]json.RawMessage
	json.Unmarshal([]byte(resp.Message), &result)

	dataStr := string(result["Data"])
	dl := len(dataStr)
	core := dataStr[1 : dl-1] // remove the curly braces
	strs := strings.Split(core, ",")
	for _, kv := range strs {
		index := strings.Index(kv, ":")
		kvMap[kv[0:index]] = kv[index+1:]
	}

	val, ok := kvMap[colName]
	if !ok {
		return val, ok
	} else {
		var err error
		var unquote string
		unquote = val
		if val[0] == '"' {
			unquote, err = strconv.Unquote(val)
			if err != nil {
				t.Fatal(err)
			}
		}
		return unquote, ok
	}
}

func getColumnDataFromDB(t *testing.T, testInfo ds.PKTestInfo, col string, isBinary bool) (string, error) {
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/", config.SqlUser(), config.SqlPassword(),
		config.SqlServerIP(), config.SqlServerPort())
	db, err := sql.Open("mysql", connectionString)
	defer db.Close()
	if err != nil {
		t.Fatalf("failed to connect to db. %v", err)
	}

	command := "use " + testInfo.Db
	_, err = db.Exec(command)
	if err != nil {
		t.Fatalf("failed to run command. %s. Error: %v", command, err)
	}

	if isBinary {
		command = fmt.Sprintf("select replace(replace(to_base64(%s), '\\r',''), '\\n', '') from %s where ", col, testInfo.Table)
	} else {
		command = fmt.Sprintf("select %s from %s where ", col, testInfo.Table)
	}
	where := ""
	for i := 0; i < len(*testInfo.PkReq.Filters); i++ {
		if where != "" {
			where += " and "
		}
		if isBinary {
			where = fmt.Sprintf("%s %s = from_base64(%s)", where, *(*testInfo.PkReq.Filters)[i].Column, string(*(*testInfo.PkReq.Filters)[i].Value))
		} else {
			where = fmt.Sprintf("%s %s = %s", where, *(*testInfo.PkReq.Filters)[i].Column, string(*(*testInfo.PkReq.Filters)[i].Value))
		}
	}

	command = fmt.Sprintf(" %s %s\n ", command, where)
	rows, err := db.Query(command)
	if err != nil {
		return "", err
	}

	// Get column names
	//columns, err := rows.Columns()
	//if err != nil {
	//	return "", err
	//}

	values := make([]sql.RawBytes, 1)
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			return "", err
		}
		var value string
		for _, col := range values {

			// Here we can check if the value is nil (NULL value)
			if col == nil {
				value = "null"
			} else {
				value = string(col)
			}
			return value, nil
		}
	}

	return "", nil
}

func RawBytes(a interface{}) json.RawMessage {
	var value json.RawMessage
	if a == nil {
		return []byte("null")
	}

	switch a.(type) {
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint, float32, float64:
		value = []byte(fmt.Sprintf("%v", a))
	case string:
		value = []byte(fmt.Sprintf("\"%v\"", a))
	default:
		panic(fmt.Errorf("Unsupported data type. Type: %v", reflect.TypeOf(a)))
	}
	return value
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

func NewOperationID(t *testing.T, size int) *string {
	opID := RandString(size)
	return &opID
}

func NewPKReadReqBodyTBD(t *testing.T) ds.PKReadBody {
	t.Helper()
	param := ds.PKReadBody{
		Filters:     NewFilters(t, "filter_col_", 3),
		ReadColumns: NewReadColumns(t, "read_col_", 5),
		OperationID: NewOperationID(t, 64),
	}
	return param
}

// creates dummy filter columns of type string
func NewFilters(t *testing.T, prefix string, numFilters int) *[]ds.Filter {
	t.Helper()

	filters := make([]ds.Filter, numFilters)
	for i := 0; i < numFilters; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		val := col + "_data"
		v := RawBytes(val)
		filters[i] = ds.Filter{Column: &col, Value: &v}
	}
	return &filters
}

func NewFilter(t *testing.T, column *string, a interface{}) *[]ds.Filter {
	t.Helper()
	filter := make([]ds.Filter, 1)

	filter[0] = ds.Filter{Column: column}
	v := RawBytes(a)
	filter[0].Value = &v
	return &filter
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
		v := RawBytes(vals[i+1])
		filters[fidx] = ds.Filter{Column: &c, Value: &v}
		fidx++
		i += 2
	}
	return &filters
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func WithDBs(t *testing.T, dbs [][][]string, registerHandler RegisterTestHandler, fn func(router *gin.Engine)) {
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

	router, err := InitRouter(t)
	registerHandler(router)

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

func InitRouter(t *testing.T) (*gin.Engine, error) {
	t.Helper()
	//router := gin.Default()
	router := gin.New()
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

func PkTest(t *testing.T, tests map[string]ds.PKTestInfo, registerHandler RegisterTestHandler, isBinaryData bool) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			WithDBs(t, [][][]string{common.Database(testInfo.Db)}, registerHandler, func(router *gin.Engine) {
				url := NewPKReadURL(testInfo.Db, testInfo.Table)
				body, _ := json.MarshalIndent(testInfo.PkReq, "", "\t")
				res := ProcessRequest(t, router, ds.PK_HTTP_VERB, url,
					string(body), testInfo.HttpCode, testInfo.BodyContains)
				if res.OK {
					ValidateResArrayData(t, testInfo, res, isBinaryData)
				}
			})
		})
	}
}
