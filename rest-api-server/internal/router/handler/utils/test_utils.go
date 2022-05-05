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
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	ds "hopsworks.ai/rdrs/internal/datastructs"
	"hopsworks.ai/rdrs/version"
)

type RegisterTestHandler func(*gin.Engine)

func ProcessRequest(t *testing.T, router *gin.Engine, httpVerb string,
	url string, body string, expectedStatus int, expectedMsg string) (int, string) {

	t.Helper()
	req, _ := http.NewRequest(httpVerb, url, strings.NewReader(body))
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	var prettyJSON bytes.Buffer
	err := json.Indent(&prettyJSON, resp.Body.Bytes(), "", "\t")
	if err != nil {
		fmt.Printf("Error %v \n", err)
	}
	fmt.Printf("Response Body. %s\n", string(prettyJSON.Bytes()))
	if resp.Code != expectedStatus || !strings.Contains(resp.Body.String(), expectedMsg) {
		if resp.Code != expectedStatus {
			t.Fatalf("Test failed. Expected: %d, Got: %d. Complete Response Body: %v ", expectedStatus, resp.Code, resp.Body)
		}
		if !strings.Contains(resp.Body.String(), expectedMsg) {
			t.Fatalf("Test failed. Response body does not contain %s. Body: %s", expectedMsg, resp.Body)
		}
	}

	return resp.Code, string(resp.Body.Bytes())
}

func ValidateResArrayData(t *testing.T, testInfo ds.PKTestInfo, resp string, isBinaryData bool) {
	t.Helper()

	for i := 0; i < len(testInfo.RespKVs); i++ {
		key := string(testInfo.RespKVs[i].(string))

		jsonVal, found := getColumnDataFromJson(t, key, resp)
		if !found {
			t.Fatalf("Key not found in the response. Key %s", key)
		}

		dbVal, err := getColumnDataFromDB(t, testInfo.Db, testInfo.Table,
			testInfo.PkReq.Filters, key, isBinaryData)
		if err != nil {
			t.Fatalf("%v", err)
		}

		if string(jsonVal) != string(dbVal) {
			t.Fatalf("The read value for key %s does not match. Got from REST Server: %s, Got from MYSQL Server: %s", key, jsonVal, dbVal)
		}
	}
}

func getColumnDataFromJson(t *testing.T, colName string, resp string) (string, bool) {
	t.Helper()

	if colName[0:1] != "\"" && colName[len(colName)-1:] != "\"" {
		colName = "\"" + colName + "\""
	}

	kvMap := make(map[string]string)

	var result map[string]json.RawMessage
	json.Unmarshal([]byte(resp), &result)

	dataStr := string(result["data"])
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

func getColumnDataFromDB(t *testing.T, db string, table string, filters *[]ds.Filter, col string, isBinary bool) (string, error) {
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/", config.SqlUser(), config.SqlPassword(),
		config.SqlServerIP(), config.SqlServerPort())
	dbConn, err := sql.Open("mysql", connectionString)
	defer dbConn.Close()
	if err != nil {
		t.Fatalf("failed to connect to db. %v", err)
	}

	command := "use " + db
	_, err = dbConn.Exec(command)
	if err != nil {
		t.Fatalf("failed to run command. %s. Error: %v", command, err)
	}

	if isBinary {
		command = fmt.Sprintf("select replace(replace(to_base64(%s), '\\r',''), '\\n', '') from %s where ", col, table)
	} else {
		command = fmt.Sprintf("select %s from %s where ", col, table)
	}
	where := ""
	for i := 0; i < len(*filters); i++ {
		if where != "" {
			where += " and "
		}
		if isBinary {
			where = fmt.Sprintf("%s %s = from_base64(%s)", where, *(*filters)[i].Column, string(*(*filters)[i].Value))
		} else {
			where = fmt.Sprintf("%s %s = %s", where, *(*filters)[i].Column, string(*(*filters)[i].Value))
		}
	}

	command = fmt.Sprintf(" %s %s\n ", command, where)
	rows, err := dbConn.Query(command)
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

func NewBatchReadURL() string {
	return "/" + version.API_VERSION + "/" + ds.BATCH_OPERATION
}

func NewStatURL() string {
	return "/" + version.API_VERSION + "/" + ds.STAT_OPERATION
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

func WithDBs(t *testing.T, dbs [][][]string, registerHandlers []RegisterTestHandler,
	fn func(router *gin.Engine)) {
	t.Helper()

	rand.Seed(int64(time.Now().Nanosecond()))

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

	router, err := InitRouter(t, registerHandlers)

	if err != nil {
		t.Fatalf("%v", err)
	}
	defer shutDownRouter(t, router)

	fn(router)
	stats := dal.GetNativeBuffersStats()
	if stats.BuffersCount != stats.FreeBuffers {
		t.Fatalf("Number of free buffers do not match. Expecting: %d, Got: %d",
			stats.BuffersCount, stats.FreeBuffers)
	}
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

func InitRouter(t *testing.T, registerHandlers []RegisterTestHandler) (*gin.Engine, error) {
	t.Helper()
	//router := gin.Default()
	router := gin.New()
	err := dal.InitRonDBConnection(config.ConnectionString(), true)
	if err != nil {
		return nil, err
	}

	for _, handler := range registerHandlers {
		handler(router)
	}
	if !dal.BuffersInitialized() {
		dal.InitializeBuffers()
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

func PkTest(t *testing.T, tests map[string]ds.PKTestInfo, isBinaryData bool, registerHandler ...RegisterTestHandler) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			WithDBs(t, [][][]string{common.Database(testInfo.Db)}, registerHandler, func(router *gin.Engine) {
				url := NewPKReadURL(testInfo.Db, testInfo.Table)
				body, _ := json.MarshalIndent(testInfo.PkReq, "", "\t")
				httpCode, res := ProcessRequest(t, router, ds.PK_HTTP_VERB, url,
					string(body), testInfo.HttpCode, testInfo.BodyContains)
				if httpCode == http.StatusOK {
					ValidateResArrayData(t, testInfo, res, isBinaryData)
				}
			})
		})
	}
}

func BatchTest(t *testing.T, tests map[string]ds.BatchOperationTestInfo, isBinaryData bool,
	registerHandlers ...RegisterTestHandler) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {

			// all databases used in this test
			dbsMap := map[string]bool{}
			dbNames := make([]string, 0, len(dbsMap))
			for _, op := range testInfo.Operations {
				if _, ok := dbsMap[op.DB]; !ok {
					dbsMap[op.DB] = true
				}
			}
			dbs := [][][]string{}
			for k := range dbsMap {
				dbNames = append(dbNames, k)
				dbs = append(dbs, common.Database(k))
			}

			//batch operation
			subOps := []ds.BatchSubOperation{}
			for _, op := range testInfo.Operations {
				subOps = append(subOps, op.SubOperation)
			}
			batch := ds.BatchOperation{Operations: &subOps}

			WithDBs(t, dbs, registerHandlers, func(router *gin.Engine) {
				url := NewBatchReadURL()
				body, _ := json.MarshalIndent(batch, "", "\t")
				httpCode, res := ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url,
					string(body), testInfo.HttpCode, "")
				if httpCode == http.StatusOK {
					validateBatchResponse(t, testInfo, res, isBinaryData)
				}
			})
		})
	}
}

func validateBatchResponse(t *testing.T, testInfo ds.BatchOperationTestInfo, resp string, isBinaryData bool) {
	t.Helper()
	validateBatchResponseOpIdsNCode(t, testInfo, resp)
	validateBatchResponseMsg(t, testInfo, resp)
	validateBatchResponseValues(t, testInfo, resp, isBinaryData)

}

func validateBatchResponseOpIdsNCode(t *testing.T, testInfo ds.BatchOperationTestInfo, resp string) {
	var res []struct {
		Code int
		Body struct {
			OperationId string
		}
	}
	json.Unmarshal([]byte(resp), &res)

	if len(res) != len(testInfo.Operations) {
		t.Fatal("Wrong number of operation responses received")
	}

	for i := 0; i < len(res); i++ {
		expectingId := testInfo.Operations[i].SubOperation.Body.OperationID
		if expectingId != nil {
			idGot := res[i].Body.OperationId
			if *expectingId != idGot {
				t.Fatalf("Operation ID does not match. Expecting: %s, Got: %s", *expectingId, idGot)
			}
		}

		expectingCode := testInfo.Operations[i].HttpCode
		codeGot := res[i].Code
		if expectingCode != codeGot {
			t.Fatalf("Return code does not match. Expecting: %d, Got: %d", expectingCode, codeGot)
		}
	}
}

func validateBatchResponseMsg(t *testing.T, testInfo ds.BatchOperationTestInfo, resp string) {

	var res []json.RawMessage
	json.Unmarshal([]byte(resp), &res)

	for i := 0; i < len(testInfo.Operations); i++ {
		if !strings.Contains(string(res[i]), testInfo.Operations[i].BodyContains) {
			t.Fatalf("Test failed. Response body does not contain %s. Body: %s",
				testInfo.Operations[i].BodyContains, string(res[i]))
		}
	}
}

func validateBatchResponseValues(t *testing.T, testInfo ds.BatchOperationTestInfo, resp string, isBinaryData bool) {
	var res []struct {
		Code int
		Body json.RawMessage
	}
	json.Unmarshal([]byte(resp), &res)

	for o := 0; o < len(testInfo.Operations); o++ {
		if res[o].Code != http.StatusOK {
			continue // data is null if the status is not OK
		}

		operation := testInfo.Operations[o]
		for i := 0; i < len(operation.RespKVs); i++ {
			key := string(operation.RespKVs[i].(string))
			bodyGot := string(res[o].Body)
			jsonVal, found := getColumnDataFromJson(t, key, bodyGot)
			if !found {
				t.Fatalf("Key not found in the response. Key %s", key)
			}
			dbVal, err := getColumnDataFromDB(t, operation.DB, operation.Table,
				operation.SubOperation.Body.Filters, key, isBinaryData)
			if err != nil {
				t.Fatalf("%v", err)
			}

			if string(jsonVal) != string(dbVal) {

				t.Fatalf("The read value for key %s does not match. Got from REST Server: %s, Got from MYSQL Server: %s", key, jsonVal, dbVal)
			}
		}
	}
}

func Encode(data string, binary bool, colWidth int, padding bool) string {

	if binary {

		newData := []byte(data)
		if padding {
			length := colWidth
			if length < len(data) {
				length = len(data)
			}

			newData = make([]byte, length)
			for i := 0; i < length; i++ {
				newData[i] = 0x00
			}
			for i := 0; i < len(data); i++ {
				newData[i] = data[i]
			}
		}
		return base64.StdEncoding.EncodeToString(newData)
	} else {
		return data
	}
}
