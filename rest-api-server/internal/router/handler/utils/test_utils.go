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
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	ds "hopsworks.ai/rdrs/internal/datastructs"
)

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

func ValidateRes(t *testing.T, testInfo ds.PKTestInfo, resp common.Response) {
	t.Helper()
	if len(testInfo.RespKVs)%2 != 0 {
		t.Fatalf("Expecting key value pairs. Items: %d\n ", len(testInfo.RespKVs))
	}

	for i := 0; i < len(testInfo.RespKVs); {
		key := string(testInfo.RespKVs[i].(string))
		value := RawBytes(testInfo.RespKVs[i+1])
		i += 2

		readVal, found := getColumnDataFromJson(t, key, testInfo, resp)
		if !found {
			t.Fatalf("Key not found in the response. Key %s", key)
		}

		if string(value) != readVal {
			t.Fatalf("The read value for key %s does not match. Exptected: %s, Got: %s", key, value, readVal)
		}
	}
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
			t.Fatalf("The read value for key %s does not match. Exptected: %s, Got: %s", key, jsonVal, dbVal)
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
