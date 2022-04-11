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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/common"
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

func ValidateResponse(t *testing.T, testInfo ds.PKTestInfo, resp common.Response) {
	t.Helper()
	if len(testInfo.RespKVs)%2 != 0 {
		t.Fatalf("Expecting key value pairs. Items: %d\n ", len(testInfo.RespKVs))
	}

	for i := 0; i < len(testInfo.RespKVs); {
		key := string(testInfo.RespKVs[i].(string))
		value := RawBytes(testInfo.RespKVs[i+1])
		i += 2

		readVal, found := getColumnDataFromJson(key, resp)
		if !found {
			t.Fatalf("Key not found in the response. Key %s", key)
		}

		if string(value) != readVal {
			t.Fatalf("The read value for key %s does not match. Exptected: %s, Got: %s", key, value, readVal)
		}
	}
}

func getColumnDataFromJson(colName string, resp common.Response) (string, bool) {
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
	return val, ok
}

// func getColumnDataFromDB(t *testing.T, testInfo ds.PKTestInfo, col string, isString bool) (string, bool) {
// 	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/", config.SqlUser(), config.SqlPassword(),
// 		config.SqlServerIP(), config.SqlServerPort())
// 	db, err := sql.Open("mysql", connectionString)
// 	defer db.Close()
// 	if err != nil {
// 		t.Fatalf("failed to connect to db. %v", err)
// 	}

// 	command := "use " + testInfo.Db
// 	_, err = db.Exec(command)
// 	if err != nil {
// 		t.Fatalf("failed to run command. %s. Error: %v", command, err)
// 	}

//   command := "select "+col+" from "+testInfo.Table+ " where "
// 	for i := 0; i < len (*testInfo.PkReq.Filters); i++) {
// 	   command += (*testInfo.PkReq.Filters)[i].Column + " = " + *(*testInfo.PkReq.Filters)[i].Value
// 	}

// }

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
