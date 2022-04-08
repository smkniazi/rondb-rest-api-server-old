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
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/common"
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

func ValidateResponse(t *testing.T, resp common.Response, kvs ...string) {
	t.Helper()
	if len(kvs)%2 != 0 {
		t.Fatalf("Expecting key value pairs. Items: %d\n ", len(kvs))
	}

	for i := 0; i < len(kvs); {
		key := kvs[i]
		value := kvs[i+1]
		i += 2

		readVal, found := getColumnData(key, resp)
		if !found {
			t.Fatalf("Key not found in the response. Key %s", key)
		}

		if value != readVal {
			t.Fatalf("The read value for key %s does not match. Exptected: %s, Got: %s", key, value, readVal)
		}
	}
}

func getColumnData(colName string, resp common.Response) (string, bool) {
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
