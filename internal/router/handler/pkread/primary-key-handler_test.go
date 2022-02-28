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

package pkread

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// Simple test with all parameters correctly supplied
func TestPKReadTest(t *testing.T) {
	router := gin.Default()
	group := router.Group(DB_OPS_EP_GROUP)
	group.GET(DB_OPERATION, PkReadHandler)

	// create params
	filtersParam := getFiltersParam(t, "filter_col_", 3)
	readColumnsParam := getReadColumnsParam(t, "read_col_", 5)
	operationID := getOperationID(t, 64)

	reqStr := fmt.Sprintf("%s%s?%s&%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filtersParam, readColumnsParam, operationID)

	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusOK, "")

	// Omit the optional operation ID param
	reqStr = fmt.Sprintf("%s%s?%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filtersParam, readColumnsParam)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusOK, "")

	// Omit the optional read-columns, and operation ID params
	reqStr = fmt.Sprintf("%s%s?%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filtersParam)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusOK, "")
}

func TestPKReadOmitRequired(t *testing.T) {
	router := gin.Default()
	group := router.Group(DB_OPS_EP_GROUP)
	group.GET(DB_OPERATION, PkReadHandler)

	// Test. Omitting filter should result in 400 error
	readColumnsParam := getReadColumnsParam(t, "read_col_", 5)

	reqStr := fmt.Sprintf("%s%s?%s", DB_OPS_EP_GROUP, DB_OPERATION,
		readColumnsParam)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusBadRequest, "Error:Field validation for 'Filters'")

	// Test. unset filter values should result in 400 error
	col := "col"
	filterParam := getFilterParam(t, &col, nil)
	readColumnsParam = getReadColumnsParam(t, "read_col_", 5)
	reqStr = fmt.Sprintf("%s%s?%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filterParam, readColumnsParam)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusBadRequest, "Error:Field validation for 'Value'")

	val := "val"
	filterParam = getFilterParam(t, nil, &val)
	reqStr = fmt.Sprintf("%s%s?%s", DB_OPS_EP_GROUP, DB_OPERATION, filterParam)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusBadRequest, "Error:Field validation for 'Column'")
}

func TestPKReadLargeColumns(t *testing.T) {
	router := gin.Default()
	group := router.Group(DB_OPS_EP_GROUP)
	group.GET(DB_OPERATION, PkReadHandler)

	// Test. Large filter column names.
	col := randStringRunes(65)
	val := "val"
	filterParam := getFilterParam(t, &col, &val)
	readColumnsParam := getReadColumnsParam(t, "read_col_", 5)
	reqStr := fmt.Sprintf("%s%s?%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filterParam, readColumnsParam)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusBadRequest, "Field validation for 'Column' failed on the 'max' tag")

	// Test. Large read column names.
	filterParam = getFiltersParam(t, "filter_col", 1)
	readColumnParam := getReadColumnParam(t, randStringRunes(65))
	reqStr = fmt.Sprintf("%s%s?%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filterParam, readColumnParam)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusBadRequest, "Field length validation failed")

	// Test. Large db and table names
	filterParam = getFiltersParam(t, "filter_col", 2)
	readColumnsParam = getReadColumnsParam(t, "read_col_", 5)
	reqStr = fmt.Sprintf("%s%s?%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filterParam, readColumnsParam)
	reqStr1 := setPathParms(reqStr, randStringRunes(65), "table")
	processRequest(t, router, reqStr1, http.StatusBadRequest, "Field validation for 'DB' failed on the 'max' tag")
	reqStr2 := setPathParms(reqStr, "db", randStringRunes(65))
	processRequest(t, router, reqStr2, http.StatusBadRequest, "Field validation for 'Table' failed on the 'max' tag")
	reqStr3 := setPathParms(reqStr, "db", "")
	processRequest(t, router, reqStr3, http.StatusBadRequest, "Field validation for 'Table' failed on the 'min' tag")
}

func TestPKInvalidIdentifier(t *testing.T) {
	router := gin.Default()
	group := router.Group(DB_OPS_EP_GROUP)
	group.GET(DB_OPERATION, PkReadHandler)

	// Test. invalid filter
	col := "col@"
	val := "val"
	filterParam := getFilterParam(t, &col, &val)
	readColumnsParam := getReadColumnsParam(t, "read_col_", 1)
	reqStr := fmt.Sprintf("%s%s?%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filterParam, readColumnsParam)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusBadRequest, "Field validation failed. Invalid character '@'")

	// Test. invalid read col
	col = "col"
	val = "val"
	filterParam = getFilterParam(t, &col, &val)
	readColumnParam := getReadColumnParam(t, "col#")
	reqStr = fmt.Sprintf("%s%s?%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filterParam, readColumnParam)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusBadRequest, "Field validation failed. Invalid character '#'")

	// Test. invalid db name
	col = "col"
	val = "val"
	filterParam = getFilterParam(t, &col, &val)
	readColumnParam = getReadColumnParam(t, "col")
	reqStr = fmt.Sprintf("%s%s?%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filterParam, readColumnParam)
	reqStr1 := setPathParms(reqStr, "db*", "table")
	processRequest(t, router, reqStr1, http.StatusBadRequest, "Field validation failed. Invalid character '*'")
	reqStr2 := setPathParms(reqStr, "db", "table!")
	processRequest(t, router, reqStr2, http.StatusBadRequest, "Field validation failed. Invalid character '!'")
}

func TestPKUniqueParams(t *testing.T) {
	router := gin.Default()
	group := router.Group(DB_OPS_EP_GROUP)
	group.GET(DB_OPERATION, PkReadHandler)

	// Test. unique read columns
	col := "col"
	val := "val"
	filterParam := getFilterParam(t, &col, &val)
	readColumnsParam1 := getReadColumnParam(t, "col1")
	readColumnsParam2 := getReadColumnParam(t, "col1")
	reqStr := fmt.Sprintf("%s%s?%s&%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filterParam, readColumnsParam1, readColumnsParam2)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusBadRequest,
		"Field validation for 'ReadColumns' failed on the 'unique' tag")

	// Test. unique filter columns
	col = "col"
	val = "val"
	filterParam1 := getFilterParam(t, &col, &val)
	filterParam2 := getFilterParam(t, &col, &val)
	readColumnsParam := getReadColumnParam(t, "col1")
	reqStr = fmt.Sprintf("%s%s?%s&%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filterParam1, filterParam2, readColumnsParam)
	reqStr = setPathParms(reqStr, "db", "table")
	processRequest(t, router, reqStr, http.StatusBadRequest,
		"Field validation for filter failed on the 'unique' tag")

	// Test that filter and read columns do not contain overlapping columns
	col = "col"
	val = "val"
	filterParam = getFilterParam(t, &col, &val)
	readColumnsParam = getReadColumnParam(t, col)
	reqStr = fmt.Sprintf("%s%s?%s&%s", DB_OPS_EP_GROUP, DB_OPERATION,
		filterParam, readColumnsParam)
	reqStr = setPathParms(reqStr, "db", "table")

	processRequest(t, router, reqStr, http.StatusBadRequest,
		fmt.Sprintf("Field validation for read columns faild. '%s' already included in filter", col))
}

func processRequest(t *testing.T, router *gin.Engine, reqStr string, expectedStatus int, expectedMsg string) {
	req, _ := http.NewRequest("GET", reqStr, nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != expectedStatus || !strings.Contains(resp.Body.String(), expectedMsg) {
		if resp.Code != expectedStatus {
			t.Errorf("Test failed. Expected: %d, Got: %d. ", expectedStatus, resp.Code)
		}
		if !strings.Contains(resp.Body.String(), expectedMsg) {
			t.Errorf("Test failed. Response body does not contain %s. Body: %s", expectedMsg, resp.Body)
		}
	}
}

func getOperationID(t *testing.T, size int) string {
	return fmt.Sprintf("%s=%s", OPERATION_ID_PARAM_NAME, url.QueryEscape(randStringRunes(size)))
}

func getFiltersParam(t *testing.T, prefix string, numFilters int) string {
	t.Helper()

	filters := make([]Filter, numFilters)
	for i := 0; i < numFilters; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		val := "value_" + fmt.Sprintf("%d", i)
		filters[i] = Filter{Column: &col, Value: &val}
	}

	// var filtersStr string
	var filtersBuff bytes.Buffer
	for i, filter := range filters {
		filterStr, _ := json.Marshal(filter)
		if i != 0 {
			filtersBuff.WriteString("&")
		}
		filtersBuff.WriteString(fmt.Sprintf("%s=%s", FILTER_PARAM_NAME, url.QueryEscape(string(filterStr))))
	}
	return filtersBuff.String()
}

func getFilterParam(t *testing.T, column *string, value *string) string {
	t.Helper()
	filter := Filter{Column: column, Value: value}
	filterStr, err := json.Marshal(filter)
	if err != nil {
		t.Errorf("Marshling the filter param failed. Error: %v", err)
	}

	return fmt.Sprintf("%s=%s", FILTER_PARAM_NAME, url.QueryEscape(string(filterStr)))
}

func getReadColumnsParam(t *testing.T, prefix string, numReadColumns int) string {
	t.Helper()

	readColumns := make([]string, numReadColumns)
	var readColumnsBuff bytes.Buffer
	for i := 0; i < numReadColumns; i++ {
		readColumns[i] = prefix + fmt.Sprintf("%d", i)
		if i != 0 {
			readColumnsBuff.WriteString("&")
		}
		readColumnsBuff.WriteString(fmt.Sprintf("%s=%s", READ_COL_PARAM_NAME, url.QueryEscape(readColumns[i])))
	}
	return readColumnsBuff.String()
}

func getReadColumnParam(t *testing.T, col string) string {
	t.Helper()
	return fmt.Sprintf("%s=%s", READ_COL_PARAM_NAME, url.QueryEscape(col))
}

func setPathParms(reqStr string, db string, table string) string {
	reqStr = strings.Replace(reqStr, ":"+DB_PP, db, 1)
	reqStr = strings.Replace(reqStr, ":"+TABLE_PP, table, 1)
	return reqStr
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
