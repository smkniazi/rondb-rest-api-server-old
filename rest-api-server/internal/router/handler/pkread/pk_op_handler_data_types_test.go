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
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/common"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
)

type TestInfo struct {
	pkReq        PKReadBody
	table        string
	db           string
	httpCode     int
	bodyContains string
	respKVs      []string
}

// INT TESTS
// Test signed and unsigned int data type
func TestDataTypesInt(t *testing.T) {

	testTable := "int_table"
	testDb := "DB004"
	tests := map[string]TestInfo{
		// "xxxxxx": {
		// pkReq:        PKReadBody{},
		// table: testTable,
		// db: testDb,
		// httpCode:     http.StatusOK,
		// bodyContains: "",
		// respKVs:      []string{},
		// },
		"simple": {
			pkReq: PKReadBody{Filters: NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "0", "col1", "0"},
		},
		"maxValues": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2147483647", "id1", "4294967295"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "2147483647", "col1", "4294967295"},
		},
		"minValues": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-2147483648", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "-2147483648", "col1", "0"},
		},

		"assignNegativeValToUnsignedCol": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "-1"), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},

		"assigningBiggerVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2147483648", "id1", "4294967295"), //bigger than the range
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},

		"assigningSmallerVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-2147483649", "id1", "0"), //smaller than range
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},

		"nullVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "null", "col1", "null"},
		},
	}

	test(t, tests)
}

func TestDataTypesBigInt(t *testing.T) {

	testTable := "bigint_table"
	testDb := "DB005"

	tests := map[string]TestInfo{
		"simple": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "0", "col1", "0"},
		},
		"maxValues": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "9223372036854775807", "id1", "18446744073709551615"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "9223372036854775807", "col1", "18446744073709551615"},
		},

		"minValues": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-9223372036854775808", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "-9223372036854775808", "col1", "0"},
		},
		"assignNegativeValToUnsignedCol": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "-1"), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},
		"assigningBiggerVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "9223372036854775807", "id1", "18446744073709551616"), //18446744073709551615+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},
		"assigningSmallerVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-9223372036854775809", "id1", "0"), //-9223372036854775808-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},
		"nullVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func TestDataTypesTinyInt(t *testing.T) {

	testTable := "tinyint_table"
	testDb := "DB006"
	tests := map[string]TestInfo{
		"simple": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "0", "col1", "0"},
		},
		"maxValues": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "127", "id1", "255"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "127", "col1", "255"},
		},

		"minValues": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-128", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "-128", "col1", "0"},
		},
		"assignNegativeValToUnsignedCol": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "-1"), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},
		"assigningBiggerVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "127", "id1", "256"), //255+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},
		"assigningSmallerVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-129", "id1", "0"), //-128-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},
		"nullVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func TestDataTypesSmallInt(t *testing.T) {

	testTable := "smallint_table"
	testDb := "DB007"
	tests := map[string]TestInfo{
		"simple": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "0", "col1", "0"},
		},
		"maxValues": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "32767", "id1", "65535"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "32767", "col1", "65535"},
		},

		"minValues": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-32768", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "-32768", "col1", "0"},
		},
		"assignNegativeValToUnsignedCol": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "-1"), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},
		"assigningBiggerVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "32768", "id1", "256"), //32767+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},
		"assigningSmallerVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-32769", "id1", "0"), //-32768-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},
		"nullVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func test(t *testing.T, tests map[string]TestInfo) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			withDBs(t, [][][]string{common.Database(testInfo.db)}, func(router *gin.Engine) {
				url := NewPKReadURL(testInfo.db, testInfo.table)
				body, _ := json.MarshalIndent(testInfo.pkReq, "", "\t")
				res := tu.ProcessRequest(t, router, HTTP_VERB, url,
					string(body), testInfo.httpCode, testInfo.bodyContains)
				fmt.Printf("Response %v\n", res)
				if len(testInfo.respKVs) > 0 {
					tu.ValidateResponse(t, res, testInfo.respKVs...)
				}
			})
		})
	}
}
