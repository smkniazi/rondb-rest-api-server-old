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

func TestDataTypesMediumInt(t *testing.T) {

	testTable := "mediumint_table"
	testDb := "DB008"
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
				Filters:     NewFiltersKVs(t, "id0", "8388607", "id1", "16777215"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "8388607", "col1", "16777215"},
		},
		"minValues": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-8388608", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "-8388608", "col1", "0"},
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
				Filters:     NewFiltersKVs(t, "id0", "8388608", "id1", "256"), //8388607+1
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
				Filters:     NewFiltersKVs(t, "id0", "-8388609", "id1", "0"), //-8388608-1
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

func TestDataTypesFloat(t *testing.T) {

	// testTable := "float_table"
	testDb := "DB009"
	tests := map[string]TestInfo{
		"floatPK": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        "float_table2",
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_017(),
		},
		"simple": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        "float_table1",
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "0", "col1", "0"},
		},
		"simple2": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        "float_table1",
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "-123.123", "col1", "123.123"},
		},
		"nullVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        "float_table1",
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func TestDataTypesDouble(t *testing.T) {

	// testTable := "float_table"
	testDb := "DB010"
	tests := map[string]TestInfo{
		"floatPK": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        "double_table2",
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_017(),
		},
		"simple": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        "double_table1",
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "0", "col1", "0"},
		},
		"simple2": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        "double_table1",
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "-123.123", "col1", "123.123"},
		},
		"nullVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        "double_table1",
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func TestDataTypesDecimal(t *testing.T) {

	testTable := "decimal_table"
	testDb := "DB011"
	tests := map[string]TestInfo{

		"simple": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-12345.12345", "id1", "12345.12345"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "-12345.12345", "col1", "12345.12345"},
		},

		"nullVals": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-67890.12345", "id1", "67890.12345"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "null", "col1", "null"},
		},

		"assignNegativeValToUnsignedCol": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-12345.12345", "id1", "-12345.12345"),
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
				Filters:     NewFiltersKVs(t, "id0", "-12345.12345", "id1", "123456789.12345"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusBadRequest,
			bodyContains: common.ERROR_015(),
			respKVs:      []string{},
		},
	}
	test(t, tests)
}

func TestDataTypesChar(t *testing.T) {

	testTable := "char_table"
	testDb := "DB012"
	tests := map[string]TestInfo{

		"notfound": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-1"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusNotFound,
			bodyContains: "",
			respKVs:      []string{},
		},

		"simple1": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "\"这是一个测验。 我不知道怎么读中文。\""},
		},

		"simple2": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "\"f\\u0000f\""},
		},

		"simple3": { // new line char in string
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "3"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "\"a\\nb\""},
		},

		"simple4": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "4"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "\"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïð\""},
		},

		"simple5": { //unicode pk
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "这是一个测验"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "\"12345\""},
		},

		"nulltest": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "5"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "null"},
		},

		"escapedChars": {
			pkReq: PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "6"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", `"\"\\\bf\n\r\t$%_?"`}, // in mysql \f is replaced by f
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
