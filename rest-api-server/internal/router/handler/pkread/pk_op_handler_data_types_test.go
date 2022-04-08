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
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	_ "github.com/ianlancetaylor/cgosymbolizer"
	"hopsworks.ai/rdrs/internal/common"
	ds "hopsworks.ai/rdrs/internal/datastructs"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
)

type PKTestInfo struct {
	pkReq        ds.PKReadBody
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
	tests := map[string]PKTestInfo{
		"simple1": {
			pkReq: ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "0", "col1", "0"},
		},

		"simple2": { //with out operation ID
			pkReq: ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "0", "col1", "0"},
		},

		"simple3": { //without read columns.
			pkReq:        ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", "0", "id1", "0")},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "0", "col1", "0"},
		},

		"simple4": { //Table with only primary keys
			pkReq: ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", "0", "id1", "0"),
				OperationID: NewOperationID(t, 64),
			},
			table:        "int_table1",
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{},
		},

		"maxValues": {
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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

	tests := map[string]PKTestInfo{

		"simple": {
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
	tests := map[string]PKTestInfo{

		"simple": {
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
	tests := map[string]PKTestInfo{

		"simple": {
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
	tests := map[string]PKTestInfo{

		"simple": {
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
	tests := map[string]PKTestInfo{

		"floatPK": {
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
	tests := map[string]PKTestInfo{

		"floatPK": {
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
	tests := map[string]PKTestInfo{

		"simple": {
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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

func TestCharacterColumnChar(t *testing.T) {
	CharacterColumnTest(t, "table1", "DB012")
}

func TestCharacterColumnVarchar(t *testing.T) {
	CharacterColumnTest(t, "table1", "DB014")
}

func TestCharacterColumnLongVarchar(t *testing.T) {
	CharacterColumnTest(t, "table1", "DB015")
}

func CharacterColumnTest(t *testing.T, table string, database string) {
	t.Helper()
	testTable := table
	testDb := database
	tests := map[string]PKTestInfo{

		"notfound1": {
			pkReq: ds.PKReadBody{
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

		"notfound2": {
			pkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", *NewOperationID(t, 256)),
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "4"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			table:        testTable,
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col0", "\"ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïð\""},
		},

		"simple5": { //unicode pk
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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
			pkReq: ds.PKReadBody{
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

func TestDataTypesBlobs(t *testing.T) {

	testDb := "DB013"
	tests := map[string]PKTestInfo{

		"blob1": {
			pkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 5),
			},
			table:        "blob_table",
			db:           testDb,
			httpCode:     http.StatusInternalServerError,
			bodyContains: common.ERROR_026(),
			respKVs:      []string{},
		},

		"blob2": {
			pkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumn(t, "col1"),
				OperationID: NewOperationID(t, 5),
			},
			table:        "blob_table",
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col1", "1"},
		},

		"text1": {
			pkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 5),
			},
			table:        "text_table",
			db:           testDb,
			httpCode:     http.StatusInternalServerError,
			bodyContains: "",
			respKVs:      []string{},
		},

		"text2": {
			pkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumn(t, "col1"),
				OperationID: NewOperationID(t, 5),
			},
			table:        "text_table",
			db:           testDb,
			httpCode:     http.StatusOK,
			bodyContains: "",
			respKVs:      []string{"col1", "1"},
		},
	}

	test(t, tests)
}

func test(t *testing.T, tests map[string]PKTestInfo) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			withDBs(t, [][][]string{common.Database(testInfo.db)}, func(router *gin.Engine) {
				url := NewPKReadURL(testInfo.db, testInfo.table)
				body, _ := json.MarshalIndent(testInfo.pkReq, "", "\t")
				res := tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url,
					string(body), testInfo.httpCode, testInfo.bodyContains)
				if len(testInfo.respKVs) > 0 {
					tu.ValidateResponse(t, res, testInfo.respKVs...)
				}
			})
		})
	}
}
