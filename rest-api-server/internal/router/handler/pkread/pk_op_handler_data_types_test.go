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

// INT TESTS
// Test signed and unsigned int data type
func TestDataTypesInt(t *testing.T) {

	testTable := "int_table"
	testDb := "DB004"
	tests := map[string]ds.PKTestInfo{
		"simple1": {
			PkReq: ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "0", "col1", "0"},
		},

		"simple2": { //with out operation ID
			PkReq: ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "0", "col1", "0"},
		},

		"simple3": { //without read columns.
			PkReq:        ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", "0", "id1", "0")},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "0", "col1", "0"},
		},

		"simple4": { //Table with only primary keys
			PkReq: ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", "0", "id1", "0"),
				OperationID: NewOperationID(t, 64),
			},
			Table:        "int_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{},
		},

		"maxValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2147483647", "id1", "4294967295"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "2147483647", "col1", "4294967295"},
		},

		"minValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-2147483648", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "-2147483648", "col1", "0"},
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "-1"), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2147483648", "id1", "4294967295"), //bigger than the range
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningSmallerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-2147483649", "id1", "0"), //smaller than range
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "null", "col1", "null"},
		},
	}

	test(t, tests)
}

func TestDataTypesBigInt(t *testing.T) {

	testTable := "bigint_table"
	testDb := "DB005"

	tests := map[string]ds.PKTestInfo{

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "0", "col1", "0"},
		},

		"maxValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "9223372036854775807", "id1", "18446744073709551615"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "9223372036854775807", "col1", "18446744073709551615"},
		},

		"minValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-9223372036854775808", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "-9223372036854775808", "col1", "0"},
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "-1"), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "9223372036854775807", "id1", "18446744073709551616"), //18446744073709551615+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningSmallerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-9223372036854775809", "id1", "0"), //-9223372036854775808-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func TestDataTypesTinyInt(t *testing.T) {

	testTable := "tinyint_table"
	testDb := "DB006"
	tests := map[string]ds.PKTestInfo{

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "0", "col1", "0"},
		},

		"maxValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "127", "id1", "255"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "127", "col1", "255"},
		},

		"minValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-128", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "-128", "col1", "0"},
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "-1"), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "127", "id1", "256"), //255+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningSmallerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-129", "id1", "0"), //-128-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func TestDataTypesSmallInt(t *testing.T) {

	testTable := "smallint_table"
	testDb := "DB007"
	tests := map[string]ds.PKTestInfo{

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "0", "col1", "0"},
		},

		"maxValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "32767", "id1", "65535"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "32767", "col1", "65535"},
		},

		"minValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-32768", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "-32768", "col1", "0"},
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "-1"), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "32768", "id1", "256"), //32767+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningSmallerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-32769", "id1", "0"), //-32768-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func TestDataTypesMediumInt(t *testing.T) {

	testTable := "mediumint_table"
	testDb := "DB008"
	tests := map[string]ds.PKTestInfo{

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "0", "col1", "0"},
		},

		"maxValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "8388607", "id1", "16777215"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "8388607", "col1", "16777215"},
		},

		"minValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-8388608", "id1", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "-8388608", "col1", "0"},
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0", "id1", "-1"), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "8388608", "id1", "256"), //8388607+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningSmallerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-8388609", "id1", "0"), //-8388608-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1", "id1", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func TestDataTypesFloat(t *testing.T) {

	// testTable := "float_table"
	testDb := "DB009"
	tests := map[string]ds.PKTestInfo{

		"floatPK": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        "float_table2",
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_017(),
		},

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        "float_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "0", "col1", "0"},
		},

		"simple2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        "float_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "-123.123", "col1", "123.123"},
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        "float_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func TestDataTypesDouble(t *testing.T) {

	// testTable := "float_table"
	testDb := "DB010"
	tests := map[string]ds.PKTestInfo{

		"floatPK": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        "double_table2",
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_017(),
		},

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "0"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        "double_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "0", "col1", "0"},
		},

		"simple2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        "double_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "-123.123", "col1", "123.123"},
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        "double_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "null", "col1", "null"},
		},
	}
	test(t, tests)
}

func TestDataTypesDecimal(t *testing.T) {

	testTable := "decimal_table"
	testDb := "DB011"
	tests := map[string]ds.PKTestInfo{

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-12345.12345", "id1", "12345.12345"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "-12345.12345", "col1", "12345.12345"},
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-67890.12345", "id1", "67890.12345"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col0", "null", "col1", "null"},
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-12345.12345", "id1", "-12345.12345"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-12345.12345", "id1", "123456789.12345"),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []string{},
		},
	}
	test(t, tests)
}

//func TestCharacterColumnChar(t *testing.T) {
//	CharacterColumnTest(t, "table1", "DB012", false, -1, false)
//}
//
//func TestCharacterColumnVarchar(t *testing.T) {
//	CharacterColumnTest(t, "table1", "DB014", false, -1, false)
//}
//
//func TestCharacterColumnLongVarchar(t *testing.T) {
//	CharacterColumnTest(t, "table1", "DB015", false, -1, false)
//}
//
//func TestCharacterColumnBinary(t *testing.T) {
//	CharacterColumnTest(t, "table1", "DB016", true, 255, true)
//}
//
//func CharacterColumnTest(t *testing.T, table string, database string, isBinary bool, colWidth int, padding bool) {
//	t.Helper()
//	testTable := table
//	testDb := database
//	tests := map[string]ds.PKTestInfo{
//
//		"notfound1": {
//			PkReq: ds.PKReadBody{
//				Filters:     NewFiltersKVs(t, "id0", encode("-1", isBinary, colWidth, padding)),
//				ReadColumns: NewReadColumns(t, "col", 1),
//				OperationID: NewOperationID(t, 5),
//			},
//			Table:        testTable,
//			Db:           testDb,
//			HttpCode:     http.StatusNotFound,
//			BodyContains: "",
//			RespKVs:      []string{},
//		},
//
//		"notfound2": {
//			PkReq: ds.PKReadBody{
//				Filters:     NewFiltersKVs(t, "id0", encode(*NewOperationID(t, 256), isBinary, colWidth, padding)),
//				ReadColumns: NewReadColumns(t, "col", 1),
//				OperationID: NewOperationID(t, 5),
//			},
//			Table:        testTable,
//			Db:           testDb,
//			HttpCode:     http.StatusNotFound,
//			BodyContains: "",
//			RespKVs:      []string{},
//		},
//
//		"simple1": {
//			PkReq: ds.PKReadBody{
//				Filters:     NewFiltersKVs(t, "id0", encode("1", isBinary, colWidth, padding)),
//				ReadColumns: NewReadColumns(t, "col", 1),
//				OperationID: NewOperationID(t, 5),
//			},
//			Table:        testTable,
//			Db:           testDb,
//			HttpCode:     http.StatusOK,
//			BodyContains: "",
//			RespKVs:      []string{"col0", encode("\"这是一个测验。 我不知道怎么读中文。\"", isBinary, colWidth, padding)},
//		},
//
//		"simple2": {
//			PkReq: ds.PKReadBody{
//				Filters:     NewFiltersKVs(t, "id0", encode("2", isBinary, colWidth, padding)),
//				ReadColumns: NewReadColumns(t, "col", 1),
//				OperationID: NewOperationID(t, 5),
//			},
//			Table:        testTable,
//			Db:           testDb,
//			HttpCode:     http.StatusOK,
//			BodyContains: "",
//			RespKVs:      []string{"col0", encode("\"f\\u0000f\"", isBinary, colWidth, padding)},
//		},
//
//		"simple3": { // new line char in string
//			PkReq: ds.PKReadBody{
//				Filters:     NewFiltersKVs(t, "id0", encode("3", isBinary, colWidth, padding)),
//				ReadColumns: NewReadColumns(t, "col", 1),
//				OperationID: NewOperationID(t, 5),
//			},
//			Table:        testTable,
//			Db:           testDb,
//			HttpCode:     http.StatusOK,
//			BodyContains: "",
//			RespKVs:      []string{"col0", encode("\"a\\nb\"", isBinary, colWidth, padding)},
//		},
//
//		"simple4": {
//			PkReq: ds.PKReadBody{
//				Filters:     NewFiltersKVs(t, "id0", encode("4", isBinary, colWidth, padding)),
//				ReadColumns: NewReadColumns(t, "col", 1),
//				OperationID: NewOperationID(t, 5),
//			},
//			Table:        testTable,
//			Db:           testDb,
//			HttpCode:     http.StatusOK,
//			BodyContains: "",
//			RespKVs:      []string{"col0", encode("\"ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïð\"", isBinary, colWidth, padding)},
//		},
//
//		"simple5": { //unicode pk
//			PkReq: ds.PKReadBody{
//				Filters:     NewFiltersKVs(t, "id0", encode("这是一个测验", isBinary, colWidth, padding)),
//				ReadColumns: NewReadColumns(t, "col", 1),
//				OperationID: NewOperationID(t, 5),
//			},
//			Table:        testTable,
//			Db:           testDb,
//			HttpCode:     http.StatusOK,
//			BodyContains: "",
//			RespKVs:      []string{"col0", encode("\"12345\"", isBinary, colWidth, padding)},
//		},
//
//		"nulltest": {
//			PkReq: ds.PKReadBody{
//				Filters:     NewFiltersKVs(t, "id0", encode("5", isBinary, colWidth, padding)),
//				ReadColumns: NewReadColumns(t, "col", 1),
//				OperationID: NewOperationID(t, 5),
//			},
//			Table:        testTable,
//			Db:           testDb,
//			HttpCode:     http.StatusOK,
//			BodyContains: "",
//			RespKVs:      []string{"col0", "null"},
//		},
//
//		"escapedChars": {
//			PkReq: ds.PKReadBody{
//				Filters:     NewFiltersKVs(t, "id0", encode("6", isBinary, colWidth, padding)),
//				ReadColumns: NewReadColumns(t, "col", 1),
//				OperationID: NewOperationID(t, 5),
//			},
//			Table:        testTable,
//			Db:           testDb,
//			HttpCode:     http.StatusOK,
//			BodyContains: "",
//			RespKVs:      []string{"col0", encode(`"\"\\\bf\n\r\t$%_?"`, isBinary, colWidth, padding)}, // in mysql \f is replaced by f
//		},
//	}
//
//	test(t, tests)
//}
//
//func encode(data string, binary bool, colWidth int, padding bool) string {
//
//	if binary {
//
//		newData := []byte(data)
//		if padding {
//			length := colWidth
//			if length < len(data) {
//				length = len(data)
//			}
//
//			newData = make([]byte, length)
//			for i := 0; i < length; i++ {
//				newData[i] = 0x00
//			}
//			for i := 0; i < len(data); i++ {
//				newData[i] = data[i]
//			}
//		}
//		fmt.Printf("----------- \n")
//		fmt.Printf("old data is %s \n", data)
//		fmt.Printf("new data len is %d \n", len(newData))
//		fmt.Printf("new data  is %x \n", newData)
//		return base64.StdEncoding.EncodeToString(newData)
//	} else {
//		return data
//	}
//}

func TestDataTypesBlobs(t *testing.T) {

	testDb := "DB013"
	tests := map[string]ds.PKTestInfo{

		"blob1": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "blob_table",
			Db:           testDb,
			HttpCode:     http.StatusInternalServerError,
			BodyContains: common.ERROR_026(),
			RespKVs:      []string{},
		},

		"blob2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumn(t, "col1"),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "blob_table",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col1", "1"},
		},

		"text1": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "text_table",
			Db:           testDb,
			HttpCode:     http.StatusInternalServerError,
			BodyContains: "",
			RespKVs:      []string{},
		},

		"text2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1"),
				ReadColumns: NewReadColumn(t, "col1"),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "text_table",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []string{"col1", "1"},
		},
	}

	test(t, tests)
}

func test(t *testing.T, tests map[string]ds.PKTestInfo) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			withDBs(t, [][][]string{common.Database(testInfo.Db)}, func(router *gin.Engine) {
				url := NewPKReadURL(testInfo.Db, testInfo.Table)
				body, _ := json.MarshalIndent(testInfo.PkReq, "", "\t")
				res := tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url,
					string(body), testInfo.HttpCode, testInfo.BodyContains)
				if len(testInfo.RespKVs) > 0 {
					tu.ValidateResponse(t, testInfo, res, testInfo.RespKVs...)
				}
			})
		})
	}
}
