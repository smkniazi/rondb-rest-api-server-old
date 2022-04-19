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
	"encoding/base64"
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
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]ds.PKTestInfo{
		"simple1": {
			PkReq: ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", 0, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"simple2": { //with out operation ID
			PkReq: ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", 0, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"simple3": { //without read columns.
			PkReq:        ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", 0, "id1", 0)},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"simple4": { //Table with only primary keys
			PkReq: ds.PKReadBody{Filters: NewFiltersKVs(t, "id0", 0, "id1", 0),
				OperationID: NewOperationID(t, 64),
			},
			Table:        "int_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      []interface{}{},
		},

		"maxValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 2147483647, "id1", 4294967295),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"minValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -2147483648, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 1, "id1", -1), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []interface{}{},
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 2147483648, "id1", 4294967295), //bigger than the range
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []interface{}{},
		},

		"assigningSmallerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -2147483649, "id1", 0), //smaller than range
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      []interface{}{},
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 1, "id1", 1),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
	}

	test(t, tests, false)
}

func TestDataTypesBigInt(t *testing.T) {

	testTable := "bigint_table"
	testDb := "DB005"

	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]ds.PKTestInfo{

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 0, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"maxValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 9223372036854775807, "id1", uint64(18446744073709551615)),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"minValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -9223372036854775808, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 0, "id1", -1), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 9223372036854775807, "id1", "18446744073709551616"), //18446744073709551615+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"assigningSmallerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "-9223372036854775809", "id1", 0), //-9223372036854775808-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 1, "id1", 1),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

func TestDataTypesTinyInt(t *testing.T) {

	testTable := "tinyint_table"
	testDb := "DB006"
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]ds.PKTestInfo{

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 0, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"maxValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 127, "id1", 255),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"minValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -128, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 0, "id1", -1), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 127, "id1", 256), //255+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"assigningSmallerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -129, "id1", 0), //-128-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 1, "id1", 1),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

func TestDataTypesSmallInt(t *testing.T) {

	testTable := "smallint_table"
	testDb := "DB007"
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]ds.PKTestInfo{

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 0, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"maxValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 32767, "id1", 65535),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"minValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -32768, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 0, "id1", -1), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 32768, "id1", 256), //32767+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"assigningSmallerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -32769, "id1", 0), //-32768-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 1, "id1", 1),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

func TestDataTypesMediumInt(t *testing.T) {

	testTable := "mediumint_table"
	testDb := "DB008"
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]ds.PKTestInfo{

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 0, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"maxValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 8388607, "id1", 16777215),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"minValues": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -8388608, "id1", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 0, "id1", -1), //id1 is unsigned
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 8388608, "id1", 256), //8388607+1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"assigningSmallerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -8388609, "id1", 0), //-8388608-1
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 1, "id1", 1),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

func TestDataTypesFloat(t *testing.T) {

	// testTable := "float_table"
	testDb := "DB009"
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]ds.PKTestInfo{

		"floatPK": { // NDB does not support floats PKs
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 0),
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
				Filters:     NewFiltersKVs(t, "id0", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        "float_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
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
			RespKVs:      validateColumns,
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 2),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        "float_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

func TestDataTypesDouble(t *testing.T) {

	// testTable := "float_table"
	testDb := "DB010"
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]ds.PKTestInfo{

		"floatPK": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 0),
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
				Filters:     NewFiltersKVs(t, "id0", 0),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        "double_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"simple2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 1),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        "double_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", 2),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        "double_table1",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

func TestDataTypesDecimal(t *testing.T) {

	testTable := "decimal_table"
	testDb := "DB011"
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]ds.PKTestInfo{

		"simple": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -12345.12345, "id1", 12345.12345),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"nullVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -67890.12345, "id1", 67890.12345),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -12345.12345, "id1", -12345.12345),
				ReadColumns: NewReadColumns(t, "col", 2),
				OperationID: NewOperationID(t, 64),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},

		"assigningBiggerVals": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", -12345.12345, "id1", 123456789.12345),
				ReadColumns: NewReadColumns(t, "col", 2),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_015(),
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

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
			RespKVs:      []interface{}{},
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
			RespKVs:      []interface{}{"col1"},
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
			RespKVs:      []interface{}{},
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
			RespKVs:      []interface{}{"col1"},
		},
	}

	test(t, tests, false)
}

func TestDataTypesChar(t *testing.T) {
	CharacterColumnTest(t, "table1", "DB012", false, 100, true)
}

func TestDataTypesVarchar(t *testing.T) {
	CharacterColumnTest(t, "table1", "DB014", false, 50, false)
}

func TestDataTypesLongVarchar(t *testing.T) {
	CharacterColumnTest(t, "table1", "DB015", false, 256, false)
}

func TestDataTypesBinary(t *testing.T) {
	CharacterColumnTest(t, "table1", "DB016", true, 100, true)
}

func TestDataTypesVarbinary(t *testing.T) {
	CharacterColumnTest(t, "table1", "DB017", true, 100, false)
}

func TestDataTypesLongVarbinary(t *testing.T) {
	CharacterColumnTest(t, "table1", "DB018", true, 256, false)
}

func CharacterColumnTest(t *testing.T, table string, database string, isBinary bool, colWidth int, padding bool) {
	t.Helper()
	testTable := table
	testDb := database
	validateColumns := []interface{}{"col0"}
	tests := map[string]ds.PKTestInfo{

		"notfound1": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", encode("-1", isBinary, colWidth, padding)),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusNotFound,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"notfound2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", encode(*NewOperationID(t, colWidth+1), isBinary, colWidth, padding)),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusNotFound,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"simple1": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", encode("1", isBinary, colWidth, padding)),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"simple2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", encode("2", isBinary, colWidth, padding)),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"simple3": { // new line char in string
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", encode("3", isBinary, colWidth, padding)),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"simple4": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", encode("4", isBinary, colWidth, padding)),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"simple5": { //unicode pk
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", encode("这是一个测验", isBinary, colWidth, padding)),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"nulltest": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", encode("5", isBinary, colWidth, padding)),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"escapedChars": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", encode("6", isBinary, colWidth, padding)),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
	}

	test(t, tests, isBinary)
}

func TestDataTypesDateColumn(t *testing.T) {
	t.Helper()
	testTable := "date_table"
	testDb := "DB019"
	validateColumns := []interface{}{"col0"}
	tests := map[string]ds.PKTestInfo{

		"validpk1": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"validpk2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11 00:00:00"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"invalidpk": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11 11:00:00"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusNotFound,
			BodyContains: "",
			RespKVs:      []interface{}{},
		},

		"invalidpk2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11 00:00:00.123123"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusNotFound,
			BodyContains: "",
			RespKVs:      []interface{}{},
		},

		"nulltest1": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-12"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"nulltest2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"error": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-13-11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        testTable,
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_027(),
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

func TestDataTypesDatetimeColumn(t *testing.T) {
	t.Helper()
	testDb := "DB020"
	validateColumns := []interface{}{"col0"}
	tests := map[string]ds.PKTestInfo{

		"validpk1_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11 11:11:11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "date_table0",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"validpk1_pre3": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11 11:11:11.123"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "date_table3",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"validpk1_pre6": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11 11:11:11.123456"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "date_table6",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"validpk2_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11 11:11:11.123123"), // nanoseconds should be ignored
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "date_table0",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"validpk2_pre3": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11 11:11:11.123000"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "date_table3",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"validpk2_pre6": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11 -11:11:11.123456"), //-iv sign should be ignored
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "date_table6",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"nulltest_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-12 11:11:11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "date_table0",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"nulltest_pre3": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-12 11:11:11.123"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "date_table3",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"nulltest_pre6": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-12 11:11:11.123456"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "date_table6",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"wrongdate_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-13-11 11:11:11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "date_table0",
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_027(),
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

func TestDataTypesTimeColumn(t *testing.T) {
	t.Helper()
	testDb := "DB021"
	validateColumns := []interface{}{"col0"}
	tests := map[string]ds.PKTestInfo{

		"validpk1_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "11:11:11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "time_table0",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"validpk1_pre3": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "11:11:11.123"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "time_table3",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"validpk1_pre6": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "11:11:11.123456"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "time_table6",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"validpk2_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "11:11:11.123123"), // nanoseconds should be ignored
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "time_table0",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"validpk2_pre3": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "11:11:11.123000"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "time_table3",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"nulltest_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "12:11:11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "time_table0",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"nulltest_pre3": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "12:11:11.123"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "time_table3",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"nulltest_pre6": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "12:11:11.123456"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "time_table6",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"wrongtime_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "11:61:11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "time_table0",
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_027(),
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

func TestDataTypesTimestampColumn(t *testing.T) {
	t.Helper()
	testDb := "DB022"
	validateColumns := []interface{}{"col0"}
	tests := map[string]ds.PKTestInfo{

		"badts_1": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1111-11-11 11:11:11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table0",
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_027(),
			RespKVs:      validateColumns,
		},

		"badts_2": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "1970-01-01 00:00:00"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table0",
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_027(),
			RespKVs:      validateColumns,
		},

		"badts_3": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2038-01-19 03:14:08"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table0",
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_027(),
			RespKVs:      validateColumns,
		},

		"validpk1_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2022-11-11 11:11:11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table0",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"validpk1_pre3": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2022-11-11 11:11:11.123"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table3",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"validpk1_pre6": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2022-11-11 11:11:11.123456"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table6",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"validpk2_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2022-11-11 11:11:11.123123"), // nanoseconds should be ignored
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table0",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"validpk2_pre3": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2022-11-11 11:11:11.123000"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table3",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"validpk2_pre6": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2022-11-11 -11:11:11.123456"), //-iv sign should be ignored
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table6",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"nulltest_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2022-11-12 11:11:11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table0",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"nulltest_pre3": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2022-11-12 11:11:11.123"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table3",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},
		"nulltest_pre6": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2022-11-12 11:11:11.123456"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table6",
			Db:           testDb,
			HttpCode:     http.StatusOK,
			BodyContains: "",
			RespKVs:      validateColumns,
		},

		"wrongdate_pre0": {
			PkReq: ds.PKReadBody{
				Filters:     NewFiltersKVs(t, "id0", "2022-13-11 11:11:11"),
				ReadColumns: NewReadColumns(t, "col", 1),
				OperationID: NewOperationID(t, 5),
			},
			Table:        "ts_table0",
			Db:           testDb,
			HttpCode:     http.StatusBadRequest,
			BodyContains: common.ERROR_027(),
			RespKVs:      validateColumns,
		},
	}
	test(t, tests, false)
}

func encode(data string, binary bool, colWidth int, padding bool) string {

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

func test(t *testing.T, tests map[string]ds.PKTestInfo, isBinaryData bool) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			withDBs(t, [][][]string{common.Database(testInfo.Db)}, func(router *gin.Engine) {
				url := NewPKReadURL(testInfo.Db, testInfo.Table)
				body, _ := json.MarshalIndent(testInfo.PkReq, "", "\t")
				res := tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url,
					string(body), testInfo.HttpCode, testInfo.BodyContains)
				if res.OK {
					tu.ValidateResArrayData(t, testInfo, res, isBinaryData)
				}
			})
		})
	}
}
