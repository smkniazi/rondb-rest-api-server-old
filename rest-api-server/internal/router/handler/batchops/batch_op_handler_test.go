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
package batchops

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	ds "hopsworks.ai/rdrs/internal/datastructs"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
)

func TestBatchSimple1(t *testing.T) {
	//int table DB004

	tests := map[string]ds.BatchOperationTestInfo{
		"simple1": { //single operation batch
			HttpCode: http.StatusOK,
			Operations: []ds.BatchSubOperationTestInfo{
				ds.BatchSubOperationTestInfo{
					SubOperation: ds.BatchSubOperation{
						Method:      &[]string{ds.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB004/int_table/" + ds.PK_DB_OPERATION)}[0],
						Body: &ds.PKReadBody{
							Filters:     tu.NewFiltersKVs(t, "id0", 0, "id1", 0),
							ReadColumns: tu.NewReadColumns(t, "col", 2),
							OperationID: tu.NewOperationID(t, 64),
						},
					},
					Table:        "int_table",
					DB:           "DB004",
					HttpCode:     http.StatusOK,
					BodyContains: "",
					RespKVs:      []interface{}{"col0", "col1"},
				},
			},
		},
		"simple2": { //small batch operation
			HttpCode: http.StatusOK,
			Operations: []ds.BatchSubOperationTestInfo{
				ds.BatchSubOperationTestInfo{
					SubOperation: ds.BatchSubOperation{
						Method:      &[]string{ds.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB004/int_table/" + ds.PK_DB_OPERATION)}[0],
						Body: &ds.PKReadBody{
							Filters:     tu.NewFiltersKVs(t, "id0", 0, "id1", 0),
							ReadColumns: tu.NewReadColumns(t, "col", 2),
							OperationID: tu.NewOperationID(t, 64),
						},
					},
					Table:        "int_table",
					DB:           "DB004",
					HttpCode:     http.StatusOK,
					BodyContains: "",
					RespKVs:      []interface{}{"col0", "col1"},
				},
				ds.BatchSubOperationTestInfo{
					SubOperation: ds.BatchSubOperation{
						Method:      &[]string{ds.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB005/bigint_table/" + ds.PK_DB_OPERATION)}[0],
						Body: &ds.PKReadBody{
							Filters:     tu.NewFiltersKVs(t, "id0", 0, "id1", 0),
							ReadColumns: tu.NewReadColumns(t, "col", 2),
							OperationID: tu.NewOperationID(t, 64),
						},
					},
					Table:        "bigint_table",
					DB:           "DB005",
					HttpCode:     http.StatusOK,
					BodyContains: "",
					RespKVs:      []interface{}{"col0", "col1"},
				},
			},
		},
		"simple3": { // bigger batch of numbers table
			HttpCode: http.StatusOK,
			Operations: []ds.BatchSubOperationTestInfo{
				ds.BatchSubOperationTestInfo{
					SubOperation: ds.BatchSubOperation{
						Method:      &[]string{ds.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB004/int_table/" + ds.PK_DB_OPERATION)}[0],
						Body: &ds.PKReadBody{
							Filters:     tu.NewFiltersKVs(t, "id0", 0, "id1", 0),
							ReadColumns: tu.NewReadColumns(t, "col", 2),
							OperationID: tu.NewOperationID(t, 64),
						},
					},
					Table:        "int_table",
					DB:           "DB004",
					HttpCode:     http.StatusOK,
					BodyContains: "",
					RespKVs:      []interface{}{"col0", "col1"},
				},
				ds.BatchSubOperationTestInfo{
					SubOperation: ds.BatchSubOperation{
						Method:      &[]string{ds.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB005/bigint_table/" + ds.PK_DB_OPERATION)}[0],
						Body: &ds.PKReadBody{
							Filters:     tu.NewFiltersKVs(t, "id0", 0, "id1", 0),
							ReadColumns: tu.NewReadColumns(t, "col", 2),
							OperationID: tu.NewOperationID(t, 64),
						},
					},
					Table:        "bigint_table",
					DB:           "DB005",
					HttpCode:     http.StatusOK,
					BodyContains: "",
					RespKVs:      []interface{}{"col0", "col1"},
				},
				ds.BatchSubOperationTestInfo{
					SubOperation: ds.BatchSubOperation{
						Method:      &[]string{ds.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB006/tinyint_table/" + ds.PK_DB_OPERATION)}[0],
						Body: &ds.PKReadBody{
							Filters:     tu.NewFiltersKVs(t, "id0", -128, "id1", 0),
							ReadColumns: tu.NewReadColumns(t, "col", 2),
							OperationID: tu.NewOperationID(t, 64),
						},
					},
					Table:        "tinyint_table",
					DB:           "DB006",
					HttpCode:     http.StatusOK,
					BodyContains: "",
					RespKVs:      []interface{}{"col0", "col1"},
				},
				ds.BatchSubOperationTestInfo{
					SubOperation: ds.BatchSubOperation{
						Method:      &[]string{ds.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB007/smallint_table/" + ds.PK_DB_OPERATION)}[0],
						Body: &ds.PKReadBody{
							Filters:     tu.NewFiltersKVs(t, "id0", 32767, "id1", 65535),
							ReadColumns: tu.NewReadColumns(t, "col", 2),
							OperationID: tu.NewOperationID(t, 64),
						},
					},
					Table:        "smallint_table",
					DB:           "DB007",
					HttpCode:     http.StatusOK,
					BodyContains: "",
					RespKVs:      []interface{}{"col0", "col1"},
				},
				ds.BatchSubOperationTestInfo{
					SubOperation: ds.BatchSubOperation{
						Method:      &[]string{ds.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB007/smallint_table/" + ds.PK_DB_OPERATION)}[0],
						Body: &ds.PKReadBody{
							Filters:     tu.NewFiltersKVs(t, "id0", 1, "id1", 1),
							ReadColumns: tu.NewReadColumns(t, "col", 2),
							OperationID: tu.NewOperationID(t, 64),
						},
					},
					Table:        "smallint_table",
					DB:           "DB007",
					HttpCode:     http.StatusOK,
					BodyContains: "",
					RespKVs:      []interface{}{"col0", "col1"},
				},
			},
		},
		"notfound": { // a batch operation with operations throwing 404
			HttpCode: http.StatusOK,
			Operations: []ds.BatchSubOperationTestInfo{
				ds.BatchSubOperationTestInfo{
					SubOperation: ds.BatchSubOperation{
						Method:      &[]string{ds.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB004/int_table/" + ds.PK_DB_OPERATION)}[0],
						Body: &ds.PKReadBody{
							Filters:     tu.NewFiltersKVs(t, "id0", 100, "id1", 100),
							ReadColumns: tu.NewReadColumns(t, "col", 2),
							OperationID: tu.NewOperationID(t, 64),
						},
					},
					Table:        "int_table",
					DB:           "DB004",
					HttpCode:     http.StatusNotFound,
					BodyContains: "",
					RespKVs:      []interface{}{"col0", "col1"},
				},
				ds.BatchSubOperationTestInfo{
					SubOperation: ds.BatchSubOperation{
						Method:      &[]string{ds.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB005/bigint_table/" + ds.PK_DB_OPERATION)}[0],
						Body: &ds.PKReadBody{
							Filters:     tu.NewFiltersKVs(t, "id0", 100, "id1", 100),
							ReadColumns: tu.NewReadColumns(t, "col", 2),
							OperationID: tu.NewOperationID(t, 64),
						},
					},
					Table:        "bigint_table",
					DB:           "DB005",
					HttpCode:     http.StatusNotFound,
					BodyContains: "",
					RespKVs:      []interface{}{"col0", "col1"},
				},
			},
		},
	}

	tu.BatchTest(t, tests, RegisterBatchTestHandler, false)
}

func TestBatchArrayTableChar(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB012", false, 100, true)
}

func TestBatchArrayTableVarchar(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB014", false, 50, false)
}

func TestBatchArrayTableLongVarchar(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB015", false, 256, false)
}

func TestBatchArrayTableBinary(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB016", true, 100, true)
}

func TestBatchArrayTableVarbinary(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB017", true, 100, false)
}

func TestBatchArrayTableLongVarbinary(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB018", true, 256, false)
}

func ArrayColumnBatchTest(t *testing.T, table string, database string, isBinary bool, colWidth int, padding bool) {

	arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "-1", http.StatusNotFound)
	tests := map[string]ds.BatchOperationTestInfo{
		"simple1": { // bigger batch of array column table
			HttpCode: http.StatusOK,
			Operations: []ds.BatchSubOperationTestInfo{
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "-1", http.StatusNotFound),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "1", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "2", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "3", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "4", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "这是一个测验", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "5", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "6", http.StatusOK),
			},
		},
	}

	tu.BatchTest(t, tests, RegisterBatchTestHandler, isBinary)
}

/*
* A bad sub operation fails the entire batch
 */
func TestBatchBadSubOp(t *testing.T) {
	table := "table1"
	database := "DB018"
	isBinary := true
	padding := false
	colWidth := 256

	arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "-1", http.StatusNotFound)
	tests := map[string]ds.BatchOperationTestInfo{
		"simple1": { // bigger batch of array column table
			HttpCode: http.StatusBadRequest,
			Operations: []ds.BatchSubOperationTestInfo{
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "-1", http.StatusNotFound),
				//This is bad operation. data is longer than the column width
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, *tu.NewOperationID(t, colWidth*4+1), http.StatusNotFound),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "1", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "2", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "3", http.StatusOK),
			},
		},
	}

	tu.BatchTest(t, tests, RegisterBatchTestHandler, isBinary)
}

func arrayColumnBatchTestSubOp(t *testing.T, table string, database string, isBinary bool, colWidth int, padding bool, pk string, expectedStatus int) ds.BatchSubOperationTestInfo {
	respKVs := []interface{}{"col0"}
	return ds.BatchSubOperationTestInfo{
		SubOperation: ds.BatchSubOperation{
			Method:      &[]string{ds.PK_HTTP_VERB}[0],
			RelativeURL: &[]string{string(database + "/" + table + "/" + ds.PK_DB_OPERATION)}[0],
			Body: &ds.PKReadBody{
				Filters:     tu.NewFiltersKVs(t, "id0", tu.Encode(pk, isBinary, colWidth, padding)),
				ReadColumns: tu.NewReadColumns(t, "col", 1),
				OperationID: tu.NewOperationID(t, 5),
			},
		},
		Table:        table,
		DB:           database,
		HttpCode:     expectedStatus,
		BodyContains: "",
		RespKVs:      respKVs,
	}
}

func TestBatchMissingReqField(t *testing.T) {
	router := gin.Default()
	group := router.Group(ds.DBS_OPS_EP_GROUP)
	group.POST(ds.BATCH_OPERATION, BatchOpsHandler)
	url := BatchURL()

	// Test missing method
	operations := NewOperationsTBD(t, 3)
	operations[1].Method = nil
	operationsWrapper := ds.BatchOperation{Operations: &operations}
	body, _ := json.Marshal(operationsWrapper)
	tu.ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'Method' failed ")

	// Test missing relative URL
	operations = NewOperationsTBD(t, 3)
	operations[1].RelativeURL = nil
	operationsWrapper = ds.BatchOperation{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	tu.ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'RelativeURL' failed ")

	// Test missing body
	operations = NewOperationsTBD(t, 3)
	operations[1].Body = nil
	operationsWrapper = ds.BatchOperation{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	tu.ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'Body' failed ")

	// Test missing filter in an operation
	operations = NewOperationsTBD(t, 3)
	*&operations[1].Body.Filters = nil
	operationsWrapper = ds.BatchOperation{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	tu.ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'Filters' failed")
}

func NewOperationsTBD(t *testing.T, numOps int) []ds.BatchSubOperation {
	operations := make([]ds.BatchSubOperation, numOps)
	for i := 0; i < numOps; i++ {
		operations[i] = NewOperationTBD(t)
	}
	return operations
}

func NewOperationTBD(t *testing.T) ds.BatchSubOperation {
	pkOp := tu.NewPKReadReqBodyTBD(t)
	method := "POST"
	relativeURL := tu.NewPKReadURL("db", "table")

	op := ds.BatchSubOperation{
		Method:      &method,
		RelativeURL: &relativeURL,
		Body:        &pkOp,
	}

	return op
}

func BatchURL() string {
	return fmt.Sprintf("%s%s", ds.DBS_OPS_EP_GROUP, ds.BATCH_OPERATION)
}
