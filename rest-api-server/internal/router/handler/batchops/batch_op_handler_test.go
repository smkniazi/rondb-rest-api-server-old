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

func TestBatchSimple(t *testing.T) {
	//int table DB004

	tests := map[string]ds.BatchOperationTestInfo{
		"simple1": {
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
		"simple2": {
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
	}

	tu.BatchTest(t, tests, RegisterBatchTestHandler, false)
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
