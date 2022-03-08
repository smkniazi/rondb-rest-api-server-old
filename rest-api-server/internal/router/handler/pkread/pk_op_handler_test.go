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
	"time"

	"github.com/gin-gonic/gin"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
)

func TestPKNative(t *testing.T) {

	router := gin.New()

	group := router.Group(DB_OPS_EP_GROUP)
	group.POST(DB_OPERATION, PkReadHandler)

	param := PKReadBody{
		Filters:     NewFilters(t, "filter_col_", 3),
		ReadColumns: NewReadColumns(t, "read_col_", 5),
		OperationID: NewOperationID(t, 64),
	}

	body, _ := json.MarshalIndent(param, "", "\t")

	for i := 0; i < 1; i++ {
		url := NewPKReadURL(fmt.Sprintf("db_%d", i), fmt.Sprintf("time_%d", i))
		tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusOK, "")
	}

	time.Sleep(5 * time.Second)
}

// Simple test with all parameters correctly supplied
func TestPKReadTest(t *testing.T) {
	router := gin.Default()
	group := router.Group(DB_OPS_EP_GROUP)
	group.POST(DB_OPERATION, PkReadHandler)

	param := PKReadBody{
		Filters:     NewFilters(t, "filter_col_", 3),
		ReadColumns: NewReadColumns(t, "read_col_", 5),
		OperationID: NewOperationID(t, 64),
	}

	body, _ := json.MarshalIndent(param, "", "\t")
	url := NewPKReadURL("db", "table")
	tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusOK, "")

	// Omit the optional operation ID param
	param.OperationID = nil
	param.ReadColumns = nil
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusOK, "")
}

func TestPKReadOmitRequired(t *testing.T) {
	router := gin.Default()
	group := router.Group(DB_OPS_EP_GROUP)
	group.POST(DB_OPERATION, PkReadHandler)

	// Test. Omitting filter should result in 400 error
	param := PKReadBody{
		Filters:     nil,
		ReadColumns: NewReadColumns(t, "read_col_", 5),
		OperationID: NewOperationID(t, 64),
	}

	body, _ := json.MarshalIndent(param, "", "\t")
	url := NewPKReadURL("db", "table")
	tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'Filters'")

	// Test. unset filter values should result in 400 error
	col := "col"
	filter := NewFilter(t, &col, nil)
	param.Filters = filter
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'Value'")

	val := "val"
	filter = NewFilter(t, nil, &val)
	param.Filters = filter
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'Column'")
}

func TestPKReadLargeColumns(t *testing.T) {
	router := gin.Default()
	group := router.Group(DB_OPS_EP_GROUP)
	group.POST(DB_OPERATION, PkReadHandler)

	// Test. Large filter column names.
	col := RandString(65)
	val := "val"
	param := PKReadBody{
		Filters:     NewFilter(t, &col, &val),
		ReadColumns: NewReadColumns(t, "read_col_", 5),
		OperationID: NewOperationID(t, 64),
	}
	body, _ := json.MarshalIndent(param, "", "\t")
	url := NewPKReadURL("db", "table")
	tu.ProcessRequest(t, router, HTTP_VERB,
		url, string(body), http.StatusBadRequest, "Field validation for 'Column' failed on the 'max' tag")

	// Test. Large read column names.
	param = PKReadBody{
		Filters:     NewFilters(t, "filter_col_", 3),
		ReadColumns: NewReadColumns(t, RandString(65), 5),
		OperationID: NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, HTTP_VERB,
		url, string(body), http.StatusBadRequest, "field length validation failed")

	// Test. Large db and table names
	param = PKReadBody{
		Filters:     NewFilters(t, "filter_col_", 3),
		ReadColumns: NewReadColumns(t, "read_col_", 5),
		OperationID: NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	url1 := NewPKReadURL(RandString(65), "table")
	tu.ProcessRequest(t, router, HTTP_VERB,
		url1, string(body), http.StatusBadRequest, "Field validation for 'DB' failed on the 'max' tag")
	url2 := NewPKReadURL("db", RandString(65))
	tu.ProcessRequest(t, router, HTTP_VERB,
		url2, string(body), http.StatusBadRequest, "Field validation for 'Table' failed on the 'max' tag")
	url3 := NewPKReadURL("", "table")
	tu.ProcessRequest(t, router, HTTP_VERB,
		url3, string(body), http.StatusBadRequest, "Field validation for 'DB' failed on the 'min' tag")
	url4 := NewPKReadURL("db", "")
	tu.ProcessRequest(t, router, HTTP_VERB,
		url4, string(body), http.StatusBadRequest, "Field validation for 'Table' failed on the 'min' tag")
}

func TestPKInvalidIdentifier(t *testing.T) {
	router := gin.Default()
	group := router.Group(DB_OPS_EP_GROUP)
	group.POST(DB_OPERATION, PkReadHandler)

	// Test. invalid filter
	col := "col@"
	val := "val"
	param := PKReadBody{
		Filters:     NewFilter(t, &col, &val),
		ReadColumns: NewReadColumn(t, "read_col"),
		OperationID: NewOperationID(t, 64),
	}
	body, _ := json.MarshalIndent(param, "", "\t")
	url := NewPKReadURL("db", "table")
	tu.ProcessRequest(t, router, HTTP_VERB, url, string(body),
		http.StatusBadRequest, "field validation failed. Invalid character '@'")

	// Test. invalid read col
	col = "col"
	val = "val"
	param = PKReadBody{
		Filters:     NewFilter(t, &col, &val),
		ReadColumns: NewReadColumn(t, "col#"),
		OperationID: NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, HTTP_VERB, url, string(body),
		http.StatusBadRequest, "field validation failed. Invalid character '#'")

	// Test. Invalid path parameteres
	param = PKReadBody{
		Filters:     NewFilter(t, &col, &val),
		ReadColumns: NewReadColumn(t, "col"),
		OperationID: NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	url1 := NewPKReadURL("db*", "table")
	tu.ProcessRequest(t, router, HTTP_VERB,
		url1, string(body), http.StatusBadRequest, "field validation failed. Invalid character '*'")
	url2 := NewPKReadURL("db", "table!")
	tu.ProcessRequest(t, router, HTTP_VERB,
		url2, string(body), http.StatusBadRequest, "field validation failed. Invalid character '!'")
}

func TestPKUniqueParams(t *testing.T) {
	router := gin.Default()
	group := router.Group(DB_OPS_EP_GROUP)
	group.POST(DB_OPERATION, PkReadHandler)

	// Test. unique read columns
	readColumns := make([]string, 2)
	readColumns[0] = "col1"
	readColumns[1] = "col1"
	param := PKReadBody{
		Filters:     NewFilters(t, "col", 1),
		ReadColumns: &readColumns,
		OperationID: NewOperationID(t, 64),
	}
	url := NewPKReadURL("db", "table")
	body, _ := json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Field validation for 'ReadColumns' failed on the 'unique' tag")

	// Test. unique filter columns
	col := "col"
	val := "val"
	filters := make([]Filter, 2)
	filters[0] = (*(NewFilter(t, &col, &val)))[0]
	filters[1] = (*(NewFilter(t, &col, &val)))[0]

	param = PKReadBody{
		Filters:     &filters,
		ReadColumns: NewReadColumns(t, "read_col_", 5),
		OperationID: NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusBadRequest,
		"field validation for filter failed on the 'unique' tag")

	//Test that filter and read columns do not contain overlapping columns
	param = PKReadBody{
		Filters:     NewFilter(t, &col, &val),
		ReadColumns: NewReadColumn(t, col),
		OperationID: NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusBadRequest,
		fmt.Sprintf("field validation for read columns faild. '%s' already included in filter", col))
}
