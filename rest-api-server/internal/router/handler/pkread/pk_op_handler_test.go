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
	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs/internal/common"
	ds "hopsworks.ai/rdrs/internal/datastructs"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
)

func TestPKReadOmitRequired(t *testing.T) {
	router, err := tu.InitRouter(t, RegisterPKTestHandler)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// Test. Omitting filter should result in 400 error
	param := ds.PKReadBody{
		Filters:     nil,
		ReadColumns: tu.NewReadColumns(t, "read_col_", 5),
		OperationID: tu.NewOperationID(t, 64),
	}

	url := tu.NewPKReadURL("db", "table")

	body, _ := json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'Filters'")

	// Test. unset filter values should result in 400 error
	col := "col"
	filter := tu.NewFilter(t, &col, nil)
	param.Filters = filter
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Field validation for 'Value' failed on the 'required' tag")

	val := "val"
	filter = tu.NewFilter(t, nil, val)
	param.Filters = filter
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Field validation for 'Column' failed on the 'required' tag")
}

func TestPKReadLargeColumns(t *testing.T) {
	router, err := tu.InitRouter(t, RegisterPKTestHandler)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// Test. Large filter column names.
	col := tu.RandString(65)
	val := "val"
	param := ds.PKReadBody{
		Filters:     tu.NewFilter(t, &col, val),
		ReadColumns: tu.NewReadColumns(t, "read_col_", 5),
		OperationID: tu.NewOperationID(t, 64),
	}
	body, _ := json.MarshalIndent(param, "", "\t")
	url := tu.NewPKReadURL("db", "table")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body),
		http.StatusBadRequest, "Field validation for 'Column' failed on the 'max' tag")

	// Test. Large read column names.
	param = ds.PKReadBody{
		Filters:     tu.NewFilters(t, "filter_col_", 3),
		ReadColumns: tu.NewReadColumns(t, tu.RandString(65), 5),
		OperationID: tu.NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB,
		url, string(body), http.StatusBadRequest, "field length validation failed")

	// Test. Large db and table names
	param = ds.PKReadBody{
		Filters:     tu.NewFilters(t, "filter_col_", 3),
		ReadColumns: tu.NewReadColumns(t, "read_col_", 5),
		OperationID: tu.NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	url1 := tu.NewPKReadURL(tu.RandString(65), "table")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url1, string(body),
		http.StatusBadRequest, "Field validation for 'DB' failed on the 'max' tag")
	url2 := tu.NewPKReadURL("db", tu.RandString(65))
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url2, string(body),
		http.StatusBadRequest, "Field validation for 'Table' failed on the 'max' tag")
	url3 := tu.NewPKReadURL("", "table")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url3, string(body),
		http.StatusBadRequest, "Field validation for 'DB' failed on the 'min' tag")
	url4 := tu.NewPKReadURL("db", "")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url4, string(body), http.StatusBadRequest,
		"Field validation for 'Table' failed on the 'min' tag")
}

func TestPKInvalidIdentifier(t *testing.T) {
	router, err := tu.InitRouter(t, RegisterPKTestHandler)
	if err != nil {
		t.Fatalf("%v", err)
	}

	//Valid chars [ U+0001 .. U+007F] and [ U+0080 .. U+FFFF]

	// Test. invalid filter
	col := "col" + string(rune(0x0000))
	val := "val"
	param := ds.PKReadBody{
		Filters:     tu.NewFilter(t, &col, val),
		ReadColumns: tu.NewReadColumn(t, "read_col"),
		OperationID: tu.NewOperationID(t, 64),
	}
	body, _ := json.MarshalIndent(param, "", "\t")
	url := tu.NewPKReadURL("db", "table")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
		fmt.Sprintf("field validation failed. Invalid character '%U' ", rune(0x0000)))

	// Test. invalid read col
	col = "col"
	val = "val"
	param = ds.PKReadBody{
		Filters:     tu.NewFilter(t, &col, val),
		ReadColumns: tu.NewReadColumn(t, "col"+string(rune(0x10000))),
		OperationID: tu.NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
		fmt.Sprintf("field validation failed. Invalid character '%U'", rune(0x10000)))

	// Test. Invalid path parameteres
	param = ds.PKReadBody{
		Filters:     tu.NewFilter(t, &col, val),
		ReadColumns: tu.NewReadColumn(t, "col"),
		OperationID: tu.NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	url1 := tu.NewPKReadURL("db"+string(rune(0x10000)), "table")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url1, string(body), http.StatusBadRequest,
		fmt.Sprintf("field validation failed. Invalid character '%U'", rune(0x10000)))
	url2 := tu.NewPKReadURL("db", "table"+string(rune(0x10000)))
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url2, string(body), http.StatusBadRequest,
		fmt.Sprintf("field validation failed. Invalid character '%U'", rune(0x10000)))
}

func TestPKUniqueParams(t *testing.T) {
	router, err := tu.InitRouter(t, RegisterPKTestHandler)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// Test. unique read columns
	readColumns := make([]ds.ReadColumn, 2)
	col := "col1"
	readColumns[0].Column = &col
	readColumns[1].Column = &col
	param := ds.PKReadBody{
		Filters:     tu.NewFilters(t, "col", 1),
		ReadColumns: &readColumns,
		OperationID: tu.NewOperationID(t, 64),
	}
	url := tu.NewPKReadURL("db", "table")
	body, _ := json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"field validation for 'ReadColumns' failed on the 'unique' tag")

	// Test. unique filter columns
	col = "col"
	val := "val"
	filters := make([]ds.Filter, 2)
	filters[0] = (*(tu.NewFilter(t, &col, val)))[0]
	filters[1] = (*(tu.NewFilter(t, &col, val)))[0]

	param = ds.PKReadBody{
		Filters:     &filters,
		ReadColumns: tu.NewReadColumns(t, "read_col_", 5),
		OperationID: tu.NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"field validation for filter failed on the 'unique' tag")

	//Test that filter and read columns do not contain overlapping columns
	param = ds.PKReadBody{
		Filters:     tu.NewFilter(t, &col, val),
		ReadColumns: tu.NewReadColumn(t, col),
		OperationID: tu.NewOperationID(t, 64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
		fmt.Sprintf("field validation for read columns faild. '%s' already included in filter", col))
}

// DB/Table does not exist
func TestPKERROR_011(t *testing.T) {

	tu.WithDBs(t, [][][]string{common.Database("DB001")}, RegisterPKTestHandler, func(router *gin.Engine) {
		pkCol := "id0"
		pkVal := "1"
		param := ds.PKReadBody{
			Filters:     tu.NewFilter(t, &pkCol, pkVal),
			ReadColumns: tu.NewReadColumn(t, "col_0"),
			OperationID: tu.NewOperationID(t, 64),
		}

		body, _ := json.MarshalIndent(param, "", "\t")

		url := tu.NewPKReadURL("DB001_XXX", "table_1")
		tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_011())

		url = tu.NewPKReadURL("DB001", "table_1_XXX")
		tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_011())
	})
}

// column does not exist
func TestPKERROR_012(t *testing.T) {

	tu.WithDBs(t, [][][]string{common.Database("DB001")}, RegisterPKTestHandler, func(router *gin.Engine) {
		pkCol := "id0"
		pkVal := "1"
		param := ds.PKReadBody{
			Filters:     tu.NewFilter(t, &pkCol, pkVal),
			ReadColumns: tu.NewReadColumn(t, "col_0_XXX"),
			OperationID: tu.NewOperationID(t, 64),
		}

		body, _ := json.MarshalIndent(param, "", "\t")

		url := tu.NewPKReadURL("DB001", "table_1")
		tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_012())
	})
}

// Primary key test.
func TestPKERROR_013_ERROR_014(t *testing.T) {

	tu.WithDBs(t, [][][]string{common.Database("DB002")}, RegisterPKTestHandler, func(router *gin.Engine) {
		// send an other request with one column missing from def
		// //		// one PK col is missing
		param := ds.PKReadBody{
			Filters:     tu.NewFilters(t, "id", 1), // PK has two cols. should thow an exception as we have define only one col in PK
			ReadColumns: tu.NewReadColumn(t, "col_0"),
			OperationID: tu.NewOperationID(t, 64),
		}
		body, _ := json.MarshalIndent(param, "", "\t")
		url := tu.NewPKReadURL("DB002", "table_1")
		tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_013())

		// send an other request with two pk cols but wrong names
		param = ds.PKReadBody{
			Filters:     tu.NewFilters(t, "idx", 2),
			ReadColumns: tu.NewReadColumn(t, "col_0"),
			OperationID: tu.NewOperationID(t, 64),
		}
		body, _ = json.MarshalIndent(param, "", "\t")
		url = tu.NewPKReadURL("DB002", "table_1")
		tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_014())

		// no of pk cols matches but the column names are different
	})
}
