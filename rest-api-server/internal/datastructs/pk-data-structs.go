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
package datastructs

import "encoding/json"

const PK_DB_OPERATION = "pk-read"
const PK_HTTP_VERB = "POST"

// Primary key column filter
const FILTER_PARAM_NAME = "filters"
const READ_COL_PARAM_NAME = "read-columns"
const OPERATION_ID_PARAM_NAME = "operation-id"

type PKReadParams struct {
	DB          *string       `json:"db" `
	Table       *string       `json:"table"`
	Filters     *[]Filter     `json:"filters"`
	ReadColumns *[]ReadColumn `json:"readColumns"`
	OperationID *string       `json:"operationId"`
}

// Path parameters
type PKReadPP struct {
	DB    *string `json:"db" uri:"db"  binding:"required,min=1,max=64"`
	Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"`
}

type PKReadBody struct {
	Filters     *[]Filter     `json:"filters"         form:"filters"         binding:"required,min=1,max=4096,dive"`
	ReadColumns *[]ReadColumn `json:"readColumns"    form:"read-columns"    binding:"omitempty,min=1,max=4096,unique"`
	OperationID *string       `json:"operationId"    form:"operation-id"    binding:"omitempty,min=1,max=64"`
}

type Filter struct {
	Column *string          `json:"column"   form:"column"   binding:"required,min=1,max=64"`
	Value  *json.RawMessage `json:"value"    form:"value"    binding:"required"`
}

const (
	DRT_DEFAULT = "default"
	DRT_BASE64  = "base64" // not implemented yet
	DRT_HEX     = "hex"    // not implemented yet
)

type ReadColumn struct {
	Column *string `json:"column"    form:"column"    binding:"required,min=1,max=64"`

	// Data return type you can change the return type for the column data
	// int/floats/decimal are returned as JSON Number type (default),
	// varchar/char are returned as strings (default) and varbinary as base64 (default)
	// Right now only default return type is supported
	DataReturnType *string `json:"dataReturnType"    form:"column"    binding:"Enum=default,min=1,max=64"`

	// more parameter can be added later.
}

type PKTestInfo struct {
	PkReq        PKReadBody
	Table        string
	Db           string
	HttpCode     int
	BodyContains string
	RespKVs      []string
}
