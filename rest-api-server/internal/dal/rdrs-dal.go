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

package dal

/*
#cgo CFLAGS: -g -Wall
#cgo LDFLAGS: -L./../../../data-access-rondb/build/ -lrdrclient
#cgo LDFLAGS: -L/usr/local/mysql/lib -lndbclient
#cgo LDFLAGS: -L/usr/local/mysql/lib -lrdrs_string
#include <stdlib.h>
#include <stdbool.h>
#include "./../../../data-access-rondb/src/rdrs-dal.h"
#include "./../../../data-access-rondb/src/rdrs-const.h"
#include "./../../../data-access-rondb/src/error-strs.h"
*/
import "C"
import (
	"net/http"
	"unsafe"
)

type DalError struct {
	HttpCode    int
	Message     string
	ErrLineNo   int
	ErrFileName string
}

func (e *DalError) Error() string {
	return e.Message
}

func InitRonDBConnection(connStr string, find_available_node_id bool) *DalError {

	cs := C.CString(connStr)
	defer C.free(unsafe.Pointer(cs))
	ret := C.Init(cs, C.bool(find_available_node_id))
	defer cleanUp(&ret)

	if ret.http_code != http.StatusOK {
		return &DalError{HttpCode: int(ret.http_code), Message: C.GoString(ret.message),
			ErrLineNo: int(ret.err_line_no), ErrFileName: C.GoString(ret.err_file_name)}
	}

	return nil
}

func ShutdownConnection() *DalError {
	ret := C.Shutdown()
	defer cleanUp(&ret)

	if ret.http_code != http.StatusOK {
		return &DalError{HttpCode: int(ret.http_code), Message: C.GoString(ret.message),
			ErrLineNo: int(ret.err_line_no), ErrFileName: C.GoString(ret.err_file_name)}
	}
	return nil
}

func RonDBPKRead(request *Native_Buffer, response *Native_Buffer) *DalError {
	// unsafe.Pointer
	// create C structs for  buffers
	var crequest C.RS_Buffer
	var cresponse C.RS_Buffer
	crequest.buffer = (*C.char)(request.Buffer)
	crequest.size = C.uint(request.Size)

	cresponse.buffer = (*C.char)(response.Buffer)
	cresponse.size = C.uint(response.Size)

	ret := C.PKRead(&crequest, &cresponse)

	defer cleanUp(&ret)
	if ret.http_code != http.StatusOK {
		return &DalError{HttpCode: int(ret.http_code), Message: C.GoString(ret.message),
			ErrLineNo: int(ret.err_line_no), ErrFileName: C.GoString(ret.err_file_name)}
	}

	return nil
}

func RonDBBatchedPKRead(noOps uint32, requests []*Native_Buffer, responses []*Native_Buffer) *DalError {
	cReqs := C.AllocRSBufferArray(C.uint(noOps))
	cResps := C.AllocRSBufferArray(C.uint(noOps))
	defer C.FreeRSBufferArray(cReqs)
	defer C.FreeRSBufferArray(cResps)

	scReqs := unsafe.Slice((*C.pRS_Buffer)(cReqs), noOps)
	scResps := unsafe.Slice((*C.pRS_Buffer)(cResps), noOps)

	for i := 0; i < int(noOps); i++ {
		var crequest C.RS_Buffer
		var cresponse C.RS_Buffer
		crequest.buffer = (*C.char)(requests[i].Buffer)
		crequest.size = C.uint(requests[i].Size)
		scReqs[i] = &crequest

		cresponse.buffer = (*C.char)(responses[i].Buffer)
		cresponse.size = C.uint(responses[i].Size)
		scResps[i] = &cresponse
	}

	ret := C.PKBatchRead(C.uint(noOps), (*C.pRS_Buffer)(cReqs), (*C.pRS_Buffer)(cResps))
	defer cleanUp(&ret)
	if ret.http_code != http.StatusOK {
		return &DalError{HttpCode: int(ret.http_code), Message: C.GoString(ret.message),
			ErrLineNo: int(ret.err_line_no), ErrFileName: C.GoString(ret.err_file_name)}
	}

	return nil
}

func cleanUp(ret *C.RS_Status) {
	if ret.message != nil {
		defer C.free(unsafe.Pointer(ret.message))
	}

	if ret.err_file_name != nil {
		defer C.free(unsafe.Pointer(ret.err_file_name))
	}
}
