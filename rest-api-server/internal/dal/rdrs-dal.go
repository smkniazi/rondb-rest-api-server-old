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

type RonDBStats struct {
	NdbObjectsCreationCount uint64
	NdbObjectsDeletionCount uint64
	NdbObjectsTotalCount    uint64
	NdbObjectsFreeCount     uint64
}

func InitRonDBConnection(connStr string, find_available_node_id bool) *DalError {

	cs := C.CString(connStr)
	defer C.free(unsafe.Pointer(cs))
	ret := C.Init(cs, C.bool(find_available_node_id))

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	return nil
}

func ShutdownConnection() *DalError {
	ret := C.Shutdown()

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}
	return nil
}

func RonDBPKRead(request *NativeBuffer, response *NativeBuffer) *DalError {
	// unsafe.Pointer
	// create C structs for  buffers
	var crequest C.RS_Buffer
	var cresponse C.RS_Buffer
	crequest.buffer = (*C.char)(request.Buffer)
	crequest.size = C.uint(request.Size)

	cresponse.buffer = (*C.char)(response.Buffer)
	cresponse.size = C.uint(response.Size)

	ret := C.PKRead(&crequest, &cresponse)

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	return nil
}

func RonDBBatchedPKRead(noOps uint32, requests []*NativeBuffer, responses []*NativeBuffer) *DalError {

	cReqs := C.AllocRSBufferArray(C.uint(noOps))
	cResps := C.AllocRSBufferArray(C.uint(noOps))
	defer C.FreeRSBufferArray(cReqs)
	defer C.FreeRSBufferArray(cResps)

	scReqs := unsafe.Slice((*C.pRS_Buffer)(cReqs), noOps)
	scResps := unsafe.Slice((*C.pRS_Buffer)(cResps), noOps)

	for i := 0; i < int(noOps); i++ {
		var crequest C.RS_Buffer
		crequest.buffer = (*C.char)(requests[i].Buffer)
		crequest.size = C.uint(requests[i].Size)
		scReqs[i] = &crequest

		var cresponse C.RS_Buffer
		cresponse.buffer = (*C.char)(responses[i].Buffer)
		cresponse.size = C.uint(responses[i].Size)
		scResps[i] = &cresponse
	}

	ret := C.PKBatchRead(C.uint(noOps), (*C.pRS_Buffer)(cReqs), (*C.pRS_Buffer)(cResps))

	for i := 0; i < int(noOps); i++ {
		scReqs[i] = nil
		scResps[i] = nil
	}

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	return nil
}

func cToGoRet(ret *C.RS_Status) *DalError {
	return &DalError{HttpCode: int(ret.http_code), Message: C.GoString(&ret.message[0]),
		ErrLineNo: int(ret.err_line_no), ErrFileName: C.GoString(&ret.err_file_name[0])}
}

func GetRonDBStats() (*RonDBStats, *DalError) {

	p := (*C.RonDB_Stats)(C.malloc(C.size_t(unsafe.Sizeof(C.sizeof_RonDB_Stats))))
	defer C.free(unsafe.Pointer(p))

	ret := C.GetRonDBStats(p)

	if ret.http_code != http.StatusOK {
		return nil, cToGoRet(&ret)
	}
	var rstats RonDBStats
	rstats.NdbObjectsCreationCount = uint64(p.ndb_objects_created)
	rstats.NdbObjectsDeletionCount = uint64(p.ndb_objects_deleted)
	rstats.NdbObjectsTotalCount = uint64(p.ndb_objects_count)
	rstats.NdbObjectsFreeCount = uint64(p.ndb_objects_available)

	return &rstats, nil
}
