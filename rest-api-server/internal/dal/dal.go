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
#include "./../../../data-access-rondb/src/rdrslib.h"
#include "./../../../data-access-rondb/src/rdrs-const.h"
*/
import "C"
import (
	"net/http"
	"unsafe"
)

type DalError struct {
	HttpCode int
	Message  string
}

func (e *DalError) Error() string {
	return e.Message
}

func InitRonDBConnection(connStr string) *DalError {

	cs := C.CString(connStr)
	defer C.free(unsafe.Pointer(cs))
	ret := C.init(cs)

	if ret.http_code != http.StatusOK {
		defer C.free(unsafe.Pointer(ret.message))
		return &DalError{HttpCode: int(ret.http_code), Message: C.GoString(ret.message)}
	}

	return nil
}

func ShutdownConnection() *DalError {
	ret := C.shutdown()

	if ret.http_code != http.StatusOK {
		defer C.free(unsafe.Pointer(ret.message))
		return &DalError{HttpCode: int(ret.http_code), Message: C.GoString(ret.message)}
	}
	return nil
}

func RonDBPKRead(request unsafe.Pointer, response unsafe.Pointer) *DalError {
	ret := C.pkRead((*C.char)(request), (*C.char)(response))
	if ret.http_code != http.StatusOK {
		defer C.free(unsafe.Pointer(ret.message))
		return &DalError{HttpCode: int(ret.http_code), Message: C.GoString(ret.message)}
	}

	return nil
}
