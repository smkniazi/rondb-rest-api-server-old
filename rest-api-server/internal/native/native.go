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

package native

/*
#cgo CFLAGS: -g -Wall
#cgo LDFLAGS: -L./../../../data-access-rondb/build/ -lrdrclient
#cgo LDFLAGS: -L/usr/local/mysql/lib -lndbclient
#include <stdlib.h>
#include "./../../../data-access-rondb/src/rdrslib.h"
#include "./../../../data-access-rondb/src/rdrs-const.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func InitRonDBConnection(connStr string) error {

	cs := C.CString(connStr)
	defer C.free(unsafe.Pointer(cs))
	ret := C.init(cs)

	if ret.ret_code != 0 {
		defer C.free(unsafe.Pointer(ret.message))
		return fmt.Errorf(C.GoString(ret.message))
	}

	return nil
}

func RonDBPKRead(request unsafe.Pointer, response unsafe.Pointer) error {
	ret := C.pkRead((*C.char)(request), (*C.char)(response))
	if ret.ret_code != 0 {
		defer C.free(unsafe.Pointer(ret.message))
		return fmt.Errorf(C.GoString(ret.message))
	}

	return nil
}
