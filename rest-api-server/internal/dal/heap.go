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
#include "./../../../data-access-rondb/src/rdrs-const.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

const BUFFER_SIZE = 512

func init() {
	if C.ADDRESS_SIZE != 4 {
		panic(fmt.Sprintf("Only 4 byte address are supported"))
	}

	if BUFFER_SIZE%C.ADDRESS_SIZE != 0 {
		panic(fmt.Sprintf("Buffer size must be multiple of %d", C.ADDRESS_SIZE))
	}
}

func GetBuffer() (unsafe.Pointer, uint32) {
	return C.malloc(C.size_t(BUFFER_SIZE)), BUFFER_SIZE
}
