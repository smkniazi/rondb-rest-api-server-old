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
#include "./../../../data-access-rondb/src/rdrs-dal.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

type Native_Buffer struct {
	Size   uint32
	Buffer unsafe.Pointer
}

const buff_size = 4 * 1024

func init() {
	if C.ADDRESS_SIZE != 4 {
		panic(fmt.Sprintf("Only 4 byte address are supported"))
	}

	if buff_size%C.ADDRESS_SIZE != 0 {
		panic(fmt.Sprintf("Buffer size must be multiple of %d", C.ADDRESS_SIZE))
	}
}

func GetBuffer() *Native_Buffer {
	buff := Native_Buffer{Buffer: C.malloc(C.size_t(buff_size)), Size: buff_size}
	dstBuf := unsafe.Slice((*byte)(buff.Buffer), buff_size)
	dstBuf[0] = 0x00 // reset buffer by putting null terminator in the begenning
	return &buff
}
