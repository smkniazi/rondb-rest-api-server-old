/*
 * Copyright (C) 2022 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#include "src/pk-read/pkr-request.hpp"
#include "src/logger.hpp"
#include "src/rdrs-const.h"
#include "src/status.hpp"

PKRRequest::PKRRequest(char *request) {
  this->buffer = request;
}

Uint32 PKRRequest::OperationType() {
  return (reinterpret_cast<Uint32 *>(buffer))[PKR_OP_TYPE_IDX];
}

Uint32 PKRRequest::Length() {
  return (reinterpret_cast<Uint32 *>(buffer))[PKR_LENGTH_IDX];
}

Uint32 PKRRequest::Capacity() {
  return (reinterpret_cast<Uint32 *>(buffer))[PKR_CAPACITY_IDX];
}

const char *PKRRequest::DB() {
  Uint32 dbOffset = (reinterpret_cast<Uint32 *>(buffer))[PKR_DB_IDX];
  return buffer + dbOffset;
}

const char *PKRRequest::Table() {
  Uint32 tableOffset = (reinterpret_cast<Uint32 *>(buffer))[PKR_TABLE_IDX];
  return buffer + tableOffset;
}

Uint32 PKRRequest::PKColumnsCount() {
  Uint32 offset = (reinterpret_cast<Uint32 *>(buffer))[PKR_PK_COLS_IDX];
  Uint32 count  = (reinterpret_cast<Uint32 *>(buffer))[offset / sizeof(Uint32)];
  return count;
}

Uint32 PKRRequest::PKTupleOffset(const int n) {
  // [count][kv offset1]...[kv offset n][k offset][v offset] [ bytes ... ] [koffset][v offset]...
  //                                      ^
  //          ............................|                                 ^
  //                         ...............................................|
  //

  Uint32 offset = (reinterpret_cast<Uint32 *>(buffer))[PKR_PK_COLS_IDX];
  Uint32 kvOffset =
      (reinterpret_cast<Uint32 *>(buffer))[(offset / sizeof(Uint32)) + 1 + n];  // +1 for count
  return kvOffset;
}

const char *PKRRequest::PKName(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 kOffset  = (reinterpret_cast<Uint32 *>(buffer))[kvOffset / 4];
  return buffer + kOffset;
}

const char *PKRRequest::PKValueCStr(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 vOffset  = (reinterpret_cast<Uint32 *>(buffer))[(kvOffset / 4) + 1];

  return buffer + vOffset + 2;  // skip first 2 bytes that contain size of string
}

int PKRRequest::PKValueNDBStr(Uint32 index, const NdbDictionary::Column *col, char **data) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 vOffset  = (reinterpret_cast<Uint32 *>(buffer))[(kvOffset / 4) + 1];
  char *data_start  = buffer + vOffset;

  // The Go layer sets the length of the string in the first two bytes of the string
  const NdbDictionary::Column::ArrayType array_type = col->getArrayType();
  const size_t max_size                             = col->getSizeInBytes();
  const size_t user_size                            = data_start[1] * 256 + data_start[0];

  if (user_size > max_size) {
    *data = NULL;
    return -1;
  }

  switch (array_type) {
  case NdbDictionary::Column::ArrayTypeFixed:
    // No prefix length is stored in string
    *data = data_start + 2;  // skip the first two bytes that contain the length of the string
    return 0;
  case NdbDictionary::Column::ArrayTypeShortVar:
    data_start[1] = data_start[0];
    *data         = data_start + 1;
    return 0;
  case NdbDictionary::Column::ArrayTypeMediumVar:
    // First two bytes of str has the length of data stored

    // the length of the string is already set in the first two bytes of the string
    *data = data_start;
    return 0;
  default:
    *data = NULL;
    return -1;
  }
}

Uint32 PKRRequest::ReadColumnsCount() {
  Uint32 offset = (reinterpret_cast<Uint32 *>(buffer))[PKR_READ_COLS_IDX];
  if (offset == 0) {
    return 0;
  } else {
    Uint32 count = (reinterpret_cast<Uint32 *>(buffer))[offset / sizeof(Uint32)];
    return count;
  }
}

const char *PKRRequest::ReadColumnName(const Uint32 n) {
  // [count][rc offset1]...[rc offset n] [ bytes ... ] [ bytes ... ]
  //                                      ^
  //          ............................|                ^
  //                         ..............................|
  //

  Uint32 offset = (reinterpret_cast<Uint32 *>(buffer))[PKR_READ_COLS_IDX];
  Uint32 rOffset =
      (reinterpret_cast<Uint32 *>(buffer))[(offset / sizeof(Uint32)) + 1 + n];  // +1 for count
  return buffer + rOffset;
}

const char *PKRRequest::OperationId() {
  Uint32 offset = (reinterpret_cast<Uint32 *>(buffer))[PKR_OP_ID_IDX];
  if (offset != 0) {
    return buffer + offset;
  } else {
    return NULL;
  }
}
