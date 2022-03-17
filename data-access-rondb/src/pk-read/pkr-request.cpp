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

#include "pkr-request.hpp"
#include "src/logger.hpp"
#include "src/rdrs-const.h"
#include "src/status.hpp"

PKRRequest::PKRRequest(char *request) {
  this->buffer = request;
}

uint32_t PKRRequest::operationType() {
  return ((uint32_t *)buffer)[PKR_OP_TYPE_IDX];
}

uint32_t PKRRequest::length() {
  return ((uint32_t *)buffer)[PKR_LENGTH_IDX];
}

uint32_t PKRRequest::capacity() {
  return ((uint32_t *)buffer)[PKR_CAPACITY_IDX];
}

const char *PKRRequest::db() {
  uint32_t dbOffset = ((uint32_t *)buffer)[PKR_DB_IDX];
  return buffer + dbOffset;
}

const char *PKRRequest::table() {
  uint32_t tableOffset = ((uint32_t *)buffer)[PKR_TABLE_IDX];
  return buffer + tableOffset;
}

uint32_t PKRRequest::pkColumnsCount() {
  uint32_t offset = ((uint32_t *)buffer)[PKR_PK_COLS_IDX];
  uint32_t count = ((uint32_t *)buffer)[offset / sizeof(uint32_t)];
  return count;
}

uint32_t PKRRequest::pkTupleOffset(const int n) {
  //[count][kv offset1]...[kv offset n][k offset][v offset] [ bytes ... ] [koffset][v offset]...
  //                                      ^
  //          ............................|                                 ^
  //                         ...............................................|
  //

  uint32_t offset = ((uint32_t *)buffer)[PKR_PK_COLS_IDX];
  uint32_t kvOffset = ((uint32_t *)buffer)[(offset / sizeof(uint32_t)) + 1 + n]; // +1 for count
  return kvOffset;
}

const char *PKRRequest::pkName(uint32_t index) {
  uint32_t kvOffset = pkTupleOffset(index);
  uint32_t kOffset = ((uint32_t *)buffer)[kvOffset / 4];
  return buffer + kOffset;
}

const char *PKRRequest::pkValueCStr(uint32_t index) {
  uint32_t kvOffset = pkTupleOffset(index);
  uint32_t vOffset = ((uint32_t *)buffer)[(kvOffset / 4) + 1];

  return buffer + vOffset + 2; // skip first 2 bytes that contain size of string
}

int PKRRequest::pkValueNDBStr(uint32_t index, const NdbDictionary::Column *col, char **data) {

  uint32_t kvOffset = pkTupleOffset(index);
  uint32_t vOffset = ((uint32_t *)buffer)[(kvOffset / 4) + 1];
  char *data_start = buffer + vOffset;

  // The Go layer sets the length of the string in the first two bytes of the string
  const NdbDictionary::Column::ArrayType array_type = col->getArrayType();
  const size_t max_size = col->getSizeInBytes();
  const size_t user_size = data_start[1] * 256 + data_start[0];

  if (user_size > max_size) {
    *data = NULL;
    return -1;
  }

  switch (array_type) {
  case NdbDictionary::Column::ArrayTypeFixed:
    // No prefix length is stored in string
    *data = data_start + 2; // skip the first two bytes that contain the length of the string
    return 0;
  case NdbDictionary::Column::ArrayTypeShortVar:
    data_start[1] = data_start[0];
    *data = data_start + 1;
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

uint32_t PKRRequest::readColumnsCount() {
  uint32_t offset = ((uint32_t *)buffer)[PKR_READ_COLS_IDX];
  if (offset == 0) {
    return 0;
  } else {
    uint32_t count = ((uint32_t *)buffer)[offset / sizeof(uint32_t)];
    return count;
  }
}

const char *PKRRequest::readColumnName(const uint32_t n) {
  //[count][rc offset1]...[rc offset n] [ bytes ... ] [ bytes ... ]
  //                                      ^
  //          ............................|                ^
  //                         ..............................|
  //

  uint32_t offset = ((uint32_t *)buffer)[PKR_READ_COLS_IDX];
  uint32_t rOffset = ((uint32_t *)buffer)[(offset / sizeof(uint32_t)) + 1 + n]; // +1 for count
  return buffer + rOffset;
}

const char *PKRRequest::operationId() {
  uint32_t offset = ((uint32_t *)buffer)[PKR_OP_ID_IDX];
  if (offset != 0) {
    return buffer + offset;
  } else {
    return NULL;
  }
}
