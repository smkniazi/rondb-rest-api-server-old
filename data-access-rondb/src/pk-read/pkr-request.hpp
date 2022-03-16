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
#ifndef PKR_REQUEST
#define PKR_REQUEST

#include <NdbApi.hpp>
#include <stdint.h>

class PKRRequest {
private:
  char *buffer;

  /**
   * Get offset of nth primary key/value pair
   *
   * @param n nth key/value pair
   * @return offset
   */
   uint32_t pkTupleOffset(const int n);

public:
  PKRRequest(char *request);

  /**
   * Opration type
   * @return Operation type
   */
   uint32_t operationType();

  /**
   * Get length of the data
   * @return data length
   */
   uint32_t length();

  /**
   * Get maximum capacity of the buffer
   * @return buffer capacity
   */
   uint32_t capacity();

  /**
   * Get database name
   * @return database name
   */
  const char *db();

  /**
   * Get table name
   * @return table name
   */
  const char *table();

  /**
   * Get number of PK columns
   * @return number of PK Columns
   */
   uint32_t pkColumnsCount();

  /**
   * Get PK column name
   *
   * @param n. index
   * @return PK column name
   */
  const char *pkName(uint32_t n);

  /**
   * Get PK column value
   *
   * @param n. index
   * @return PK column value
   */
  const char *pkValue(uint32_t n);

  /**
   * Get number of read columns
   * @return number of read columns
   */
   uint32_t readColumnsCount();

  /**
   * Get read column name
   *
   * @param n. index
   * @return read column name
   */
  const char *readColumnName(const uint32_t n);

  /**
   * Get operation ID
   *
   * @return operation ID
   */
  const char *operationId();
};
#endif
