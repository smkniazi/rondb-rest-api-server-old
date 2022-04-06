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
#ifndef DATA_ACCESS_RONDB_SRC_PK_READ_PKR_RESPONSE_HPP_
#define DATA_ACCESS_RONDB_SRC_PK_READ_PKR_RESPONSE_HPP_

#include <stdint.h>
#include <cstring>
#include <string>
#include "src/rdrs-dal.h"
#include "src/status.hpp"
#include "src/error-strs.h"

class PKRResponse {
 private:
  char *respBuff;
  Uint32 capacity    = 512;  // TODO(salman) FIX ME
  Uint32 writeHeader = 0;

 public:
  explicit PKRResponse(char *respBuff);

  /**
   * Get maximum capacity of the response buffer
   *
   * @return max capacity
   */
  Uint32 GetMaxCapacity();

  /**
   * Get remaining capacity of the response buffer
   *
   * @return remaining capacity
   */
  Uint32 GetRemainingCapacity();

  /**
   * Append to response buffer
   */
  RS_Status Append_string(std::string str, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_cstring(const char *str, bool appendComma);

  /**
   * Get response buffer
   */
  char *GetResponseBuffer();

  /**
   * Get write header location
   */
  Uint32 GetWriteHeader();

  /**
   * Append to response buffer
   */
  RS_Status Append_iu32(Uint32 num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_i32(Int32 num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_i64(Int64 num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_iu64(Uint64 num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_i8(char num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_iu8(unsigned char num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_i16(Int16 num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_iu16(Uint16 num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_i24(int num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_iu24(unsigned int num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_f32(float num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status Append_d64(double num, bool appendComma);

  /**
   * Append to response buffer. Append
   */
  RS_Status Append_char(const char *from_buffer, Uint32 from_length, CHARSET_INFO *from_cs,
                        bool appendComma);

  /**
   * Append null. Used to terminate string response message
   */
  RS_Status Append_NULL();
};

#endif  // DATA_ACCESS_RONDB_SRC_PK_READ_PKR_RESPONSE_HPP_
