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
#ifndef PKR_RESPONSE
#define PKR_RESPONSE

#include <stdint.h>
#include <cstring>
#include <string>
#include "src/rdrslib.h"
#include "src/status.hpp"
#include "src/error-strs.h"

using namespace std;

class PKRResponse {

private:
  char *respBuff;
  uint32_t capacity = 512; //TODO FIX ME
  uint32_t writeHeader = 0;

public:
  /**
   * Append to response buffer
   */
  RS_Status append(string str, bool appendComma); 

  /**
   * Append to response buffer
   */
  RS_Status append(const char* str, bool appendComma); 

  PKRResponse(char *respBuff);

  char *getResponseBuffer();

  /**
   * Get write header location
   */
  uint32_t getWriteHeader();

  /**
   * Set write header location
   */
  void setWriteHeader(uint32_t writeHeader);

  /**
   * Append to response buffer
   */
  RS_Status append(uint32_t num, bool appendComma);

  /**
   * Append to response buffer
   */
  RS_Status append(int num, bool appendComma);

  /**
   * Append null. Used to terminate string response message
   */
  RS_Status appendNULL();
};

#endif
