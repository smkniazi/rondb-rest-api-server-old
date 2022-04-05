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

#include "pkr-response.hpp"
#include "src/common/rdrs_string.hpp"
#include <iostream>
#include <mysql.h>
#include <sstream>

PKRResponse::PKRResponse(char *respBuff) {
  this->respBuff = respBuff;
}

char *PKRResponse::getResponseBuffer() {
  return respBuff;
}

uint32_t PKRResponse::getMaxCapacity() {
  return this->capacity;
}

uint32_t PKRResponse::getRemainingCapacity() {
  return getMaxCapacity() - getWriteHeader();
}

uint32_t PKRResponse::getWriteHeader() {
  return this->writeHeader;
}

RS_Status PKRResponse::append_string(string str, bool appendComma) {
  return append_cstring(str.c_str(), appendComma);
}

RS_Status PKRResponse::append_cstring(const char *str, bool appendComma) {
  int strl = strlen(str);
  if (strl + writeHeader >= capacity) {
    return RS_SERVER_ERROR(ERROR_016);
  }

  std::memcpy(respBuff + writeHeader, str, strl);
  writeHeader += strl;

  if (appendComma) {
    respBuff[writeHeader] = ',';
    writeHeader += 1;
  }

  return RS_OK;
}

RS_Status PKRResponse::append_i8(char num, bool appendComma) {
  return append_i64(num, appendComma);
}

RS_Status PKRResponse::append_iu8(unsigned char num, bool appendComma) {
  return append_iu64(num, appendComma);
}

RS_Status PKRResponse::append_i16(short int num, bool appendComma) {
  return append_i64(num, appendComma);
}

RS_Status PKRResponse::append_iu16(unsigned short int num, bool appendComma) {
  return append_iu64(num, appendComma);
}

RS_Status PKRResponse::append_i24(int num, bool appendComma) {
  return append_i64(num, appendComma);
}

RS_Status PKRResponse::append_iu24(unsigned int num, bool appendComma) {
  return append_iu64(num, appendComma);
}

RS_Status PKRResponse::append_iu32(uint32_t num, bool appendComma) {
  return append_iu64(num, appendComma);
}

RS_Status PKRResponse::append_i32(int num, bool appendComma) {
  return append_i64(num, appendComma);
}

RS_Status PKRResponse::append_f32(float num, bool appendComma) {
  return append_d64(num, appendComma);
}

RS_Status PKRResponse::append_d64(double num, bool appendComma) {
  try {
    stringstream ss;
    ss << num;
    append_string(ss.str(), appendComma);
  } catch (...) {
    return RS_SERVER_ERROR(ERROR_015);
  }
  return RS_OK;
}

RS_Status PKRResponse::appendNULL() {
  respBuff[writeHeader] = 0x00;
  writeHeader += 1;
  return RS_OK;
}

RS_Status PKRResponse::append_iu64(unsigned long long num, bool appendComma) {
  try {
    string numStr = to_string(num);
    append_string(numStr, appendComma);
  } catch (...) {
    return RS_SERVER_ERROR(ERROR_015);
  }
  return RS_OK;
}

RS_Status PKRResponse::append_i64(long long num, bool appendComma) {
  try {
    string numStr = to_string(num);
    append_string(numStr, appendComma);
  } catch (...) {
    return RS_SERVER_ERROR(ERROR_015);
  }
  return RS_OK;
}

RS_Status PKRResponse::append_char(const char *fromBuff, uint32_t fromBuffLen, CHARSET_INFO *fromCS,
                                   bool appendComma) {

  int extraSpace = 3; // +2 for quotation marks and +1 for null character
  if (appendComma) {
    extraSpace += 1;
  }

  uint32_t estimatedBytes = fromBuffLen + extraSpace;

  if (estimatedBytes > getRemainingCapacity()) {
    return RS_SERVER_ERROR(ERROR_010 + string(" Response buffer remaining capacity: ") +
                                      to_string(getRemainingCapacity()) + string(" Required: ") +
                                      to_string(estimatedBytes));
  }

  // from_buffer -> printable string  -> escaped string
  char tempBuff[estimatedBytes];
  const char *well_formed_error_pos;
  const char *cannot_convert_error_pos;
  const char *from_end_pos;
  const char *error_pos;

  /* convert_to_printable(tempBuff, tempBuffLen, fromBuffer, fromLength, fromCS, 0); */
  int bytesFormed = well_formed_copy_nchars(fromCS, tempBuff, estimatedBytes, fromCS, fromBuff,
                                            fromBuffLen, UINT32_MAX, &well_formed_error_pos,
                                            &cannot_convert_error_pos, &from_end_pos);

  error_pos = well_formed_error_pos ? well_formed_error_pos : cannot_convert_error_pos;
  if (error_pos) {
    char printable_buff[32];
    convert_to_printable(printable_buff, sizeof(printable_buff), error_pos,
                         fromBuff + fromBuffLen - error_pos, fromCS, 6);
    return RS_SERVER_ERROR(ERROR_008 + string(" Invalid string: ") + string(printable_buff));
  } else if (from_end_pos < fromBuff + fromBuffLen) {
    /*
      result is longer than UINT_MAX32 and doesn't fit into String
    */
    return RS_SERVER_ERROR(ERROR_021 + string(" Buffer size: ") + to_string(estimatedBytes) +
                                      string(". Bytes left to copy: ") +
                                      to_string((fromBuff + fromBuffLen) - from_end_pos));
  }
  string wellFormedString = string(tempBuff, bytesFormed);
  // remove blank spaces that are padded to the string
  size_t endpos = wellFormedString.find_last_not_of(" ");
  if (string::npos != endpos) {
    wellFormedString = wellFormedString.substr(0, endpos + 1);
  }

  string escapedstr = escape_string(wellFormedString);
  if ((escapedstr.length() + extraSpace) >= getRemainingCapacity()) { // +2 for quotation marks
    return RS_SERVER_ERROR(ERROR_010);
  }

  append_string("\"", appendComma);
  append_string(escapedstr, appendComma);
  append_string("\"", appendComma);

  return RS_OK;
}
