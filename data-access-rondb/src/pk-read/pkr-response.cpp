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

#include "src/pk-read/pkr-response.hpp"

#include <mysql.h>
#include <iostream>
#include <sstream>
#include "src/common/rdrs_string.hpp"

PKRResponse::PKRResponse(char *respBuff) {
  this->respBuff = respBuff;
}

char *PKRResponse::GetResponseBuffer() {
  return respBuff;
}

Uint32 PKRResponse::GetMaxCapacity() {
  return this->capacity;
}

Uint32 PKRResponse::GetRemainingCapacity() {
  return GetMaxCapacity() - GetWriteHeader();
}

Uint32 PKRResponse::GetWriteHeader() {
  return this->writeHeader;
}

RS_Status PKRResponse::Append_string(std::string str, bool appendComma) {
  return Append_cstring(str.c_str(), appendComma);
}

RS_Status PKRResponse::Append_cstring(const char *str, bool appendComma) {
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

RS_Status PKRResponse::Append_i8(char num, bool appendComma) {
  return Append_i64(num, appendComma);
}

RS_Status PKRResponse::Append_iu8(unsigned char num, bool appendComma) {
  return Append_iu64(num, appendComma);
}

RS_Status PKRResponse::Append_i16(Int16 num, bool appendComma) {
  return Append_i64(num, appendComma);
}

RS_Status PKRResponse::Append_iu16(Uint16 num, bool appendComma) {
  return Append_iu64(num, appendComma);
}

RS_Status PKRResponse::Append_i24(int num, bool appendComma) {
  return Append_i64(num, appendComma);
}

RS_Status PKRResponse::Append_iu24(Uint32 num, bool appendComma) {
  return Append_iu64(num, appendComma);
}

RS_Status PKRResponse::Append_iu32(Uint32 num, bool appendComma) {
  return Append_iu64(num, appendComma);
}

RS_Status PKRResponse::Append_i32(Int32 num, bool appendComma) {
  return Append_i64(num, appendComma);
}

RS_Status PKRResponse::Append_f32(float num, bool appendComma) {
  return Append_d64(num, appendComma);
}

RS_Status PKRResponse::Append_d64(double num, bool appendComma) {
  try {
    std::stringstream ss;
    ss << num;
    Append_string(ss.str(), appendComma);
  } catch (...) {
    return RS_SERVER_ERROR(ERROR_015);
  }
  return RS_OK;
}

RS_Status PKRResponse::Append_NULL() {
  respBuff[writeHeader] = 0x00;
  writeHeader += 1;
  return RS_OK;
}

RS_Status PKRResponse::Append_iu64(Uint64 num, bool appendComma) {
  try {
    std::string numStr = std::to_string(num);
    Append_string(numStr, appendComma);
  } catch (...) {
    return RS_SERVER_ERROR(ERROR_015);
  }
  return RS_OK;
}

RS_Status PKRResponse::Append_i64(Int64 num, bool appendComma) {
  try {
    std::string numStr = std::to_string(num);
    Append_string(numStr, appendComma);
  } catch (...) {
    return RS_SERVER_ERROR(ERROR_015);
  }
  return RS_OK;
}

RS_Status PKRResponse::Append_char(const char *fromBuff, Uint32 fromBuffLen, CHARSET_INFO *fromCS,
                                   bool appendComma) {
  int extraSpace = 3;  // +2 for quotation marks and +1 for null character
  if (appendComma) {
    extraSpace += 1;
  }

  Uint32 estimatedBytes = fromBuffLen + extraSpace;

  if (estimatedBytes > GetRemainingCapacity()) {
    return RS_SERVER_ERROR(ERROR_010 + std::string(" Response buffer remaining capacity: ") +
                           std::to_string(GetRemainingCapacity()) + std::string(" Required: ") +
                           std::to_string(estimatedBytes));
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
    return RS_SERVER_ERROR(ERROR_008 + std::string(" Invalid string: ") +
                           std::string(printable_buff));
  } else if (from_end_pos < fromBuff + fromBuffLen) {
    /*
      result is longer than UINT_MAX32 and doesn't fit into String
    */
    return RS_SERVER_ERROR(ERROR_021 + std::string(" Buffer size: ") +
                           std::to_string(estimatedBytes) + std::string(". Bytes left to copy: ") +
                           std::to_string((fromBuff + fromBuffLen) - from_end_pos));
  }
  std::string wellFormedString = std::string(tempBuff, bytesFormed);
  // remove blank spaces that are padded to the string
  size_t endpos = wellFormedString.find_last_not_of(" ");
  if (std::string::npos != endpos) {
    wellFormedString = wellFormedString.substr(0, endpos + 1);
  }

  std::string escapedstr = escape_string(wellFormedString);
  if ((escapedstr.length() + extraSpace) >= GetRemainingCapacity()) {  // +2 for quotation marks
    return RS_SERVER_ERROR(ERROR_010);
  }

  Append_string("\"", appendComma);
  Append_string(escapedstr, appendComma);
  Append_string("\"", appendComma);

  return RS_OK;
}
