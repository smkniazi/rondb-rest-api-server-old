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

PKRResponse::PKRResponse(char *respBuff) {
  this->respBuff = respBuff;
}

char *PKRResponse::getResponseBuffer() {
  return respBuff;
}

uint32_t PKRResponse::getWriteHeader() {
  return this->writeHeader;
}

void PKRResponse::setWriteHeader(uint32_t writeHeader) {
  this->writeHeader = writeHeader;
}

RS_Status PKRResponse::appendStr(string str, bool appendComma) {
  return appendCStr(str.c_str(), appendComma);
}

RS_Status PKRResponse::appendCStr(const char *str, bool appendComma) {
  int strl = strlen(str);
  if (strl + writeHeader >= capacity) {
    return RS_ERROR(SERVER_ERROR, ERROR_016);
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

RS_Status PKRResponse::append_iu32(uint32_t num, bool appendComma) {
  return append_iu64(num, appendComma);
}

RS_Status PKRResponse::append_i32(int num, bool appendComma) {
  return append_i64(num, appendComma);
}

RS_Status PKRResponse::appendNULL() {
  respBuff[writeHeader] = 0x00;
  writeHeader += 1;
  return RS_OK;
}

RS_Status PKRResponse::append_iu64(unsigned long long num, bool appendComma) {
  try {
    string numStr = to_string(num);
    appendStr(numStr, appendComma);
  } catch (...) {
    return RS_ERROR(SERVER_ERROR, ERROR_015);
  }
  return RS_OK;
}

RS_Status PKRResponse::append_i64(long long num, bool appendComma) {
  try {
    string numStr = to_string(num);
    appendStr(numStr, appendComma);
  } catch (...) {
    return RS_ERROR(SERVER_ERROR, ERROR_015);
  }
  return RS_OK;
}

