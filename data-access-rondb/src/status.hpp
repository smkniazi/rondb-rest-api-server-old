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

#ifndef STATUS_H 
#define STATUS_H

#include "rdrslib.h"
#include <NdbApi.hpp>
#include <cstring>
#include <string>

/**
 * create an object of RS_Status.
 * Note it is the receiver responsibility to free the memory for msg character array
 */
inline char *__strToCharArr(std::string msg) {
  char *charArr = nullptr;
  if (!msg.empty()) {
    charArr = new char[msg.length() + 1];
    strcpy(charArr, msg.c_str());
  }
  return charArr;
}

inline RS_Status __RS_ERROR(const int rs_code, int status, int classification, int code,
                            int mysql_code, char *msg) {
  RS_Status ret = {rs_code, status, classification, code, mysql_code, msg};
  return ret;
}

inline RS_Status RS_ERROR(const int rs_code, std::string msg) {
  return __RS_ERROR(rs_code, -1, -1, -1, -1, __strToCharArr(msg));
}

inline RS_Status RS_ERROR(std::string msg) {
  return RS_ERROR(1, msg);
}

inline RS_Status RS_ERROR(const struct NdbError &error, std::string msg) {
  std::string userMsg = "Error: " + msg + " Error: code:" + std::to_string(error.code) +
                   " MySQL Code: " + std::to_string(error.mysql_code) + " Message: " + error.message;
  return __RS_ERROR(1, error.status, error.classification, error.code, error.mysql_code,
                    __strToCharArr(msg));
}

#define RS_OK RS_ERROR(0, "")

#endif
