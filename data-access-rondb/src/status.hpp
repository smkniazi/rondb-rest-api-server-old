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

#ifndef DATA_ACCESS_RONDB_SRC_STATUS_HPP_
#define DATA_ACCESS_RONDB_SRC_STATUS_HPP_

#include <cstring>
#include <string>
#include <iostream>
#include <NdbApi.hpp>
#include "src/rdrs-dal.h"

/**
 * create an object of RS_Status.
 * Note it is the receiver responsibility to free the memory for msg and fileName character array
 */
inline char *__strToCharArr(std::string msg) {
  char *charArr = nullptr;
  if (!msg.empty()) {
    charArr = new char[msg.length() + 1];
    strncpy(charArr, msg.c_str(), msg.length() + 1);
  }
  return charArr;
}

inline RS_Status __RS_ERROR(const HTTP_CODE http_code, int status, int classification, int code,
                            int mysql_code, char *msg, int lineNo, char *fileName) {
  RS_Status ret = {http_code, status, classification, code, mysql_code, msg, lineNo, fileName};
  return ret;
}

inline RS_Status __RS_ERROR_RONDB(const struct NdbError &error, std::string msg, int lineNo,
                                  char *fileName) {
  std::string userMsg = "Error: " + msg + " Error: code:" + std::to_string(error.code) +
                        " MySQL Code: " + std::to_string(error.mysql_code) +
                        " Message: " + error.message;
  return __RS_ERROR(SERVER_ERROR, error.status, error.classification, error.code, error.mysql_code,
                    __strToCharArr(msg), lineNo, fileName);
}

#define __MYFILENAME__ __FILE__

#define RS_OK __RS_ERROR(SUCCESS, -1, -1, -1, -1, nullptr, 0, nullptr);
#define RS_CLIENT_ERROR(msg)                                                                       \
  __RS_ERROR(CLIENT_ERROR, -1, -1, -1, -1, __strToCharArr(msg), __LINE__,                          \
             __strToCharArr(__MYFILENAME__));
#define RS_CLIENT_404_ERROR()                                                                      \
  __RS_ERROR(NOT_FOUND, -1, -1, -1, -1, __strToCharArr("Not Found"), __LINE__,                     \
             __strToCharArr(__MYFILENAME__));
#define RS_SERVER_ERROR(msg)                                                                       \
  __RS_ERROR(SERVER_ERROR, -1, -1, -1, -1, __strToCharArr(msg), __LINE__,                          \
             __strToCharArr(__MYFILENAME__));
#define RS_RONDB_SERVER_ERROR(ndberror, msg)                                                       \
  __RS_ERROR_RONDB(ndberror, msg, __LINE__, __strToCharArr(__MYFILENAME__));

#endif  // DATA_ACCESS_RONDB_SRC_STATUS_HPP_
