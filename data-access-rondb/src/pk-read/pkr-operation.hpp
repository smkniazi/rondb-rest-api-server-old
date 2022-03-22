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
#ifndef PKR_OPERATION
#define PKR_OPERATION

#include "pkr-request.hpp"
#include "pkr-response.hpp"
#include "src/rdrslib.h"
#include <NdbApi.hpp>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

class PKROperation {
private:
  PKRRequest request;
  PKRResponse response;

  const NdbDictionary::Table *tableDic = nullptr;
  NdbTransaction *transaction          = nullptr;
  NdbOperation *operation              = nullptr;
  Ndb *ndbObject                       = nullptr;

  std::vector<NdbRecAttr *> recs; // records that will be read from DB
  std::unordered_map<std::string, const NdbDictionary::Column *> nonPkCols;
  std::unordered_map<std::string, const NdbDictionary::Column *> pkCols;

public:
  PKROperation(char *reqBuff, char *respBuff, Ndb *ndbObject);

  /**
   * perform the operation
   */
  RS_Status performOperation();

private:
  /**
   * start a transaction
   *
   * @param[in] ndbObject
   * @param[in] pkread
   * @param[out] table
   * @param[out] transaction
   *
   * @return status
   */
  RS_Status setupTransaction();

  /**
   * Set up read operation
   *
   * @param[in] ndbObject
   * @param[in] table
   * @param[in] transaction
   * @param[out] operation
   *
   * @return status
   */
  RS_Status setOperationPKCols(const NdbDictionary::Column *col, uint32_t colIdx);

  /**
   * setup pk read operation
   * @returns status
   */
  RS_Status setupReadOperation();

  /**
   * Set primary key column values
   * @returns status
   */
  RS_Status setOperationPKCols();

  /**
   * it stores the data read from the DB into the response buffer
   */
  RS_Status writeColToRespBuff(const NdbRecAttr *attr, bool appendComma);

  /**
   * Execute transaction
   *
   * @return status
   */
  RS_Status execute();

  /**
   * Close transaction
   */
  void closeTransaction();

  /**
   * create response
   *
   * @return status
   */
  RS_Status createResponse();

  /**
   * initialize data structures
   * @return status
   */
  RS_Status init();

  /**
   * Validate request
   * @return status
   */
  RS_Status validateRequest();

  int get_byte_array(const NdbRecAttr *attr, const char *&first_byte, int *bytes);

  int copyString(const NdbRecAttr *attr, int start);
};
#endif
