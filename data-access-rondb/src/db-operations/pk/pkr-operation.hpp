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
#ifndef DATA_ACCESS_RONDB_SRC_PK_READ_PKR_OPERATION_HPP_
#define DATA_ACCESS_RONDB_SRC_PK_READ_PKR_OPERATION_HPP_

#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <NdbApi.hpp>
#include "src/db-operations/pk/pkr-request.hpp"
#include "src/db-operations/pk/pkr-response.hpp"
#include "src/rdrs-dal.h"

class PKROperation {
 private:
  PKRRequest request;
  PKRResponse response;

  const NdbDictionary::Table *table_dic = nullptr;
  NdbTransaction *transaction           = nullptr;
  NdbOperation *operation               = nullptr;
  Ndb *ndb_object                       = nullptr;

  std::vector<NdbRecAttr *> recs;  // records that will be read from DB
  std::unordered_map<std::string, const NdbDictionary::Column *> non_pk_cols;
  std::unordered_map<std::string, const NdbDictionary::Column *> pk_cols;

 public:
  PKROperation(RS_Buffer *req_buff, RS_Buffer *resp_buff, Ndb *ndb_object);

  /**
   * perform the operation
   */
  RS_Status PerformOperation();

 private:
  /**
   * start a transaction
   *
   * @return status
   */
  RS_Status SetupTransaction();

  /**
   * setup pk read operation
   * @returns status
   */
  RS_Status SetupReadOperation();

  /**
   * Set primary key column values
   * @returns status
   */
  RS_Status SetOperationPKCols();

  /**
   * Execute transaction
   *
   * @return status
   */
  RS_Status Execute();

  /**
   * Close transaction
   */
  void CloseTransaction();

  /**
   * abort operation
   */
  RS_Status Abort();

  /**
   * create response
   *
   * @return status
   */
  RS_Status CreateResponse();

  /**
   * initialize data structures
   * @return status
   */
  RS_Status Init();

  /**
   * Validate request
   * @return status
   */
  RS_Status ValidateRequest();

};
#endif  // DATA_ACCESS_RONDB_SRC_PK_READ_PKR_OPERATION_HPP_