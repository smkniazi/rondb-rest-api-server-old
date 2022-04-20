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

#include "src/db-operations/pk/pkr-operation.hpp"
#include <mysql_time.h>
#include <algorithm>
#include <utility>
#include <NdbDictionary.hpp>
#include "src/db-operations/pk/pkr-request.hpp"
#include "src/db-operations/pk/pkr-response.hpp"
#include "src/db-operations/pk/common.hpp"
#include "src/rondb-lib/decimal_utils.hpp"
#include "src/error-strs.h"
#include "src/logger.hpp"
#include "src/rdrs-const.h"
#include "src/status.hpp"
#include "src/rondb-lib/rdrs_date.hpp"
#include "src/mystring.hpp"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/beast/core/detail/base64.hpp>

PKROperation::PKROperation(RS_Buffer *reqBuff, RS_Buffer *respBuff, Ndb *ndbObject)
    : request(reqBuff), response(respBuff) {
  this->ndb_object = ndbObject;
}

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

RS_Status PKROperation::SetupTransaction() {
  transaction = ndb_object->startTransaction(table_dic);
  if (transaction == nullptr) {
    return RS_RONDB_SERVER_ERROR(ndb_object->getNdbError(), ERROR_005);
  }
  return RS_OK;
}

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
RS_Status PKROperation::SetupReadOperation() {
  if (operation != nullptr) {
    return RS_CLIENT_ERROR(ERROR_006);
  }

  operation = transaction->getNdbOperation(table_dic);
  if (operation == nullptr) {
    return RS_RONDB_SERVER_ERROR(transaction->getNdbError(), ERROR_007);
  }

  if (operation->readTuple(NdbOperation::LM_CommittedRead) != 0) {
    return RS_SERVER_ERROR(ERROR_022)
  }

  for (Uint32 i = 0; i < request.PKColumnsCount(); i++) {
    RS_Status status = SetOperationPKCol(table_dic->getColumn(request.PKName(i)), operation, &request, i);
    if (status.http_code != SUCCESS) {
      return status;
    }
  }

  if (request.ReadColumnsCount() > 0) {
    for (Uint32 i = 0; i < request.ReadColumnsCount(); i++) {
      NdbRecAttr *rec = operation->getValue(request.ReadColumnName(i), nullptr);
      recs.insert(recs.begin(), rec);
    }
  } else {
    std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator it =
        non_pk_cols.begin();
    while (it != non_pk_cols.end()) {
      NdbRecAttr *rec = operation->getValue(it->first.c_str(), nullptr);
      it++;
      recs.insert(recs.begin(), rec);
    }
  }

  return RS_OK;
}

RS_Status PKROperation::Execute() {
  if (transaction->execute(NdbTransaction::Commit) != 0) {
    return RS_RONDB_SERVER_ERROR(transaction->getNdbError(), ERROR_009);
  }

  return RS_OK;
}

RS_Status PKROperation::CreateResponse() {
  if (transaction->getNdbError().classification == NdbError::NoDataFound) {
    return RS_CLIENT_404_ERROR();
  } else {
    // iterate over all columns
    response.Append_string("{", false, false);
    if (request.OperationId() != nullptr) {
      response.Append_string("\"operationId\": ", false, false);
      response.Append_string(std::string("\"") + request.OperationId() + std::string("\""), false,
                             true);
    }
    response.Append_string("\"Data\": {", false, false);

    for (Uint32 i = 0; i < recs.size(); i++) {
      RS_Status status = response.Append_string(
          std::string("\"") + recs[i]->getColumn()->getName() + std::string("\":"), false, false);
      if (status.http_code != SUCCESS) {
        return status;
      }

      status = WriteColToRespBuff(recs[i], &response, i == (recs.size() - 1) ? false : true);
      if (status.http_code != SUCCESS) {
        return status;
      }
    }
    response.Append_string("} } ", false, false);
    response.Append_NULL();
    return RS_OK;
  }
}


RS_Status PKROperation::Init() {
  if (table_dic == nullptr) {
    if (ndb_object->setCatalogName(request.DB()) != 0) {
      return RS_CLIENT_ERROR(ERROR_011 + std::string(" Database: ") + std::string(request.DB()) +
                             " Table: " + request.Table());
    }
    const NdbDictionary::Dictionary *dict = ndb_object->getDictionary();
    table_dic                             = dict->getTable(request.Table());

    if (table_dic == nullptr) {
      return RS_CLIENT_ERROR(ERROR_011 + std::string(" Database: ") + std::string(request.DB()) +
                             " Table: " + request.Table());
    }
  }

  // get all primary key columnns
  for (int i = 0; i < table_dic->getNoOfPrimaryKeys(); i++) {
    const char *priName           = table_dic->getPrimaryKey(i);
    pk_cols[std::string(priName)] = table_dic->getColumn(priName);
  }

  // get all non primary key columnns
  for (int i = 0; i < table_dic->getNoOfColumns(); i++) {
    const NdbDictionary::Column *col = table_dic->getColumn(i);
    std::string colNameStr(col->getName());
    std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
        pk_cols.find(colNameStr);
    if (got == pk_cols.end()) {  // not found
      non_pk_cols[std::string(col->getName())] = table_dic->getColumn(col->getName());
    }
  }

  return RS_OK;
}

RS_Status PKROperation::ValidateRequest() {
  // Check primary key columns
  if (request.PKColumnsCount() != pk_cols.size()) {
    return RS_CLIENT_ERROR(ERROR_013 + std::string(" Expecting: ") +
                           std::to_string(pk_cols.size()) +
                           " Got: " + std::to_string(request.PKColumnsCount()));
  }

  for (Uint32 i = 0; i < request.PKColumnsCount(); i++) {
    std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
        pk_cols.find(std::string(request.PKName(i)));
    if (got == pk_cols.end()) {  // not found
      return RS_CLIENT_ERROR(ERROR_014 + std::string(" Column: ") + std::string(request.PKName(i)));
    }
  }

  // Check non primary key columns
  // check that all columns exist
  // check that data return type is supported
  // check for reading blob columns
  if (request.ReadColumnsCount() > 0) {
    for (Uint32 i = 0; i < request.ReadColumnsCount(); i++) {
      std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
          non_pk_cols.find(std::string(request.ReadColumnName(i)));
      if (got == non_pk_cols.end()) {  // not found
        return RS_CLIENT_ERROR(ERROR_012 + std::string(" Column: ") +
                               std::string(request.ReadColumnName(i)));
      }

      // check that the data return type is supported
      // for now we only support DataReturnType.DEFAULT
      if (request.ReadColumnReturnType(i) > __MAX_TYPE_NOT_A_DRT ||
          DEFAULT_DRT != request.ReadColumnReturnType(i)) {
        return RS_SERVER_ERROR(ERROR_025 + std::string(" Column: ") +
                               std::string(request.ReadColumnName(i)));
      }

      if (table_dic->getColumn(request.ReadColumnName(i))->getType() ==
              NdbDictionary::Column::Blob ||
          table_dic->getColumn(request.ReadColumnName(i))->getType() ==
              NdbDictionary::Column::Text) {
        return RS_SERVER_ERROR(ERROR_026 + std::string(" Column: ") +
                               std::string(request.ReadColumnName(i)));
      }
    }
  } else {
    // user wants to read all columns. make sure that we are not reading Blobs
    std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator it =
        non_pk_cols.begin();
    while (it != non_pk_cols.end()) {
      NdbDictionary::Column::Type type = it->second->getType();
      std::cout << "here 2 --" << std::endl;
      if (type == NdbDictionary::Column::Blob || type == NdbDictionary::Column::Text) {
        return RS_SERVER_ERROR(ERROR_026 + std::string(" Column: ") + it->first);
      }
      it++;
    }
  }

  return RS_OK;
}

void PKROperation::CloseTransaction() {
  ndb_object->closeTransaction(transaction);
}

RS_Status PKROperation::PerformOperation() {
  RS_Status status = Init();
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = ValidateRequest();
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = SetupTransaction();
  if (status.http_code != SUCCESS) {
    this->Abort();
    return status;
  }

  status = SetupReadOperation();
  if (status.http_code != SUCCESS) {
    this->Abort();
    return status;
  }

  status = Execute();
  if (status.http_code != SUCCESS) {
    this->Abort();
    return status;
  }

  status = CreateResponse();
  if (status.http_code != SUCCESS) {
    this->Abort();
    return status;
  }

  CloseTransaction();
  return RS_OK;
}

RS_Status PKROperation::Abort() {
  if (transaction != nullptr) {
    NdbTransaction::CommitStatusType status = transaction->commitStatus();
    if (status == NdbTransaction::CommitStatusType::Started) {
      transaction->execute(NdbTransaction::Rollback);
    }
    ndb_object->closeTransaction(transaction);
  }

  return RS_OK;
}



