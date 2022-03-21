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

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ERROR_STRS_H 
#define ERROR_STRS_H

#define ERR000 "Failed to start transaction."
#define ERR001 "An operation has already been created."
#define ERR002 "Failed to start read operation."
#define ERR003 "Invalid column data."
//#define ERR004 "Failed to read column."
#define ERR005 "Failed to execute transaction."
#define ERR006 "Unable to copy data to the response buffer."
#define ERR007 "Database/Table does not exist."
/* #define ERR008 "Table does not exist." */
#define ERR009 "Column does not exist."
#define ERR010 "Wrong number of primary-key columns."
#define ERR011 "Wrong primay-key column."
#define ERR012 "ndb_init() failed"
#define ERR013 "failed to connect to RonDB mgm server"
#define ERR014 "Cluster was not ready within 30 secs"
#define ERR015 "Failed to initialize ndb object"
//#define ERR016 ""
//#define ERR017 ""
//#define ERR018 ""
//#define ERR019 ""
//#define ERR020 ""
//#define ERR021 ""
//#define ERR022 ""
//#define ERR023 ""
//#define ERR024 ""
//#define ERR025 ""
//#define ERR026 ""
//#define ERR027 ""
//#define ERR028 ""
//#define ERR029 ""
//#define ERR030 ""



#endif

#ifdef __cplusplus
}
#endif
