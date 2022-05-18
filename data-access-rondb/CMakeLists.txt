
# Copyright (C) 2022 Hopsworks AB 
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

project (rdrclient.so)

cmake_minimum_required (VERSION 3.5.0)

set (CMAKE_CXX_STANDARD 17)

include_directories (${CMAKE_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR})
set(RONDB_DEFAULT_DIR "/srv/hops/mysql")

if(NOT RONDB_DIR)
  IF(NOT EXISTS ${RONDB_DEFAULT_DIR})
    message(FATAL_ERROR "Unable to find RonDB at ${RONDB_DEFAULT_DIR}. Use -DRONDB_DIR:STRING=/rondb/path to specify RonDB install directory ")
  endif()

  set(RONDB_DIR ${RONDB_DEFAULT_DIR})
endif()

message(STATUS "Using RONDB_DIR installed at ${RONDB_DIR} ")

include_directories(${RONDB_DIR}/include ${RONDB_DIR}/include/storage/ndb ${RONDB_DIR}/include/storage/ndb/ndbapi ${RONDB_DIR}/include/storage/ndb/mgmapi)

if(BOOST_ROOT)
  IF(NOT EXISTS ${BOOST_ROOT})
    message(FATAL_ERROR "Invalid boost root directory")
  endif()

  include_directories(${BOOST_ROOT}) 
endif()


link_directories(${RONDB_DIR}/lib)

SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wformat-security  -Woverloaded-virtual -Wno-unused-variable -Wno-unused-parameter -Wno-unused-but-set-variable -Wno-strict-aliasing -Wno-switch -O3 -DDBUG_OFF -D__FILENAME__='\"$(subst ${CMAKE_SOURCE_DIR}/,,$(abspath $<))\"'" )

file(GLOB_RECURSE SOURCE ${PROJECT_SOURCE_DIR}/src/*)

#add_executable(rdrclient ${SOURCE})
add_library(rdrclient SHARED ${SOURCE})

target_link_libraries(rdrclient ndbclient rdrs_string pthread)
