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

#ifndef DATA_ACCESS_RONDB_SRC_COMMON_RDRS_DATE_HPP_
#define DATA_ACCESS_RONDB_SRC_COMMON_RDRS_DATE_HPP_

#include <NdbApi.hpp>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <mysql_time.h>
#include <sys/time.h>  // struct timeval


typedef unsigned char uchar;
typedef Uint32 uint32;
typedef Int32 int32;
typedef Int64 int64;
typedef Uint64 uint64;
typedef unsigned long long ulonglong;
typedef long long longlong;

struct my_timeval {
  int64_t m_tv_sec;
  int64_t m_tv_usec;
};

constexpr const std::size_t MAX_DATE_STRING_REP_LENGTH =
    sizeof("YYYY-MM-DD AM HH:MM:SS.FFFFFF+HH:MM");
typedef struct MYSQL_TIME_STATUS {
  int warnings{0};
  unsigned int fractional_digits{0};
  unsigned int nanoseconds{0};
} MYSQL_TIME_STATUS;

using my_time_flags_t = unsigned int;
bool str_to_datetime(const char *str, std::size_t length, MYSQL_TIME *l_time, my_time_flags_t flags,
                     MYSQL_TIME_STATUS *status);

bool str_to_time(const char *str, std::size_t length, MYSQL_TIME *l_time, MYSQL_TIME_STATUS *status,
                 my_time_flags_t flags);

int my_date_to_str(const MYSQL_TIME &my_time, char *to);

int my_datetime_to_str(const MYSQL_TIME &my_time, char *to, uint dec);

int my_TIME_to_str(const MYSQL_TIME &my_time, char *to, uint dec);

longlong TIME_to_longlong_datetime_packed(const MYSQL_TIME &my_time);

void my_date_to_binary(const MYSQL_TIME *ltime, uchar *ptr);

void my_datetime_packed_to_binary(longlong nr, uchar *ptr, uint dec);

longlong number_to_datetime(longlong nr, MYSQL_TIME *time_res,
                            my_time_flags_t flags, int *was_cut);

void TIME_from_longlong_date_packed(MYSQL_TIME *ltime, longlong tmp);

void TIME_from_longlong_datetime_packed(MYSQL_TIME *ltime, longlong tmp);

void TIME_from_longlong_time_packed(MYSQL_TIME *ltime, longlong tmp);

longlong my_time_packed_from_binary(const uchar *ptr, uint dec);

longlong my_datetime_packed_from_binary(const uchar *ptr, uint dec);

longlong TIME_to_longlong_time_packed(const MYSQL_TIME &my_time);
void my_time_packed_to_binary(longlong nr, uchar *ptr, uint dec);

void my_timestamp_to_binary(const struct timeval *tm, uchar *ptr, uint dec);

void my_timestamp_from_binary(struct timeval *tm, const uchar *ptr, uint dec);

void my_timestamp_to_binary(const my_timeval *tm, unsigned char *ptr,
                            unsigned int dec);
void my_timestamp_from_binary(my_timeval *tm, const unsigned char *ptr,
                              unsigned int dec);

int my_timeval_to_str(const my_timeval *tm, char *to, uint dec);


//static int TIME_to_datetime_str(const MYSQL_TIME &my_time, char *to) {
/**
 * Unpack and pack date/time types.  There is no check that the data
 * is valid for MySQL.  Random input gives equally random output.
 * Fractional seconds wl#946 introduce new formats (type names with
 * suffix 2).  The methods for these take an extra precision argument
 * with range 0-6 which translates to 0-3 bytes.
 */

//typedef struct Year {
//  Uint32 year;
//} Year;
//struct Date {
//  Uint32 year, month, day;
//};
//struct Time {
//  Uint32 sign;  // as in Time2
//  Uint32 hour, minute, second;
//};
//struct Datetime {
//  Uint32 year, month, day;
//  Uint32 hour, minute, second;
//};
//struct Timestamp {
//  Uint32 second;
//};
//struct Time2 {
//  Uint32 sign;
//  Uint32 interval;
//  Uint32 hour, minute, second;
//  Uint32 fraction;
//};
//struct Datetime2 {
//  Uint32 sign;
//  Uint32 year, month, day;
//  Uint32 hour, minute, second;
//  Uint32 fraction;
//};
//struct Timestamp2 {
//  Uint32 second;
//  Uint32 fraction;
//};
//
//// unpack and pack date/time types
//
//// Year
//static inline int32 sint3korr(const uchar *A) {
//  return ((int32)(((A[2]) & 128)
//                      ? (((uint32)255L << 24) | (((uint32)A[2]) << 16) | (((uint32)A[1]) << 8) |
//                         ((uint32)A[0]))
//                      : (((uint32)A[2]) << 16) | (((uint32)A[1]) << 8) | ((uint32)A[0])));
//}
//
static inline uint32 uint3korr(const uchar *A) {
 return (uint32)(((uint32)(A[0])) + (((uint32)(A[1])) << 8) + (((uint32)(A[2])) << 16));
}
void my_unpack_date(MYSQL_TIME *l_time, const void *d) {
 uchar b[4];
 memcpy(b, d, 3);
 b[3]   = 0;
 uint w = (uint)uint3korr(b);
 l_time->day  = (w & 31);
 w >>= 5;
 l_time->month = (w & 15);
 w >>= 4;
 l_time->year = w;
 l_time->time_type = MYSQL_TIMESTAMP_DATE;
}
//
//static inline ulonglong uint5korr(const uchar *A) {
//  return ((ulonglong)(((uint32)(A[0])) + (((uint32)(A[1])) << 8) + (((uint32)(A[2])) << 16) +
//                      (((uint32)(A[3])) << 24)) +
//          (((ulonglong)(A[4])) << 32));
//}
//
//static inline ulonglong uint6korr(const uchar *A) {
//  return ((ulonglong)(((uint32)(A[0])) + (((uint32)(A[1])) << 8) + (((uint32)(A[2])) << 16) +
//                      (((uint32)(A[3])) << 24)) +
//          (((ulonglong)(A[4])) << 32) + (((ulonglong)(A[5])) << 40));
//}
//
///**
//  int3store
//
//  Stores an unsinged integer in a platform independent way
//
//  @param T  The destination buffer. Must be at least 3 bytes long
//  @param A  The integer to store.
//
//  _Example:_
//  A @ref a_protocol_type_int3 "int \<3\>" with the value 1 is stored as:
//  ~~~~~~~~~~~~~~~~~~~~~
//  01 00 00
//  ~~~~~~~~~~~~~~~~~~~~~
//*/
//static inline void int3store(uchar *T, uint A) {
//  *(T)     = (uchar)(A);
//  *(T + 1) = (uchar)(A >> 8);
//  *(T + 2) = (uchar)(A >> 16);
//}
//
//static inline void int5store(uchar *T, ulonglong A) {
//  *(T)     = (uchar)(A);
//  *(T + 1) = (uchar)(A >> 8);
//  *(T + 2) = (uchar)(A >> 16);
//  *(T + 3) = (uchar)(A >> 24);
//  *(T + 4) = (uchar)(A >> 32);
//}
//
//static inline void int6store(uchar *T, ulonglong A) {
//  *(T)     = (uchar)(A);
//  *(T + 1) = (uchar)(A >> 8);
//  *(T + 2) = (uchar)(A >> 16);
//  *(T + 3) = (uchar)(A >> 24);
//  *(T + 4) = (uchar)(A >> 32);
//  *(T + 5) = (uchar)(A >> 40);
//}
//
//void unpack_year(Year &s, const uchar *d) {
//  s.year = (uint)(1900 + d[0]);
//}
//
//void pack_year(const Year &s, uchar *d) {
//  d[0] = (uchar)(s.year - 1900);
//}
//
//// Date
//
//void unpack_date(Date &s, const uchar *d) {
//  uchar b[4];
//  memcpy(b, d, 3);
//  b[3]   = 0;
//  uint w = (uint)uint3korr(b);
//  s.day  = (w & 31);
//  w >>= 5;
//  s.month = (w & 15);
//  w >>= 4;
//  s.year = w;
//}
//
//void pack_date(const Date &s, uchar *d) {
//  uint w = 0;
//  w |= s.year;
//  w <<= 4;
//  w |= s.month;
//  w <<= 5;
//  w |= s.day;
//  int3store(d, w);
//}
//
//// Time
//
//void unpack_time(Time &s, const uchar *d) {
//  uchar b[4];
//  memcpy(b, d, 3);
//  b[3]   = 0;
//  uint w = 0;
//  int v  = (int)sint3korr(b);
//  if (v >= 0) {
//    s.sign = 1;
//    w      = (uint)v;
//  } else {
//    s.sign = 0;
//    w      = (uint)(-v);
//  }
//  const uint f = (uint)100;
//  s.second     = (w % f);
//  w /= f;
//  s.minute = (w % f);
//  w /= f;
//  s.hour = w;
//}
//
//void pack_time(const Time &s, uchar *d) {
//  const uint f = (uint)100;
//  uint w       = 0;
//  w += s.hour;
//  w *= f;
//  w += s.minute;
//  w *= f;
//  w += s.second;
//  int v = 0;
//  if (s.sign == 1) {
//    v = (int)w;
//  } else {
//    v = (int)w;
//    v = -v;
//  }
//  int3store(d, v);
//}
//
//// Datetime
//
//void unpack_datetime(Datetime &s, const uchar *d) {
//  uint64 w;
//  memcpy(&w, d, 8);
//  const uint64 f = (uint64)100;
//  s.second       = (w % f);
//  w /= f;
//  s.minute = (w % f);
//  w /= f;
//  s.hour = (w % f);
//  w /= f;
//  s.day = (w % f);
//  w /= f;
//  s.month = (w % f);
//  w /= f;
//  s.year = (uint)w;
//}
//
//void pack_datetime(const Datetime &s, uchar *d) {
//  const uint64 f = (uint64)100;
//  uint64 w       = 0;
//  w += s.year;
//  w *= f;
//  w += s.month;
//  w *= f;
//  w += s.day;
//  w *= f;
//  w += s.hour;
//  w *= f;
//  w += s.minute;
//  w *= f;
//  w += s.second;
//  memcpy(d, &w, 8);
//}
//
//// Timestamp
//
//void unpack_timestamp(Timestamp &s, const uchar *d) {
//  uint32 w;
//  memcpy(&w, d, 4);
//  s.second = (uint)w;
//}
//
//void pack_timestamp(const Timestamp &s, uchar *d) {
//  uint32 w = s.second;
//  memcpy(d, &w, 4);
//}
//
//// types with fractional seconds
//
//static uint64 unpack_bigendian(const uchar *d, uint len) {
//  // assert(len <= 8);
//  uint64 val = 0;
//  int i      = (int)len;
//  int s      = 0;
//  while (i != 0) {
//    i--;
//    uint64 v = d[i];
//    val += (v << s);
//    s += 8;
//  }
//  return val;
//}
//
//static void pack_bigendian(uint64 val, uchar *d, uint len) {
//  uchar b[8];
//  uint i = 0;
//  while (i < len) {
//    b[i] = (uchar)(val & 255);
//    val >>= 8;
//    i++;
//  }
//  uint j = 0;
//  while (i != 0) {
//    i--;
//    d[i] = b[j];
//    j++;
//  }
//}
//
//// Time2 : big-endian time(3 bytes).fraction(0-3 bytes)
//
//void unpack_time2(Time2 &s, const uchar *d, uint prec) {
//  const uint64 one = (uint64)1;
//  uint flen        = (1 + prec) / 2;
//  uint fbit        = 8 * flen;
//  uint64 val       = unpack_bigendian(&d[0], 3 + flen);
//  uint spos        = 23 + fbit;
//  uint sign        = (uint)((val & (one << spos)) >> spos);
//  if (sign == 0)  // negative
//    val = (one << spos) - val;
//  uint64 w = (val >> fbit);
//  s.second = (uint)(w & 63);
//  w >>= 6;
//  s.minute = (uint)(w & 63);
//  w >>= 6;
//  s.hour = (uint)(w & 1023);
//  w >>= 10;
//  s.interval = (uint)(w & 1);
//  w >>= 1;
//  s.sign = sign;
//  uint f = (uint)(val & ((one << fbit) - 1));
//  if (prec % 2 != 0)
//    f /= 10;
//  s.fraction = f;
//}
//
//void pack_time2(const Time2 &s, uchar *d, uint prec) {
//  const uint64 one = (uint64)1;
//  uint flen        = (1 + prec) / 2;
//  uint fbit        = 8 * flen;
//  uint spos        = 23 + fbit;
//  uint64 w         = 0;
//  w |= s.sign;
//  w <<= 1;
//  w |= s.interval;
//  w <<= 10;
//  w |= s.hour;
//  w <<= 6;
//  w |= s.minute;
//  w <<= 6;
//  w |= s.second;
//  uint f = s.fraction;
//  if (prec % 2 != 0)
//    f *= 10;
//  uint64 val = (w << fbit) | f;
//  if (s.sign == 0)
//    val = (one << spos) - val;
//  pack_bigendian(val, &d[0], 3 + flen);
//}
//
//// Datetime2 : big-endian date(5 bytes).fraction(0-3 bytes)
//
//void unpack_datetime2(Datetime2 &s, const uchar *d, uint prec) {
//  const uint64 one = (uint64)1;
//  uint flen        = (1 + prec) / 2;
//  uint fbit        = 8 * flen;
//  uint64 val       = unpack_bigendian(&d[0], 5 + flen);
//  uint spos        = 39 + fbit;
//  uint sign        = (uint)((val & (one << spos)) >> spos);
//  if (sign == 0)  // negative
//    val = (one << spos) - val;
//  uint64 w = (val >> fbit);
//  s.second = (uint)(w & 63);
//  w >>= 6;
//  s.minute = (uint)(w & 63);
//  w >>= 6;
//  s.hour = (uint)(w & 31);
//  w >>= 5;
//  s.day = (uint)(w & 31);
//  w >>= 5;
//  uint year_month = (uint)(w & ((1 << 17) - 1));
//  s.month         = year_month % 13;
//  s.year          = year_month / 13;
//  w >>= 17;
//  s.sign = sign;
//  uint f = (uint)(val & ((one << fbit) - 1));
//  if (prec % 2 != 0)
//    f /= 10;
//  s.fraction = f;
//}
//
//void pack_datetime2(const Datetime2 &s, uchar *d, uint prec) {
//  const uint64 one = (uint64)1;
//  uint flen        = (1 + prec) / 2;
//  uint fbit        = 8 * flen;
//  uint spos        = 39 + fbit;
//  uint64 w         = 0;
//  w |= s.sign;
//  w <<= 17;
//  w |= (s.year * 13 + s.month);
//  w <<= 5;
//  w |= s.day;
//  w <<= 5;
//  w |= s.hour;
//  w <<= 6;
//  w |= s.minute;
//  w <<= 6;
//  w |= s.second;
//  uint f = s.fraction;
//  if (prec % 2 != 0)
//    f *= 10;
//  uint64 val = (w << fbit) | f;
//  if (s.sign == 0)
//    val = (one << spos) - val;
//  pack_bigendian(val, &d[0], 5 + flen);
//}
//
//// Timestamp2 : big-endian non-negative unix time
//
//void unpack_timestamp2(Timestamp2 &s, const uchar *d, uint prec) {
//  uint flen = (1 + prec) / 2;
//  uint w    = (uint)unpack_bigendian(&d[0], 4);
//  s.second  = w;
//  uint f    = (uint)unpack_bigendian(&d[4], flen);
//  if (prec % 2 != 0)
//    f /= 10;
//  s.fraction = f;
//}
//
//void pack_timestamp2(const Timestamp2 &s, uchar *d, uint prec) {
//  uint flen = (1 + prec) / 2;
//  uint w    = s.second;
//  pack_bigendian(w, &d[0], 4);
//  uint f = s.fraction;
//  if (prec % 2 != 0)
//    f *= 10;
//  pack_bigendian(f, &d[4], flen);
//}
//
#endif  // DATA_ACCESS_RONDB_SRC_COMMON_RDRS_DATE_HPP_
