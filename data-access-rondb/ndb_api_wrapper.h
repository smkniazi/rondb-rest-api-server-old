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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#include <stdbool.h>
#ifdef __cplusplus
extern "C" {
#endif
   bool do_write(long long key, char* value);
   long long do_read(long long key, char* bytes_read);
   bool do_delete(long long key);
   bool initialize(const char *connectstring);
   void shutdown();
   void hello_world();
#ifdef __cplusplus
}
#endif



