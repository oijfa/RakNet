/*
 *  Copyright (c) 2014, Oculus VR, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant 
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include <assert.h>

#ifndef RakAssert
#if   defined(__native_client__)
#define RakAssert(x)
#else
#if defined(_DEBUG)
#define RakAssert(x) assert(x);
#else
#define RakAssert(x) (void)(x);
#endif
#endif
#endif
