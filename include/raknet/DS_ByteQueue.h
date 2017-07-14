/*
 *  Copyright (c) 2014, Oculus VR, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant 
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

/// \file DS_ByteQueue.h
/// \internal
/// \brief Byte queue
///


#ifndef __BYTE_QUEUE_H
#define __BYTE_QUEUE_H

#include <cstdio>
#include "Export.h"

/// The namespace DataStructures was only added to avoid compiler errors for commonly named data structures
/// As these data structures are stand-alone, you can use them outside of RakNet for your own projects if you wish.
namespace DataStructures
{
    class ByteQueue
    {
    public:
        ByteQueue();
        ~ByteQueue();
        void WriteBytes(const char *in, size_t length);
        bool ReadBytes(char *out, size_t maxLengthToRead, bool peek);
        size_t GetBytesWritten(void) const;
        char* PeekContiguousBytes(size_t *outLength) const;
        void IncrementReadOffset(size_t length);
        void DecrementReadOffset(size_t length);
        void Clear();
        void Print(void);

    protected:
        char *data;
        size_t readOffset, writeOffset, lengthAllocated;
    };
}

#endif
