/*
 *  Copyright (c) 2014, Oculus VR, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant 
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

/// \file DS_ThreadsafeAllocatingQueue.h
/// \internal
/// A threadsafe queue, that also uses a memory pool for allocation

#ifndef __THREADSAFE_ALLOCATING_QUEUE
#define __THREADSAFE_ALLOCATING_QUEUE

#include "DS_Queue.h"
#include "SimpleMutex.h"
#include "DS_MemoryPool.h"
#include <new>

// #if defined(new)
// #pragma push_macro("new")
// #undef new
// #define RMO_NEW_UNDEF_ALLOCATING_QUEUE
// #endif

namespace DataStructures
{

    template<class structureType>
    class RAK_DLL_EXPORT ThreadsafeAllocatingQueue
    {
    public:
        // Queue operations
        void Push(structureType *s);
        structureType *PopInaccurate(void);
        structureType *Pop(void);
        void SetPageSize(size_t size);
        bool IsEmpty(void);
        structureType *operator[](size_t position);
        void RemoveAtIndex(size_t position);
        size_t Size(void);

        // Memory pool operations
        structureType *Allocate();
        void Deallocate(structureType *s);
        void Clear();
    protected:

        mutable MemoryPool<structureType> memoryPool;
        RakNet::SimpleMutex memoryPoolMutex;
        Queue<structureType *> queue;
        RakNet::SimpleMutex queueMutex;
    };

    template<class structureType>
    void ThreadsafeAllocatingQueue<structureType>::Push(structureType *s)
    {
        queueMutex.Lock();
        queue.Push(s);
        queueMutex.Unlock();
    }

    template<class structureType>
    structureType *ThreadsafeAllocatingQueue<structureType>::PopInaccurate(void)
    {
        structureType *s;
        if (queue.IsEmpty())
            return 0;
        queueMutex.Lock();
        if (!queue.IsEmpty())
            s = queue.Pop();
        else
            s = 0;
        queueMutex.Unlock();
        return s;
    }

    template<class structureType>
    structureType *ThreadsafeAllocatingQueue<structureType>::Pop(void)
    {
        structureType *s;
        queueMutex.Lock();
        if (queue.IsEmpty())
        {
            queueMutex.Unlock();
            return 0;
        }
        s = queue.Pop();
        queueMutex.Unlock();
        return s;
    }

    template<class structureType>
    structureType *ThreadsafeAllocatingQueue<structureType>::Allocate()
    {
        structureType *s;
        memoryPoolMutex.Lock();
        s = memoryPool.Allocate();
        memoryPoolMutex.Unlock();
        // Call new operator, memoryPool doesn't do this
        s = new((void *) s) structureType;
        return s;
    }

    template<class structureType>
    void ThreadsafeAllocatingQueue<structureType>::Deallocate(structureType *s)
    {
        // Call delete operator, memory pool doesn't do this
        s->~structureType();
        memoryPoolMutex.Lock();
        memoryPool.Release(s);
        memoryPoolMutex.Unlock();
    }

    template<class structureType>
    void ThreadsafeAllocatingQueue<structureType>::Clear()
    {
        memoryPoolMutex.Lock();
        for (size_t i = 0; i < queue.Size(); i++)
        {
            queue[i]->~structureType();
            memoryPool.Release(queue[i]);
        }
        queue.Clear();
        memoryPoolMutex.Unlock();
        memoryPoolMutex.Lock();
        memoryPool.Clear();
        memoryPoolMutex.Unlock();
    }

    template<class structureType>
    void ThreadsafeAllocatingQueue<structureType>::SetPageSize(size_t size)
    {
        memoryPool.SetPageSize(size);
    }

    template<class structureType>
    bool ThreadsafeAllocatingQueue<structureType>::IsEmpty(void)
    {
        bool isEmpty;
        queueMutex.Lock();
        isEmpty = queue.IsEmpty();
        queueMutex.Unlock();
        return isEmpty;
    }

    template<class structureType>
    structureType *ThreadsafeAllocatingQueue<structureType>::operator[](size_t position)
    {
        structureType *s;
        queueMutex.Lock();
        s = queue[position];
        queueMutex.Unlock();
        return s;
    }

    template<class structureType>
    void ThreadsafeAllocatingQueue<structureType>::RemoveAtIndex(size_t position)
    {
        queueMutex.Lock();
        queue.RemoveAtIndex(position);
        queueMutex.Unlock();
    }

    template<class structureType>
    size_t ThreadsafeAllocatingQueue<structureType>::Size(void)
    {
        size_t s;
        queueMutex.Lock();
        s = queue.Size();
        queueMutex.Unlock();
        return s;
    }

}


// #if defined(RMO_NEW_UNDEF_ALLOCATING_QUEUE)
// #pragma pop_macro("new")
// #undef RMO_NEW_UNDEF_ALLOCATING_QUEUE
// #endif

#endif
