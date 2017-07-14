/*
 *  Copyright (c) 2014, Oculus VR, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant 
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#ifndef __THREAD_POOL_H
#define __THREAD_POOL_H

#include "DS_Queue.h"
#include "SimpleMutex.h"
#include "Export.h"
#include "RakThread.h"
#include "SignaledEvent.h"

#ifdef _MSC_VER
#pragma warning( push )
#endif

class ThreadDataInterface
{
public:
    ThreadDataInterface()
    {
    }

    virtual ~ThreadDataInterface()
    {
    }

    virtual void *PerThreadFactory(void *context)=0;
    virtual void PerThreadDestructor(void *factoryResult, void *context)=0;
};

/// A simple class to create worker threads that processes a queue of functions with data.
/// This class does not allocate or deallocate memory.  It is up to the user to handle memory management.
/// InputType and OutputType are stored directly in a queue.  For large structures, if you plan to delete from the middle of the queue,
/// you might wish to store pointers rather than the structures themselves so the array can shift efficiently.
template<class InputType, class OutputType>
struct RAK_DLL_EXPORT ThreadPool
{
    ThreadPool();
    ~ThreadPool();

    /// Start the specified number of threads.
    /// \param[in] numThreads The number of threads to start
    /// \param[in] stackSize 0 for default (except on consoles).
    /// \param[in] _perThreadInit User callback to return data stored per thread.  Pass 0 if not needed.
    /// \param[in] _perThreadDeinit User callback to destroy data stored per thread, created by _perThreadInit.  Pass 0 if not needed.
    /// \return True on success, false on failure.
    bool
    StartThreads(int numThreads, int stackSize, void *(*_perThreadInit)() = 0, void (*_perThreadDeinit)(void *) = 0);

    // Alternate form of _perThreadDataFactory, _perThreadDataDestructor
    void SetThreadDataInterface(ThreadDataInterface *tdi, void *context);

    /// Stops all threads
    void StopThreads(void);

    /// Adds a function to a queue with data to pass to that function.  This function will be called from the thread
    /// Memory management is your responsibility!  This class does not allocate or deallocate memory.
    /// The best way to deallocate \a inputData is in userCallback.  If you call EndThreads such that callbacks were not called, you
    /// can iterate through the inputQueue and deallocate all pending input data there
    /// The best way to deallocate output is as it is returned to you from GetOutput.  Similarly, if you end the threads such that
    /// not all output was returned, you can iterate through outputQueue and deallocate it there.
    /// \param[in] workerThreadCallback The function to call from the thread
    /// \param[in] inputData The parameter to pass to \a userCallback
    void AddInput(OutputType (*workerThreadCallback)(InputType, bool *returnOutput, void *perThreadData),
                  InputType inputData);

    /// Adds to the output queue
    /// Use it if you want to inject output into the same queue that the system uses. Normally you would not use this. Consider it a convenience function.
    /// \param[in] outputData The output to inject
    void AddOutput(OutputType outputData);

    /// Returns true if output from GetOutput is waiting.
    /// \return true if output is waiting, false otherwise
    bool HasOutput(void);

    /// Inaccurate but fast version of HasOutput.  If this returns true, you should still check HasOutput for the real value.
    /// \return true if output is probably waiting, false otherwise
    bool HasOutputFast(void);

    /// Returns true if input from GetInput is waiting.
    /// \return true if input is waiting, false otherwise
    bool HasInput(void);

    /// Inaccurate but fast version of HasInput.  If this returns true, you should still check HasInput for the real value.
    /// \return true if input is probably waiting, false otherwise
    bool HasInputFast(void);

    /// Gets the output of a call to \a userCallback
    /// HasOutput must return true before you call this function.  Otherwise it will assert.
    /// \return The output of \a userCallback.  If you have different output signatures, it is up to you to encode the data to indicate this
    OutputType GetOutput(void);

    /// Clears internal buffers
    void Clear(void);

    /// Lock the input buffer before calling the functions InputSize, InputAtIndex, and RemoveInputAtIndex
    /// It is only necessary to lock the input or output while the threads are running
    void LockInput(void);

    /// Unlock the input buffer after you are done with the functions InputSize, GetInputAtIndex, and RemoveInputAtIndex
    void UnlockInput(void);

    /// Length of the input queue
    size_t InputSize(void);

    /// Get the input at a specified index
    InputType GetInputAtIndex(unsigned index);

    /// Remove input from a specific index.  This does NOT do memory deallocation - it only removes the item from the queue
    void RemoveInputAtIndex(unsigned index);

    /// Lock the output buffer before calling the functions OutputSize, OutputAtIndex, and RemoveOutputAtIndex
    /// It is only necessary to lock the input or output while the threads are running
    void LockOutput(void);

    /// Unlock the output buffer after you are done with the functions OutputSize, GetOutputAtIndex, and RemoveOutputAtIndex
    void UnlockOutput(void);

    /// Length of the output queue
    size_t OutputSize(void);

    /// Get the output at a specified index
    OutputType GetOutputAtIndex(unsigned index);

    /// Remove output from a specific index.  This does NOT do memory deallocation - it only removes the item from the queue
    void RemoveOutputAtIndex(unsigned index);

    /// Removes all items from the input queue
    void ClearInput(void);

    /// Removes all items from the output queue
    void ClearOutput(void);

    /// Are any of the threads working, or is input or output available?
    bool IsWorking(void);

    /// The number of currently active threads.
    int NumThreadsWorking(void);

    /// Did we call Start?
    bool WasStarted(void);

    // Block until all threads are stopped.
    bool Pause(void);

    // Continue running
    void Resume(void);

protected:
    // It is valid to cancel input before it is processed.  To do so, lock the inputQueue with inputQueueMutex,
    // Scan the list, and remove the item you don't want.
    RakNet::SimpleMutex inputQueueMutex, outputQueueMutex, workingThreadCountMutex, runThreadsMutex;

    void *(*perThreadDataFactory)();
    void (*perThreadDataDestructor)(void *);

    // inputFunctionQueue & inputQueue are paired arrays so if you delete from one at a particular index you must
    // delete from the other at the same index
    DataStructures::Queue<OutputType (*)(InputType, bool *, void *)> inputFunctionQueue;
    DataStructures::Queue<InputType> inputQueue;
    DataStructures::Queue<OutputType> outputQueue;

    ThreadDataInterface *threadDataInterface;
    void *tdiContext;

    template<class ThreadInputType, class ThreadOutputType>
    friend RAK_THREAD_DECLARATION(WorkerThread);

    /// \internal
    bool runThreads;
    /// \internal
    int numThreadsRunning;
    /// \internal
    int numThreadsWorking;
    /// \internal
    RakNet::SimpleMutex numThreadsRunningMutex;

    RakNet::SignaledEvent quitAndIncomingDataEvents;
};

#include "ThreadPool.h"
#include "RakSleep.h"

#ifndef _WIN32
#include <unistd.h>
#endif

#ifdef _MSC_VER
#pragma warning(disable:4127)
#pragma warning( disable : 4701 )  // potentially uninitialized local variable 'inputData' used
#endif

template<class ThreadInputType, class ThreadOutputType>
RAK_THREAD_DECLARATION(WorkerThread)
{

    ThreadPool<ThreadInputType, ThreadOutputType> *threadPool = (ThreadPool<ThreadInputType, ThreadOutputType> *) arguments;

    ThreadOutputType (*userCallback)(ThreadInputType, bool *, void *);
    ThreadInputType inputData;

    userCallback = 0;

    void *perThreadData;
    if (threadPool->perThreadDataFactory)
        perThreadData = threadPool->perThreadDataFactory();
    else if (threadPool->threadDataInterface)
        perThreadData = threadPool->threadDataInterface->PerThreadFactory(threadPool->tdiContext);
    else
        perThreadData = 0;

    // Increase numThreadsRunning
    threadPool->numThreadsRunningMutex.Lock();
    ++threadPool->numThreadsRunning;
    threadPool->numThreadsRunningMutex.Unlock();

    while (1)
    {
//#ifdef _WIN32
        if (userCallback == 0)
            threadPool->quitAndIncomingDataEvents.WaitOnEvent(1000);
// #else
//         if (userCallback==0)
//             RakSleep(30);
// #endif

        threadPool->runThreadsMutex.Lock();
        if (!threadPool->runThreads)
        {
            threadPool->runThreadsMutex.Unlock();
            break;
        }
        threadPool->runThreadsMutex.Unlock();

        threadPool->workingThreadCountMutex.Lock();
        ++threadPool->numThreadsWorking;
        threadPool->workingThreadCountMutex.Unlock();

        // Read input data
        userCallback = 0;
        threadPool->inputQueueMutex.Lock();
        if (threadPool->inputFunctionQueue.Size())
        {
            userCallback = threadPool->inputFunctionQueue.Pop();
            inputData = threadPool->inputQueue.Pop();
        }
        threadPool->inputQueueMutex.Unlock();

        if (userCallback)
        {
            bool returnOutput;
            ThreadOutputType callbackOutput = userCallback(inputData, &returnOutput, perThreadData);
            if (returnOutput)
            {
                threadPool->outputQueueMutex.Lock();
                threadPool->outputQueue.Push(callbackOutput);
                threadPool->outputQueueMutex.Unlock();
            }
        }

        threadPool->workingThreadCountMutex.Lock();
        --threadPool->numThreadsWorking;
        threadPool->workingThreadCountMutex.Unlock();
    }

    // Decrease numThreadsRunning
    threadPool->numThreadsRunningMutex.Lock();
    --threadPool->numThreadsRunning;
    threadPool->numThreadsRunningMutex.Unlock();

    if (threadPool->perThreadDataDestructor)
        threadPool->perThreadDataDestructor(perThreadData);
    else if (threadPool->threadDataInterface)
        threadPool->threadDataInterface->PerThreadDestructor(perThreadData, threadPool->tdiContext);

    return 0;
}

template<class InputType, class OutputType>
ThreadPool<InputType, OutputType>::ThreadPool()
{
    runThreads = false;
    numThreadsRunning = 0;
    threadDataInterface = 0;
    tdiContext = 0;
    numThreadsWorking = 0;

}

template<class InputType, class OutputType>
ThreadPool<InputType, OutputType>::~ThreadPool()
{
    StopThreads();
    Clear();
}

template<class InputType, class OutputType>
bool ThreadPool<InputType, OutputType>::StartThreads(int numThreads, int stackSize, void *(*_perThreadDataFactory)(),
                                                     void (*_perThreadDataDestructor)(void *))
{
    (void) stackSize;

// #if defined(SN_TARGET_PSP2)
//     runtime = RakNet::RakThread::AllocRuntime(numThreads);
// #endif

    runThreadsMutex.Lock();
    if (runThreads)
    {
        // Already running
        runThreadsMutex.Unlock();
        return false;
    }
    runThreadsMutex.Unlock();

    quitAndIncomingDataEvents.InitEvent();

    perThreadDataFactory = _perThreadDataFactory;
    perThreadDataDestructor = _perThreadDataDestructor;

    runThreadsMutex.Lock();
    runThreads = true;
    runThreadsMutex.Unlock();

    numThreadsWorking = 0;

    for (int i = 0; i < numThreads; i++)
    {
        int errorCode = RakNet::RakThread::Create(WorkerThread<InputType, OutputType>, this);

        if (errorCode != 0)
        {
            StopThreads();
            return false;
        }
    }
    // Wait for number of threads running to increase to numThreads
    bool done = false;
    while (!done)
    {
        RakSleep(50);
        numThreadsRunningMutex.Lock();
        if (numThreadsRunning == numThreads)
            done = true;
        numThreadsRunningMutex.Unlock();
    }

    return true;
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::SetThreadDataInterface(ThreadDataInterface *tdi, void *context)
{
    threadDataInterface = tdi;
    tdiContext = context;
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::StopThreads(void)
{
    runThreadsMutex.Lock();
    if (!runThreads)
    {
        runThreadsMutex.Unlock();
        return;
    }

    runThreads = false;
    runThreadsMutex.Unlock();

    // Wait for number of threads running to decrease to 0
    bool done = false;
    while (!done)
    {
        quitAndIncomingDataEvents.SetEvent();

        RakSleep(50);
        numThreadsRunningMutex.Lock();
        if (numThreadsRunning == 0)
            done = true;
        numThreadsRunningMutex.Unlock();
    }

    quitAndIncomingDataEvents.CloseEvent();

// #if defined(SN_TARGET_PSP2)
//     RakNet::RakThread::DeallocRuntime(runtime);
//     runtime=0;
// #endif

}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::AddInput(
        OutputType (*workerThreadCallback)(InputType, bool *returnOutput, void *perThreadData), InputType inputData)
{
    inputQueueMutex.Lock();
    inputQueue.Push(inputData);
    inputFunctionQueue.Push(workerThreadCallback);
    inputQueueMutex.Unlock();

    quitAndIncomingDataEvents.SetEvent();
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::AddOutput(OutputType outputData)
{
    outputQueueMutex.Lock();
    outputQueue.Push(outputData);
    outputQueueMutex.Unlock();
}

template<class InputType, class OutputType>
bool ThreadPool<InputType, OutputType>::HasOutputFast(void)
{
    return !outputQueue.IsEmpty();
}

template<class InputType, class OutputType>
bool ThreadPool<InputType, OutputType>::HasOutput(void)
{
    bool res;
    outputQueueMutex.Lock();
    res = !outputQueue.IsEmpty();
    outputQueueMutex.Unlock();
    return res;
}

template<class InputType, class OutputType>
bool ThreadPool<InputType, OutputType>::HasInputFast(void)
{
    return !inputQueue.IsEmpty();
}

template<class InputType, class OutputType>
bool ThreadPool<InputType, OutputType>::HasInput(void)
{
    bool res;
    inputQueueMutex.Lock();
    res = !inputQueue.IsEmpty();
    inputQueueMutex.Unlock();
    return res;
}

template<class InputType, class OutputType>
OutputType ThreadPool<InputType, OutputType>::GetOutput(void)
{
    // Real output check
    OutputType output;
    outputQueueMutex.Lock();
    output = outputQueue.Pop();
    outputQueueMutex.Unlock();
    return output;
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::Clear(void)
{
    runThreadsMutex.Lock();
    if (runThreads)
    {
        runThreadsMutex.Unlock();
        inputQueueMutex.Lock();
        inputFunctionQueue.Clear();
        inputQueue.Clear();
        inputQueueMutex.Unlock();

        outputQueueMutex.Lock();
        outputQueue.Clear();
        outputQueueMutex.Unlock();
    }
    else
    {
        inputFunctionQueue.Clear();
        inputQueue.Clear();
        outputQueue.Clear();
    }
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::LockInput(void)
{
    inputQueueMutex.Lock();
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::UnlockInput(void)
{
    inputQueueMutex.Unlock();
}

template<class InputType, class OutputType>
size_t ThreadPool<InputType, OutputType>::InputSize(void)
{
    return inputQueue.Size();
}

template<class InputType, class OutputType>
InputType ThreadPool<InputType, OutputType>::GetInputAtIndex(unsigned index)
{
    return inputQueue[index];
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::RemoveInputAtIndex(unsigned index)
{
    inputQueue.RemoveAtIndex(index);
    inputFunctionQueue.RemoveAtIndex(index);
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::LockOutput(void)
{
    outputQueueMutex.Lock();
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::UnlockOutput(void)
{
    outputQueueMutex.Unlock();
}

template<class InputType, class OutputType>
size_t ThreadPool<InputType, OutputType>::OutputSize(void)
{
    return outputQueue.Size();
}

template<class InputType, class OutputType>
OutputType ThreadPool<InputType, OutputType>::GetOutputAtIndex(unsigned index)
{
    return outputQueue[index];
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::RemoveOutputAtIndex(unsigned index)
{
    outputQueue.RemoveAtIndex(index);
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::ClearInput(void)
{
    inputQueue.Clear();
    inputFunctionQueue.Clear();
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::ClearOutput(void)
{
    outputQueue.Clear();
}

template<class InputType, class OutputType>
bool ThreadPool<InputType, OutputType>::IsWorking(void)
{
    bool isWorking;
//    workingThreadCountMutex.Lock();
//    isWorking=numThreadsWorking!=0;
//    workingThreadCountMutex.Unlock();

//    if (isWorking)
//        return true;

    // Bug fix: Originally the order of these two was reversed.
    // It's possible with the thread timing that working could have been false, then it picks up the data in the other thread, then it checks
    // here and sees there is no data.  So it thinks the thread is not working when it was.
    if (HasOutputFast() && HasOutput())
        return true;

    if (HasInputFast() && HasInput())
        return true;

    // Need to check is working again, in case the thread was between the first and second checks
    workingThreadCountMutex.Lock();
    isWorking = numThreadsWorking != 0;
    workingThreadCountMutex.Unlock();

    return isWorking;
}

template<class InputType, class OutputType>
int ThreadPool<InputType, OutputType>::NumThreadsWorking(void)
{
    return numThreadsWorking;
}

template<class InputType, class OutputType>
bool ThreadPool<InputType, OutputType>::WasStarted(void)
{
    bool b;
    runThreadsMutex.Lock();
    b = runThreads;
    runThreadsMutex.Unlock();
    return b;
}

template<class InputType, class OutputType>
bool ThreadPool<InputType, OutputType>::Pause(void)
{
    if (!WasStarted())
        return false;

    workingThreadCountMutex.Lock();
    while (numThreadsWorking > 0)
    {
        RakSleep(30);
    }
    return true;
}

template<class InputType, class OutputType>
void ThreadPool<InputType, OutputType>::Resume(void)
{
    workingThreadCountMutex.Unlock();
}

#ifdef _MSC_VER
#pragma warning( pop )
#endif

#endif

