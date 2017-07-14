/*
 *  Copyright (c) 2014, Oculus VR, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant 
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include "NativeFeatureIncludes.h"

#if _RAKNET_SUPPORT_FileListTransfer == 1 && _RAKNET_SUPPORT_FileOperations == 1

#include "FileListTransfer.h"
#include "DS_HuffmanEncodingTree.h"
#include "FileListTransferCBInterface.h"
#include "StringCompressor.h"
#include "FileList.h"
#include "DS_Queue.h"
#include "MessageIdentifiers.h"
#include "RakNetTypes.h"
#include "RakPeerInterface.h"
#include "RakNetStatistics.h"
#include "IncrementalReadInterface.h"
#include "RakAssert.h"
#include "RakAlloca.h"

#ifdef _MSC_VER
#pragma warning( push )
#endif

namespace RakNet
{

    struct FLR_MemoryBlock
    {
        char *flrMemoryBlock;
    };

    struct FileListReceiver
    {
        FileListReceiver();
        ~FileListReceiver();
        FileListTransferCBInterface *downloadHandler;
        SystemAddress allowedSender;
        unsigned short setID;
        unsigned setCount;
        unsigned setTotalCompressedTransmissionLength;
        unsigned setTotalFinalLength;
        unsigned setTotalDownloadedLength;
        bool gotSetHeader;
        bool deleteDownloadHandler;
        bool isCompressed;
        int filesReceived;
        DataStructures::Map<unsigned int, FLR_MemoryBlock> pushedFiles;

        // Notifications
        unsigned int partLength;

    };

} // namespace RakNet

using namespace RakNet;

FileListReceiver::FileListReceiver()
{
    filesReceived = 0;
    setTotalDownloadedLength = 0;
    partLength = 1;
    DataStructures::Map<unsigned int, FLR_MemoryBlock>::IMPLEMENT_DEFAULT_COMPARISON();
}

FileListReceiver::~FileListReceiver()
{
    for (size_t i = 0; i < pushedFiles.Size(); i++)
        free(pushedFiles[i].flrMemoryBlock);
}

STATIC_FACTORY_DEFINITIONS(FileListTransfer, FileListTransfer)

void FileListTransfer::FileToPushRecipient::DeleteThis(void)
{
    for (size_t j = 0; j < filesToPush.Size(); j++)
        delete filesToPush[j];
    delete this;
}

void FileListTransfer::FileToPushRecipient::AddRef(void)
{
    refCountMutex.Lock();
    ++refCount;
    refCountMutex.Unlock();
}

void FileListTransfer::FileToPushRecipient::Deref(void)
{
    refCountMutex.Lock();
    --refCount;
    if (refCount == 0)
    {
        refCountMutex.Unlock();
        DeleteThis();
        return;
    }
    refCountMutex.Unlock();
}

FileListTransfer::FileListTransfer()
{
    setId = 0;
    DataStructures::Map<unsigned short, FileListReceiver *>::IMPLEMENT_DEFAULT_COMPARISON();
}

FileListTransfer::~FileListTransfer()
{
    threadPool.StopThreads();
    Clear();
}

void FileListTransfer::StartIncrementalReadThreads(int numThreads, int threadPriority)
{
    (void) threadPriority;

    threadPool.StartThreads(numThreads, 0);
}

unsigned short FileListTransfer::SetupReceive(FileListTransferCBInterface *handler, bool deleteHandler, SystemAddress allowedSender)
{
    if (rakPeerInterface && rakPeerInterface->GetConnectionState(allowedSender) != IS_CONNECTED)
        return (unsigned short) -1;
    FileListReceiver *receiver;

    if (fileListReceivers.Has(setId))
    {
        receiver = fileListReceivers.Get(setId);
        receiver->downloadHandler->OnDereference();
        if (receiver->deleteDownloadHandler)
            delete receiver->downloadHandler;
        delete receiver;
        fileListReceivers.Delete(setId);
    }

    receiver = new FileListReceiver;
    RakAssert(handler);
    receiver->downloadHandler = handler;
    receiver->allowedSender = allowedSender;
    receiver->gotSetHeader = false;
    receiver->deleteDownloadHandler = deleteHandler;
    receiver->setID = setId;
    fileListReceivers.Set(setId, receiver);
    unsigned short oldId = setId;
    if (++setId == (unsigned short) -1)
        setId = 0;
    return oldId;
}

void FileListTransfer::Send(FileList *fileList, RakNet::RakPeerInterface *rakPeer, SystemAddress recipient,
                            unsigned short setID, PacketPriority priority, char orderingChannel,
                            IncrementalReadInterface *_incrementalReadInterface, size_t _chunkSize)
{
    for (size_t flpcIndex = 0; flpcIndex < fileListProgressCallbacks.Size(); flpcIndex++)
        fileList->AddCallback(fileListProgressCallbacks[flpcIndex]);

    RakNet::BitStream outBitstream;
    bool sendReference;
    const char *dataBlocks[2];
    size_t lengths[2];
    size_t totalLength = 0;
    for (size_t i = 0; i < fileList->fileList.Size(); i++)
    {
        const FileListNode &fileListNode = fileList->fileList[i];
        totalLength += fileListNode.dataLengthBytes;
    }

    // Write the chunk header, which contains the frequency table, the total number of files, and the total number of bytes
    bool anythingToWrite;
    outBitstream.Write((MessageID) ID_FILE_LIST_TRANSFER_HEADER);
    outBitstream.Write(setID);
    anythingToWrite = fileList->fileList.Size() > 0;
    outBitstream.Write(anythingToWrite);
    if (anythingToWrite)
    {
        outBitstream.WriteCompressed(fileList->fileList.Size());
        outBitstream.WriteCompressed(totalLength);

        if (rakPeer)
            rakPeer->Send(&outBitstream, priority, RELIABLE_ORDERED, orderingChannel, recipient, false);
        else
            SendUnified(&outBitstream, priority, RELIABLE_ORDERED, orderingChannel, recipient, false);

        DataStructures::Queue<FileToPush *> filesToPush;

        for (size_t i = 0; i < fileList->fileList.Size(); i++)
        {
            sendReference = fileList->fileList[i].isAReference && _incrementalReadInterface != 0;
            if (sendReference)
            {
                FileToPush *fileToPush = new FileToPush;
                fileToPush->fileListNode.context = fileList->fileList[i].context;
                fileToPush->setIndex = i;
                fileToPush->fileListNode.filename = fileList->fileList[i].filename;
                fileToPush->fileListNode.fullPathToFile = fileList->fileList[i].fullPathToFile;
                fileToPush->fileListNode.fileLengthBytes = fileList->fileList[i].fileLengthBytes;
                fileToPush->fileListNode.dataLengthBytes = fileList->fileList[i].dataLengthBytes;
                //    fileToPush->systemAddress=recipient;
                //fileToPush->setID=setID;
                fileToPush->packetPriority = priority;
                fileToPush->orderingChannel = orderingChannel;
                fileToPush->currentOffset = 0;
                fileToPush->incrementalReadInterface = _incrementalReadInterface;
                fileToPush->chunkSize = _chunkSize;
                filesToPush.Push(fileToPush);
            }
            else
            {
                outBitstream.Reset();
                outBitstream.Write((MessageID) ID_FILE_LIST_TRANSFER_FILE);
                outBitstream << fileList->fileList[i].context;
                // outBitstream.Write(fileList->fileList[i].context);
                outBitstream.Write(setID);
                StringCompressor::Instance().EncodeString(fileList->fileList[i].filename, 512, &outBitstream);

                outBitstream.WriteCompressed(i);
                outBitstream.WriteCompressed(fileList->fileList[i].dataLengthBytes); // Original length in bytes

                outBitstream.AlignWriteToByteBoundary();

                dataBlocks[0] = (char *) outBitstream.GetData();
                lengths[0] = outBitstream.GetNumberOfBytesUsed();
                dataBlocks[1] = fileList->fileList[i].data;
                lengths[1] = fileList->fileList[i].dataLengthBytes;
                SendListUnified(dataBlocks, lengths, 2, priority, RELIABLE_ORDERED, orderingChannel, recipient, false);
            }
        }

        if (!filesToPush.IsEmpty())
        {
            FileToPushRecipient *ftpr;

            fileToPushRecipientListMutex.Lock();
            for (size_t i = 0; i < fileToPushRecipientList.Size(); i++)
            {
                if (fileToPushRecipientList[i]->systemAddress == recipient &&
                    fileToPushRecipientList[i]->setId == setId)
                {
//                     ftpr=fileToPushRecipientList[i];
//                     ftpr->AddRef();
//                     break;
                    RakAssert("setId already in use for this recipient" && 0);
                }
            }
            fileToPushRecipientListMutex.Unlock();

            //if (ftpr==0)
            //{
            ftpr = new FileToPushRecipient;
            ftpr->systemAddress = recipient;
            ftpr->setId = setID;
            ftpr->refCount = 2; // Allocated and in the list
            fileToPushRecipientList.Push(ftpr);
            //}
            while (!filesToPush.IsEmpty())
            {
                ////ftpr->filesToPushMutex.Lock();
                ftpr->filesToPush.Push(filesToPush.Pop());
                ////ftpr->filesToPushMutex.Unlock();
            }
            // ftpr out of scope
            ftpr->Deref();
            SendIRIToAddress(recipient, setID);
            return;
        }
        else
        {
            for (size_t flpcIndex = 0; flpcIndex < fileListProgressCallbacks.Size(); flpcIndex++)
                fileListProgressCallbacks[flpcIndex]->OnFilePushesComplete(recipient, setID);
        }
    }
    else
    {
        for (size_t flpcIndex = 0; flpcIndex < fileListProgressCallbacks.Size(); flpcIndex++)
            fileListProgressCallbacks[flpcIndex]->OnFilePushesComplete(recipient, setID);

        if (rakPeer)
            rakPeer->Send(&outBitstream, priority, RELIABLE_ORDERED, orderingChannel, recipient, false);
        else
            SendUnified(&outBitstream, priority, RELIABLE_ORDERED, orderingChannel, recipient, false);
    }
}

bool FileListTransfer::DecodeSetHeader(Packet *packet)
{
    unsigned short setID;
    RakNet::BitStream inBitStream(packet->data, packet->length, false);
    inBitStream.IgnoreBits(8);
    inBitStream.Read(setID);
    if (!fileListReceivers.Has(setID))
    {
        // If this assert hits you didn't call SetupReceive
        RakAssert(0);
        return false;
    }

    FileListReceiver *fileListReceiver = fileListReceivers.Get(setID);
    if (fileListReceiver->allowedSender != packet->systemAddress)
    {
        RakAssert(0);
        return false;
    }

    RakAssert(!fileListReceiver->gotSetHeader);

    bool anythingToWrite = false;
    inBitStream.Read(anythingToWrite);

    if (anythingToWrite)
    {
        inBitStream.ReadCompressed(fileListReceiver->setCount);
        if (inBitStream.ReadCompressed(fileListReceiver->setTotalFinalLength))
        {
            fileListReceiver->setTotalCompressedTransmissionLength = fileListReceiver->setTotalFinalLength;
            fileListReceiver->gotSetHeader = true;
            return true;
        }
    }
    else
    {
        FileListTransferCBInterface::DownloadCompleteStruct dcs;
        dcs.setID = fileListReceiver->setID;
        dcs.numberOfFilesInThisSet = fileListReceiver->setCount;
        dcs.byteLengthOfThisSet = fileListReceiver->setTotalFinalLength;
        dcs.senderSystemAddress = packet->systemAddress;
        dcs.senderGuid = packet->guid;

        if (!fileListReceiver->downloadHandler->OnDownloadComplete(&dcs))
        {
            fileListReceiver->downloadHandler->OnDereference();
            fileListReceivers.Delete(setID);
            if (fileListReceiver->deleteDownloadHandler)
                delete fileListReceiver->downloadHandler;
            delete fileListReceiver;
        }
        return true;
    }
    return false;
}

bool FileListTransfer::DecodeFile(Packet *packet, bool isTheFullFile)
{
    FileListTransferCBInterface::OnFileStruct onFileStruct;
    RakNet::BitStream inBitStream(packet->data, packet->length, false);
    inBitStream.IgnoreBits(8);

    onFileStruct.senderSystemAddress = packet->systemAddress;
    onFileStruct.senderGuid = packet->guid;

    unsigned int partCount = 0;
    unsigned int partTotal = 0;
    unsigned int partLength = 0;
    onFileStruct.fileData = 0;
    if (!isTheFullFile)
    {
        // Disable endian swapping on reading this, as it's generated locally in ReliabilityLayer.cpp
        inBitStream.ReadBits((unsigned char *) &partCount, BYTES_TO_BITS(sizeof(partCount)), true);
        inBitStream.ReadBits((unsigned char *) &partTotal, BYTES_TO_BITS(sizeof(partTotal)), true);
        inBitStream.ReadBits((unsigned char *) &partLength, BYTES_TO_BITS(sizeof(partLength)), true);
        inBitStream.IgnoreBits(8);
        // The header is appended to every chunk, which we continue to read after this statement flrMemoryBlock
    }
    inBitStream >> onFileStruct.context;
    // inBitStream.Read(onFileStruct.context);
    inBitStream.Read(onFileStruct.setID);
    FileListReceiver *fileListReceiver;
    if (!fileListReceivers.Has(onFileStruct.setID))
        return false;

    fileListReceiver = fileListReceivers.Get(onFileStruct.setID);
    if (fileListReceiver->allowedSender != packet->systemAddress)
    {
        RakAssert(0);
        return false;
    }
    RakAssert(fileListReceiver->gotSetHeader);
    if (!StringCompressor::Instance().DecodeString(onFileStruct.fileName, 512, &inBitStream))
    {
        RakAssert(0);
        return false;
    }

    inBitStream.ReadCompressed(onFileStruct.fileIndex);
    inBitStream.ReadCompressed(onFileStruct.byteLengthOfThisFile);

    onFileStruct.numberOfFilesInThisSet = fileListReceiver->setCount;
    onFileStruct.byteLengthOfThisSet = fileListReceiver->setTotalFinalLength;

    if (isTheFullFile)
    {
        onFileStruct.bytesDownloadedForThisFile = onFileStruct.byteLengthOfThisFile;
        fileListReceiver->setTotalDownloadedLength += onFileStruct.byteLengthOfThisFile;
        onFileStruct.bytesDownloadedForThisSet = fileListReceiver->setTotalDownloadedLength;
    }
    else
    {
        onFileStruct.bytesDownloadedForThisFile = partLength * partCount;
        onFileStruct.bytesDownloadedForThisSet = fileListReceiver->setTotalDownloadedLength + onFileStruct.bytesDownloadedForThisFile;
    }

    // User callback for this file.
    if (isTheFullFile)
    {
        inBitStream.AlignReadToByteBoundary();
        onFileStruct.fileData = (char *) malloc((size_t) onFileStruct.byteLengthOfThisFile);
        inBitStream.Read((char *) onFileStruct.fileData, onFileStruct.byteLengthOfThisFile);

        FileListTransferCBInterface::FileProgressStruct fps;
        fps.onFileStruct = &onFileStruct;
        fps.partCount = 1;
        fps.partTotal = 1;
        fps.dataChunkLength = onFileStruct.byteLengthOfThisFile;
        fps.firstDataChunk = onFileStruct.fileData;
        fps.iriDataChunk = onFileStruct.fileData;
        fps.allocateIrIDataChunkAutomatically = true;
        fps.iriWriteOffset = 0;
        fps.senderSystemAddress = packet->systemAddress;
        fps.senderGuid = packet->guid;
        fileListReceiver->downloadHandler->OnFileProgress(&fps);

        // Got a complete file
        // Either we are using IncrementalReadInterface and it was a small file or
        // We are not using IncrementalReadInterface
        if (fileListReceiver->downloadHandler->OnFile(&onFileStruct))
            free(onFileStruct.fileData);

        fileListReceiver->filesReceived++;

        // If this set is done, free the memory for it.
        if ((int) fileListReceiver->setCount == fileListReceiver->filesReceived)
        {
            FileListTransferCBInterface::DownloadCompleteStruct dcs;
            dcs.setID = fileListReceiver->setID;
            dcs.numberOfFilesInThisSet = fileListReceiver->setCount;
            dcs.byteLengthOfThisSet = fileListReceiver->setTotalFinalLength;
            dcs.senderSystemAddress = packet->systemAddress;
            dcs.senderGuid = packet->guid;

            if (!fileListReceiver->downloadHandler->OnDownloadComplete(&dcs))
            {
                fileListReceiver->downloadHandler->OnDereference();
                if (fileListReceiver->deleteDownloadHandler)
                    delete fileListReceiver->downloadHandler;
                fileListReceivers.Delete(onFileStruct.setID);
                delete fileListReceiver;
            }
        }

    }
    else
    {
        inBitStream.AlignReadToByteBoundary();

        char *firstDataChunk;
        size_t unreadBits = inBitStream.GetNumberOfUnreadBits();
        size_t unreadBytes = BITS_TO_BYTES(unreadBits);
        firstDataChunk = (char *) inBitStream.GetData() + BITS_TO_BYTES(inBitStream.GetReadOffset());

        FileListTransferCBInterface::FileProgressStruct fps;
        fps.onFileStruct = &onFileStruct;
        fps.partCount = partCount;
        fps.partTotal = partTotal;
        fps.dataChunkLength = unreadBytes;
        fps.firstDataChunk = firstDataChunk;
        fps.iriDataChunk = 0;
        fps.allocateIrIDataChunkAutomatically = true;
        fps.iriWriteOffset = 0;
        fps.senderSystemAddress = packet->systemAddress;
        fps.senderGuid = packet->guid;

        // Remote system is sending a complete file, but the file is large enough that we get ID_PROGRESS_NOTIFICATION from the transport layer
        fileListReceiver->downloadHandler->OnFileProgress(&fps);

    }

    return true;
}

PluginReceiveResult FileListTransfer::OnReceive(Packet *packet)
{
    switch (packet->data[0])
    {
        case ID_FILE_LIST_TRANSFER_HEADER:
            DecodeSetHeader(packet);
            return RR_STOP_PROCESSING_AND_DEALLOCATE;
        case ID_FILE_LIST_TRANSFER_FILE:
            DecodeFile(packet, true);
            return RR_STOP_PROCESSING_AND_DEALLOCATE;
        case ID_FILE_LIST_REFERENCE_PUSH:
            OnReferencePush(packet, true);
            return RR_STOP_PROCESSING_AND_DEALLOCATE;
        case ID_FILE_LIST_REFERENCE_PUSH_ACK:
            OnReferencePushAck(packet);
            return RR_STOP_PROCESSING_AND_DEALLOCATE;
        case ID_DOWNLOAD_PROGRESS:
            if (packet->length > sizeof(MessageID) + sizeof(unsigned int) * 3)
            {
                if (packet->data[sizeof(MessageID) + sizeof(unsigned int) * 3] == ID_FILE_LIST_TRANSFER_FILE)
                {
                    DecodeFile(packet, false);
                    return RR_STOP_PROCESSING_AND_DEALLOCATE;
                }
                if (packet->data[sizeof(MessageID) + sizeof(unsigned int) * 3] == ID_FILE_LIST_REFERENCE_PUSH)
                {
                    OnReferencePush(packet, false);
                    return RR_STOP_PROCESSING_AND_DEALLOCATE;
                }
            }
            break;
    }

    return RR_CONTINUE_PROCESSING;
}

void FileListTransfer::OnRakPeerShutdown(void)
{
    threadPool.StopThreads();
    threadPool.ClearInput();
    Clear();
}

void FileListTransfer::Clear(void)
{
    for (size_t i = 0; i < fileListReceivers.Size(); i++)
    {
        fileListReceivers[i]->downloadHandler->OnDereference();
        if (fileListReceivers[i]->deleteDownloadHandler)
            delete fileListReceivers[i]->downloadHandler;
        delete fileListReceivers[i];
    }
    fileListReceivers.Clear();

    fileToPushRecipientListMutex.Lock();
    for (size_t i = 0; i < fileToPushRecipientList.Size(); i++)
    {
        FileToPushRecipient *ftpr = fileToPushRecipientList[i];
        // Taken out of the list
        ftpr->Deref();
    }
    fileToPushRecipientList.Clear(false);
    fileToPushRecipientListMutex.Unlock();

    //filesToPush.Clear(false);
}

void FileListTransfer::OnClosedConnection(const SystemAddress &systemAddress, RakNetGUID rakNetGUID, PI2_LostConnectionReason lostConnectionReason)
{
    (void) lostConnectionReason;
    (void) rakNetGUID;

    RemoveReceiver(systemAddress);
}

void FileListTransfer::CancelReceive(unsigned short setId)
{
    if (!fileListReceivers.Has(setId))
    {
        RakAssert(0);
        return;
    }
    FileListReceiver *fileListReceiver = fileListReceivers.Get(setId);
    fileListReceiver->downloadHandler->OnDereference();
    if (fileListReceiver->deleteDownloadHandler)
        delete fileListReceiver->downloadHandler;
    delete fileListReceiver;
    fileListReceivers.Delete(setId);
}

void FileListTransfer::RemoveReceiver(SystemAddress systemAddress)
{
    threadPool.LockInput();
    for (size_t i = 0; i < threadPool.InputSize();)
    {
        if (threadPool.GetInputAtIndex(i).systemAddress == systemAddress)
            threadPool.RemoveInputAtIndex(i);
        else
            i++;
    }
    threadPool.UnlockInput();

    for (size_t i = 0; i < fileListReceivers.Size();)
    {
        if (fileListReceivers[i]->allowedSender == systemAddress)
        {
            fileListReceivers[i]->downloadHandler->OnDereference();
            if (fileListReceivers[i]->deleteDownloadHandler)
                delete fileListReceivers[i]->downloadHandler;
            delete fileListReceivers[i];
            fileListReceivers.RemoveAtIndex(i);
        }
        else
            i++;
    }

    fileToPushRecipientListMutex.Lock();
    for (size_t i = 0; i < fileToPushRecipientList.Size();)
    {
        if (fileToPushRecipientList[i]->systemAddress == systemAddress)
        {
            FileToPushRecipient *ftpr = fileToPushRecipientList[i];

            // Tell the user that this recipient was lost
            for (unsigned int flpcIndex = 0; flpcIndex < fileListProgressCallbacks.Size(); flpcIndex++)
                fileListProgressCallbacks[flpcIndex]->OnSendAborted(ftpr->systemAddress);

            fileToPushRecipientList.RemoveAtIndex(i);
            // Taken out of the list
            ftpr->Deref();
        }
        else
            i++;
    }
    fileToPushRecipientListMutex.Unlock();
}

bool FileListTransfer::IsHandlerActive(unsigned short setId)
{
    return fileListReceivers.Has(setId);
}

void FileListTransfer::AddCallback(FileListProgress *cb)
{
    if (cb == 0)
        return;

    if (fileListProgressCallbacks.GetIndexOf(cb) == (unsigned int) -1)
        fileListProgressCallbacks.Push(cb);
}

void FileListTransfer::RemoveCallback(FileListProgress *cb)
{
    size_t idx = fileListProgressCallbacks.GetIndexOf(cb);
    if (idx != (size_t) -1)
        fileListProgressCallbacks.RemoveAtIndex(idx);
}

void FileListTransfer::ClearCallbacks(void)
{
    fileListProgressCallbacks.Clear(true);
}

void FileListTransfer::GetCallbacks(DataStructures::List<FileListProgress *> &callbacks)
{
    callbacks = fileListProgressCallbacks;
}

void FileListTransfer::Update(void)
{
    for (size_t i = 0; i < fileListReceivers.Size();)
    {
        if (!fileListReceivers[i]->downloadHandler->Update())
        {
            fileListReceivers[i]->downloadHandler->OnDereference();
            if (fileListReceivers[i]->deleteDownloadHandler)
                delete fileListReceivers[i]->downloadHandler;
            delete fileListReceivers[i];
            fileListReceivers.RemoveAtIndex(i);
        }
        else
            i++;
    }
}

void FileListTransfer::OnReferencePush(Packet *packet, bool isTheFullFile)
{
    RakNet::BitStream refPushAck;
    if (!isTheFullFile)
    {
        // 12/23/09 Why do I care about ID_DOWNLOAD_PROGRESS for reference pushes?
        // 2/16/2012 I care because a reference push is 16 megabytes by default. Also, if it is the last file "if (ftpr->filesToPush.Size()<2)" or total file size exceeds smallFileTotalSize it always sends a reference push.
//        return;
    }

    FileListTransferCBInterface::OnFileStruct onFileStruct;
    RakNet::BitStream inBitStream(packet->data, packet->length, false);
    inBitStream.IgnoreBits(8);

    unsigned int partCount = 0;
    unsigned int partTotal = 1;
    unsigned int partLength = 0;
    onFileStruct.fileData = 0;
    if (!isTheFullFile)
    {
        // Disable endian swapping on reading this, as it's generated locally in ReliabilityLayer.cpp
        inBitStream.ReadBits((unsigned char *) &partCount, BYTES_TO_BITS(sizeof(partCount)), true);
        inBitStream.ReadBits((unsigned char *) &partTotal, BYTES_TO_BITS(sizeof(partTotal)), true);
        inBitStream.ReadBits((unsigned char *) &partLength, BYTES_TO_BITS(sizeof(partLength)), true);
        inBitStream.IgnoreBits(8);
        // The header is appended to every chunk, which we continue to read after this statement flrMemoryBlock
    }

    inBitStream >> onFileStruct.context;
    inBitStream.Read(onFileStruct.setID);

    // This is not a progress notification, it is actually the entire packet
    if (isTheFullFile)
    {
        refPushAck.Write((MessageID) ID_FILE_LIST_REFERENCE_PUSH_ACK);
        refPushAck.Write(onFileStruct.setID);
        SendUnified(&refPushAck, HIGH_PRIORITY, RELIABLE, 0, packet->systemAddress, false);
    }

    // inBitStream.Read(onFileStruct.context);
    FileListReceiver *fileListReceiver;
    if (!fileListReceivers.Has(onFileStruct.setID))
        return;

    fileListReceiver = fileListReceivers.Get(onFileStruct.setID);
    if (fileListReceiver->allowedSender != packet->systemAddress)
    {
        RakAssert(0);
        return;
    }

    RakAssert(fileListReceiver->gotSetHeader);

    if (!StringCompressor::Instance().DecodeString(onFileStruct.fileName, 512, &inBitStream))
    {
        RakAssert(0);
        return;
    }

    inBitStream.ReadCompressed(onFileStruct.fileIndex);
    inBitStream.ReadCompressed(onFileStruct.byteLengthOfThisFile);
    unsigned int offset;
    unsigned int chunkLength;
    inBitStream.ReadCompressed(offset);
    inBitStream.ReadCompressed(chunkLength);

    bool lastChunk = false;
    inBitStream.Read(lastChunk);
    bool finished = lastChunk && isTheFullFile;

    if (!isTheFullFile)
        fileListReceiver->partLength = partLength;

    FLR_MemoryBlock mb;
    if (!fileListReceiver->pushedFiles.Has(onFileStruct.fileIndex))
    {
        mb.flrMemoryBlock = (char *) malloc(onFileStruct.byteLengthOfThisFile);
        fileListReceiver->pushedFiles.SetNew(onFileStruct.fileIndex, mb);
    }
    else
    {
        mb = fileListReceiver->pushedFiles.Get(onFileStruct.fileIndex);
    }

    size_t unreadBits = inBitStream.GetNumberOfUnreadBits();
    BitSize_t unreadBytes = BITS_TO_BYTES(unreadBits);
    size_t amountToRead;
    if (isTheFullFile)
        amountToRead = chunkLength;
    else
        amountToRead = unreadBytes;

    inBitStream.AlignReadToByteBoundary();

    FileListTransferCBInterface::FileProgressStruct fps;

    if (isTheFullFile)
    {
        if (mb.flrMemoryBlock)
        {
            // Either the very first block, or a subsequent block and allocateIrIDataChunkAutomatically was true for the first block
            memcpy(mb.flrMemoryBlock + offset, inBitStream.GetData() + BITS_TO_BYTES(inBitStream.GetReadOffset()), amountToRead);
            fps.iriDataChunk = mb.flrMemoryBlock + offset;
        }
        else
        {
            // In here mb.flrMemoryBlock is null
            // This means the first block explicitly deallocated the memory, and no blocks will be permanently held by RakNet
            fps.iriDataChunk = (char *) inBitStream.GetData() + BITS_TO_BYTES(inBitStream.GetReadOffset());
        }

        onFileStruct.bytesDownloadedForThisFile = offset + chunkLength;
        fileListReceiver->setTotalDownloadedLength += chunkLength;
        onFileStruct.bytesDownloadedForThisSet = fileListReceiver->setTotalDownloadedLength;
    }
    else
    {
        onFileStruct.bytesDownloadedForThisFile = offset + partLength * partCount;
        onFileStruct.bytesDownloadedForThisSet = fileListReceiver->setTotalDownloadedLength + partCount * partLength;
        fps.iriDataChunk = (char *) inBitStream.GetData() + BITS_TO_BYTES(inBitStream.GetReadOffset());
    }

    onFileStruct.numberOfFilesInThisSet = fileListReceiver->setCount;
//    onFileStruct.setTotalCompressedTransmissionLength=fileListReceiver->setTotalCompressedTransmissionLength;
    onFileStruct.byteLengthOfThisSet = fileListReceiver->setTotalFinalLength;
    // Note: mb.flrMemoryBlock may be null here
    onFileStruct.fileData = mb.flrMemoryBlock;
    onFileStruct.senderSystemAddress = packet->systemAddress;
    onFileStruct.senderGuid = packet->guid;

    BitSize_t totalNotifications;
    unsigned int currentNotificationIndex;
    if (chunkLength == 0 || chunkLength == onFileStruct.byteLengthOfThisFile)
        totalNotifications = 1;
    else
        totalNotifications = onFileStruct.byteLengthOfThisFile / chunkLength + 1;

    if (chunkLength == 0)
        currentNotificationIndex = 0;
    else
        currentNotificationIndex = offset / chunkLength;

    fps.onFileStruct = &onFileStruct;
    fps.partCount = currentNotificationIndex;
    fps.partTotal = totalNotifications;
    fps.dataChunkLength = amountToRead;
    fps.firstDataChunk = mb.flrMemoryBlock;
    fps.allocateIrIDataChunkAutomatically = true;
    fps.onFileStruct->fileData = mb.flrMemoryBlock;
    fps.iriWriteOffset = offset;
    fps.senderSystemAddress = packet->systemAddress;
    fps.senderGuid = packet->guid;

    if (finished)
    {
        char *oldFileData = fps.onFileStruct->fileData;
        if (fps.partCount == 0)
            fps.firstDataChunk = fps.iriDataChunk;
        if (fps.partTotal == 1)
            fps.onFileStruct->fileData = fps.iriDataChunk;
        fileListReceiver->downloadHandler->OnFileProgress(&fps);

        // Incremental read interface sent us a file chunk
        // This is the last file chunk we were waiting for to consider the file done
        if (fileListReceiver->downloadHandler->OnFile(&onFileStruct))
            free(oldFileData);
        fileListReceiver->pushedFiles.Delete(onFileStruct.fileIndex);

        fileListReceiver->filesReceived++;

        // If this set is done, free the memory for it.
        if ((int) fileListReceiver->setCount == fileListReceiver->filesReceived)
        {
            FileListTransferCBInterface::DownloadCompleteStruct dcs;
            dcs.setID = fileListReceiver->setID;
            dcs.numberOfFilesInThisSet = fileListReceiver->setCount;
            dcs.byteLengthOfThisSet = fileListReceiver->setTotalFinalLength;
            dcs.senderSystemAddress = packet->systemAddress;
            dcs.senderGuid = packet->guid;

            if (!fileListReceiver->downloadHandler->OnDownloadComplete(&dcs))
            {
                fileListReceiver->downloadHandler->OnDereference();
                fileListReceivers.Delete(onFileStruct.setID);
                if (fileListReceiver->deleteDownloadHandler)
                    delete fileListReceiver->downloadHandler;
                delete fileListReceiver;
            }
        }
    }
    else
    {
        if (isTheFullFile)
        {
            // 12/23/09 Don't use OnReferencePush anymore, just use OnFileProgress
            fileListReceiver->downloadHandler->OnFileProgress(&fps);

            if (!fps.allocateIrIDataChunkAutomatically)
            {
                free(fileListReceiver->pushedFiles.Get(onFileStruct.fileIndex).flrMemoryBlock);
                fileListReceiver->pushedFiles.Get(onFileStruct.fileIndex).flrMemoryBlock = 0;
            }
        }
        else
        {
            // This is a download progress notification for a file chunk using incremental read interface
            // We don't have all the data for this chunk yet

            totalNotifications = onFileStruct.byteLengthOfThisFile / fileListReceiver->partLength + 1;
            currentNotificationIndex =
                    (offset + partCount * fileListReceiver->partLength) / fileListReceiver->partLength;
            unreadBytes = onFileStruct.byteLengthOfThisFile - ((currentNotificationIndex + 1) * fileListReceiver->partLength);
            fps.partCount = currentNotificationIndex;
            fps.partTotal = totalNotifications;

// 2/19/2013 Why was this check here? It prevent smaller progress notifications
//            if (rakPeerInterface)
            {
                // Thus chunk is incomplete
                fps.iriDataChunk = 0;

                fileListReceiver->downloadHandler->OnFileProgress(&fps);
            }
        }
    }

    return;
}

namespace RakNet
{

/*
SendIRIToAddress - executes from Send(). =
1, Find the recipient to send for
2. Send ID_FILE_LIST_TRANSFER_FILE for each small file in the queue of ifles to be sent
3. If the file we are working on is done, remove it from the list
4. Send ID_FILE_LIST_REFERENCE_PUSH for the file we are working on

File sender:
ID_FILE_LIST_REFERENCE_PUSH sent from end of SendIRIToAddressCB

Recipient:
send ID_FILE_LIST_REFERENCE_PUSH_ACK sent from OnReferencePush() when 2nd parameter is true.

File sender:
Got ID_FILE_LIST_REFERENCE_PUSH_ACK. Calls OnReferencePushAck, calls SendIRIToAddress, calls SendIRIToAddressCB
*/

    int SendIRIToAddressCB(FileListTransfer::ThreadData threadData, bool *returnOutput, void *perThreadData)
    {
        (void) perThreadData;

        FileListTransfer *fileListTransfer = threadData.fileListTransfer;
        SystemAddress systemAddress = threadData.systemAddress;
        unsigned short setId = threadData.setId;
        *returnOutput = false;

        // Was previously using GetStatistics to get outgoing buffer size, but TCP with UnifiedSend doesn't have this
        const char *dataBlocks[2];
        size_t lengths[2];
        size_t smallFileTotalSize = 0;
        RakNet::BitStream outBitstream;

        fileListTransfer->fileToPushRecipientListMutex.Lock();
        for (size_t ftpIndex = 0; ftpIndex < fileListTransfer->fileToPushRecipientList.Size(); ftpIndex++)
        {
            FileListTransfer::FileToPushRecipient *ftpr = fileListTransfer->fileToPushRecipientList[ftpIndex];
            // Referenced by both ftpr and list
            ftpr->AddRef();

            fileListTransfer->fileToPushRecipientListMutex.Unlock();

            if (ftpr->systemAddress == systemAddress && ftpr->setId == setId)
            {
                FileListTransfer::FileToPush *ftp = ftpr->filesToPush.Pop();

                // Read and send chunk. If done, delete at this index
                void *buff = malloc(ftp->chunkSize);
                if (buff == 0)
                {
                    ftpr->filesToPush.PushAtHead(ftp, 0);

                    ftpr->Deref();
                    RakAssert(0)
                    return 0;
                }

                // Read the next file chunk
                size_t bytesRead = ftp->incrementalReadInterface->GetFilePart(ftp->fileListNode.fullPathToFile,
                                                                       ftp->currentOffset, ftp->chunkSize, buff,
                                                                       ftp->fileListNode.context);

                bool done = ftp->fileListNode.dataLengthBytes == ftp->currentOffset + bytesRead;
                while (done && ftp->currentOffset == 0 && smallFileTotalSize < ftp->chunkSize)
                {
                    // The reason for 2 is that ID_FILE_LIST_REFERENCE_PUSH gets ID_FILE_LIST_REFERENCE_PUSH_ACK.
                    // Without ID_FILE_LIST_REFERENCE_PUSH_ACK, SendIRIToAddressCB would not be called again
                    if (ftpr->filesToPush.Size() < 2)
                        break;

                    // Send all small files at once, rather than wait for ID_FILE_LIST_REFERENCE_PUSH.
                    // But at least one ID_FILE_LIST_REFERENCE_PUSH must be sent
                    outBitstream.Reset();
                    outBitstream.Write((MessageID) ID_FILE_LIST_TRANSFER_FILE);
                    // outBitstream.Write(ftp->fileListNode.context);
                    outBitstream << ftp->fileListNode.context;
                    outBitstream.Write(setId);
                    StringCompressor::Instance().EncodeString(ftp->fileListNode.filename, 512, &outBitstream);
                    outBitstream.WriteCompressed(ftp->setIndex);
                    outBitstream.WriteCompressed(ftp->fileListNode.dataLengthBytes); // Original length in bytes
                    outBitstream.AlignWriteToByteBoundary();
                    dataBlocks[0] = (char *) outBitstream.GetData();
                    lengths[0] = outBitstream.GetNumberOfBytesUsed();
                    dataBlocks[1] = (const char *) buff;
                    lengths[1] = bytesRead;

                    fileListTransfer->SendListUnified(dataBlocks, lengths, 2, ftp->packetPriority, RELIABLE_ORDERED,
                                                      ftp->orderingChannel, systemAddress, false);

                    // LWS : fixed freed pointer reference
//                unsigned int chunkSize = ftp->chunkSize;
                    delete ftp;
                    smallFileTotalSize += bytesRead;
                    //done = bytesRead!=ftp->chunkSize;
                    ////ftpr->filesToPushMutex.Lock();
                    ftp = ftpr->filesToPush.Pop();
                    ////ftpr->filesToPushMutex.Unlock();

                    bytesRead = ftp->incrementalReadInterface->GetFilePart(ftp->fileListNode.fullPathToFile, 
                                                                           ftp->currentOffset, ftp->chunkSize, buff,
                                                                           ftp->fileListNode.context);
                    done = ftp->fileListNode.dataLengthBytes == ftp->currentOffset + bytesRead;
                }


                outBitstream.Reset();
                outBitstream.Write((MessageID) ID_FILE_LIST_REFERENCE_PUSH);
                // outBitstream.Write(ftp->fileListNode.context);
                outBitstream << ftp->fileListNode.context;
                outBitstream.Write(setId);
                StringCompressor::Instance().EncodeString(ftp->fileListNode.filename, 512, &outBitstream);
                outBitstream.WriteCompressed(ftp->setIndex);
                outBitstream.WriteCompressed(ftp->fileListNode.dataLengthBytes); // Original length in bytes
                outBitstream.WriteCompressed(ftp->currentOffset);
                ftp->currentOffset += bytesRead;
                outBitstream.WriteCompressed(bytesRead);
                outBitstream.Write(done);

                for (size_t flpcIndex = 0; flpcIndex < fileListTransfer->fileListProgressCallbacks.Size(); flpcIndex++)
                    fileListTransfer->fileListProgressCallbacks[flpcIndex]->OnFilePush(ftp->fileListNode.filename,
                                                                                       ftp->fileListNode.fileLengthBytes,
                                                                                       ftp->currentOffset - bytesRead,
                                                                                       bytesRead, done, systemAddress,
                                                                                       setId);

                dataBlocks[0] = (char *) outBitstream.GetData();
                lengths[0] = outBitstream.GetNumberOfBytesUsed();
                dataBlocks[1] = (char *) buff;
                lengths[1] = bytesRead;
                //rakPeerInterface->SendList(dataBlocks,lengths,2,ftp->packetPriority, RELIABLE_ORDERED, ftp->orderingChannel, ftp->systemAddress, false);
                char orderingChannel = ftp->orderingChannel;
                PacketPriority packetPriority = ftp->packetPriority;

                // Mutex state: FileToPushRecipient (ftpr) has AddRef. fileToPushRecipientListMutex not locked.
                if (done)
                {
                    // Done
                    //unsigned short setId = ftp->setID;
                    delete ftp;

                    ////ftpr->filesToPushMutex.Lock();
                    if (ftpr->filesToPush.Size() == 0)
                    {
                        ////ftpr->filesToPushMutex.Unlock();

                        for (size_t flpcIndex = 0; flpcIndex < fileListTransfer->fileListProgressCallbacks.Size(); flpcIndex++)
                            fileListTransfer->fileListProgressCallbacks[flpcIndex]->OnFilePushesComplete(systemAddress, setId);

                        // Remove ftpr from fileToPushRecipientList
                        fileListTransfer->RemoveFromList(ftpr);
                    }
                    else
                    {
                        ////ftpr->filesToPushMutex.Unlock();
                    }
                }
                else
                {
                    ////ftpr->filesToPushMutex.Lock();
                    ftpr->filesToPush.PushAtHead(ftp, 0);
                    ////ftpr->filesToPushMutex.Unlock();
                }
                // ftpr out of scope
                ftpr->Deref();

                // 2/12/2012 Moved this line at after the if (done) block above.
                // See http://www.jenkinssoftware.com/forum/index.php?topic=4768.msg19738#msg19738
                fileListTransfer->SendListUnified(dataBlocks, lengths, 2, packetPriority, RELIABLE_ORDERED, orderingChannel, systemAddress, false);

                free(buff);
                return 0;
            }
            else
            {
                ftpr->Deref();
                fileListTransfer->fileToPushRecipientListMutex.Lock();
            }
        }

        fileListTransfer->fileToPushRecipientListMutex.Unlock();

        return 0;
    }
}

void FileListTransfer::SendIRIToAddress(SystemAddress systemAddress, unsigned short setId)
{
    ThreadData threadData;
    threadData.fileListTransfer = this;
    threadData.systemAddress = systemAddress;
    threadData.setId = setId;

    if (threadPool.WasStarted())
        threadPool.AddInput(SendIRIToAddressCB, threadData);
    else
    {
        bool doesNothing;
        SendIRIToAddressCB(threadData, &doesNothing, 0);
    }
}

void FileListTransfer::OnReferencePushAck(Packet *packet)
{
    RakNet::BitStream inBitStream(packet->data, packet->length, false);
    inBitStream.IgnoreBits(8);
    unsigned short setId;
    inBitStream.Read(setId);
    SendIRIToAddress(packet->systemAddress, setId);
}

void FileListTransfer::RemoveFromList(FileToPushRecipient *ftpr)
{
    fileToPushRecipientListMutex.Lock();
    for (size_t i = 0; i < fileToPushRecipientList.Size(); i++)
    {
        if (fileToPushRecipientList[i] == ftpr)
        {
            fileToPushRecipientList.RemoveAtIndex(i);
            // List no longer references
            ftpr->Deref();
            fileToPushRecipientListMutex.Unlock();
            return;
        }
    }
    fileToPushRecipientListMutex.Unlock();
}

size_t FileListTransfer::GetPendingFilesToAddress(SystemAddress recipient)
{
    fileToPushRecipientListMutex.Lock();
    for (size_t i = 0; i < fileToPushRecipientList.Size(); i++)
    {
        if (fileToPushRecipientList[i]->systemAddress == recipient)
        {
            size_t size = fileToPushRecipientList[i]->filesToPush.Size();
            fileToPushRecipientListMutex.Unlock();
            return size;
        }
    }
    fileToPushRecipientListMutex.Unlock();

    return 0;
}

#ifdef _MSC_VER
#pragma warning( pop )
#endif

#endif // _RAKNET_SUPPORT_*
