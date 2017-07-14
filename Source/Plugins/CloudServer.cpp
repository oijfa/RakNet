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

#if _RAKNET_SUPPORT_CloudServer == 1

#include "CloudServer.h"
#include "GetTime.h"
#include "MessageIdentifiers.h"
#include "BitStream.h"
#include "RakPeerInterface.h"

enum ServerToServerCommands
{
    STSC_PROCESS_GET_REQUEST,
    STSC_PROCESS_GET_RESPONSE,
    STSC_ADD_UPLOADED_AND_SUBSCRIBED_KEYS,
    STSC_ADD_UPLOADED_KEY,
    STSC_ADD_SUBSCRIBED_KEY,
    STSC_REMOVE_UPLOADED_KEY,
    STSC_REMOVE_SUBSCRIBED_KEY,
    STSC_DATA_CHANGED,
};

using namespace RakNet;

int CloudServer::RemoteServerComp(const RakNetGUID &key, RemoteServer *const &data)
{
    if (key < data->serverAddress)
        return -1;
    if (key > data->serverAddress)
        return 1;
    return 0;
}

int CloudServer::KeySubscriberIDComp(const CloudKey &key, KeySubscriberID *const &data)
{
    if (key.primaryKey < data->key.primaryKey)
        return -1;
    if (key.primaryKey > data->key.primaryKey)
        return 1;
    if (key.secondaryKey < data->key.secondaryKey)
        return -1;
    if (key.secondaryKey > data->key.secondaryKey)
        return 1;
    return 0;
}

int CloudServer::KeyDataPtrComp(const RakNetGUID &key, CloudData *const &data)
{
    if (key < data->clientGUID)
        return -1;
    if (key > data->clientGUID)
        return 1;
    return 0;
}

int CloudServer::KeyDataListComp(const CloudKey &key, CloudDataList *const &data)
{
    if (key.primaryKey < data->key.primaryKey)
        return -1;
    if (key.primaryKey > data->key.primaryKey)
        return 1;
    if (key.secondaryKey < data->key.secondaryKey)
        return -1;
    if (key.secondaryKey > data->key.secondaryKey)
        return 1;
    return 0;
}

int CloudServer::BufferedGetResponseFromServerComp(const RakNetGUID &key, CloudServer::BufferedGetResponseFromServer *const &data)
{
    if (key < data->serverAddress)
        return -1;
    if (key > data->serverAddress)
        return 1;
    return 0;
}

int CloudServer::GetRequestComp(const uint32_t &key, CloudServer::GetRequest *const &data)
{
    if (key < data->requestId)
        return -1;
    if (key > data->requestId)
        return -1;
    return 0;
}

void CloudServer::CloudQueryWithAddresses::Serialize(bool writeToBitstream, BitStream *bitStream)
{
    cloudQuery.Serialize(writeToBitstream, bitStream);

    if (writeToBitstream)
    {
        bitStream->WriteCasted<uint16_t>(specificSystems.Size());
        RakAssert(specificSystems.Size() < (uint16_t) -1);
        for (uint16_t i = 0; i < specificSystems.Size(); i++)
            bitStream->Write(specificSystems[i]);
    }
    else
    {
        uint16_t specificSystemsCount;
        RakNetGUID addressOrGuid;
        bitStream->Read(specificSystemsCount);
        for (uint16_t i = 0; i < specificSystemsCount; i++)
        {
            bitStream->Read(addressOrGuid);
            specificSystems.Push(addressOrGuid);
        }
    }
}

bool CloudServer::GetRequest::AllRemoteServersHaveResponded(void) const
{
    for (unsigned i = 0; i < remoteServerResponses.Size(); i++)
        if (remoteServerResponses[i]->gotResult == false)
            return false;
    return true;
}

void CloudServer::GetRequest::Clear(CloudAllocator *allocator)
{
    for (unsigned i = 0; i < remoteServerResponses.Size(); i++)
    {
        remoteServerResponses[i]->Clear(allocator);
        delete remoteServerResponses[i];
    }
    remoteServerResponses.Clear(false);
}

void CloudServer::BufferedGetResponseFromServer::Clear(CloudAllocator *allocator)
{
    for (unsigned i = 0; i < queryResult.rowsReturned.Size(); i++)
    {
        allocator->DeallocateRowData(queryResult.rowsReturned[i]->data);
        allocator->DeallocateCloudQueryRow(queryResult.rowsReturned[i]);
    }
    queryResult.rowsReturned.Clear(false);
}

CloudServer::CloudServer()
{
    maxUploadBytesPerClient = 0;
    maxBytesPerDowload = 0;
    nextGetRequestId = 0;
    nextGetRequestsCheck = 0;
}

CloudServer::~CloudServer()
{
    Clear();
}

void CloudServer::SetMaxUploadBytesPerClient(uint64_t bytes)
{
    maxUploadBytesPerClient = bytes;
}

void CloudServer::SetMaxBytesPerDownload(uint64_t bytes)
{
    maxBytesPerDowload = bytes;
}

void CloudServer::Update(void)
{
    // Timeout getRequests
    RakNet::Time time = RakNet::Time();
    if (time > nextGetRequestsCheck)
    {
        nextGetRequestsCheck = time + 1000;

        for (unsigned i = 0; i < getRequests.Size();)
        {
            if (time - getRequests[i]->requestStartTime > 3000)
            {
                // Remote server is not responding, just send back data with whoever did respond
                ProcessAndTransmitGetRequest(getRequests[i]);
                getRequests[i]->Clear(this);
                delete getRequests[i];
                getRequests.RemoveAtIndex(i);
            }
            else
                i++;
        }
    }
}

PluginReceiveResult CloudServer::OnReceive(Packet *packet)
{
    switch (packet->data[0])
    {
        case ID_CLOUD_POST_REQUEST:
            OnPostRequest(packet);
            return RR_STOP_PROCESSING_AND_DEALLOCATE;
        case ID_CLOUD_RELEASE_REQUEST:
            OnReleaseRequest(packet);
            return RR_STOP_PROCESSING_AND_DEALLOCATE;
        case ID_CLOUD_GET_REQUEST:
            OnGetRequest(packet);
            return RR_STOP_PROCESSING_AND_DEALLOCATE;
        case ID_CLOUD_UNSUBSCRIBE_REQUEST:
            OnUnsubscribeRequest(packet);
            return RR_STOP_PROCESSING_AND_DEALLOCATE;
        case ID_CLOUD_SERVER_TO_SERVER_COMMAND:
            if (packet->length > 1)
            {
                switch (packet->data[1])
                {
                    case STSC_PROCESS_GET_REQUEST:
                        OnServerToServerGetRequest(packet);
                        return RR_STOP_PROCESSING_AND_DEALLOCATE;
                    case STSC_PROCESS_GET_RESPONSE:
                        OnServerToServerGetResponse(packet);
                        return RR_STOP_PROCESSING_AND_DEALLOCATE;
                    case STSC_ADD_UPLOADED_AND_SUBSCRIBED_KEYS:
                        OnSendUploadedAndSubscribedKeysToServer(packet);
                        return RR_STOP_PROCESSING_AND_DEALLOCATE;
                    case STSC_ADD_UPLOADED_KEY:
                        OnSendUploadedKeyToServers(packet);
                        return RR_STOP_PROCESSING_AND_DEALLOCATE;
                    case STSC_ADD_SUBSCRIBED_KEY:
                        OnSendSubscribedKeyToServers(packet);
                        return RR_STOP_PROCESSING_AND_DEALLOCATE;
                    case STSC_REMOVE_UPLOADED_KEY:
                        OnRemoveUploadedKeyFromServers(packet);
                        return RR_STOP_PROCESSING_AND_DEALLOCATE;
                    case STSC_REMOVE_SUBSCRIBED_KEY:
                        OnRemoveSubscribedKeyFromServers(packet);
                        return RR_STOP_PROCESSING_AND_DEALLOCATE;
                    case STSC_DATA_CHANGED:
                        OnServerDataChanged(packet);
                        return RR_STOP_PROCESSING_AND_DEALLOCATE;
                }
            }
            return RR_STOP_PROCESSING_AND_DEALLOCATE;
    }
    return RR_CONTINUE_PROCESSING;
}

void CloudServer::OnPostRequest(Packet *packet)
{
    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID));
    CloudKey key;
    key.Serialize(false, &bsIn);
    uint32_t dataLengthBytes;
    bsIn.Read(dataLengthBytes);
    if (maxUploadBytesPerClient > 0 && dataLengthBytes > maxUploadBytesPerClient)
        return; // Exceeded max upload bytes

    bsIn.AlignReadToByteBoundary();
    for (unsigned filterIndex = 0; filterIndex < queryFilters.Size(); filterIndex++)
    {
        if (!queryFilters[filterIndex]->OnPostRequest(packet->guid, packet->systemAddress, key, dataLengthBytes,
                                                             (const char *) bsIn.GetData() +
                                                             BITS_TO_BYTES(bsIn.GetReadOffset())))
            return;
    }

    unsigned char *data;
    if (dataLengthBytes > CLOUD_SERVER_DATA_STACK_SIZE)
    {
        data = (unsigned char *) malloc(dataLengthBytes);
        if (data == 0)
        {
            RakAssert(0)
            return;
        }
        bsIn.ReadAlignedBytes(data, dataLengthBytes);
    }
    else
        data = 0;

    // Add this system to remoteSystems if they aren't there already
    DataStructures::HashIndex remoteSystemsHashIndex = remoteSystems.GetIndexOf(packet->guid);
    RemoteCloudClient *remoteCloudClient;
    if (remoteSystemsHashIndex.IsInvalid())
    {
        remoteCloudClient = new RemoteCloudClient;
        remoteCloudClient->uploadedKeys.Insert(key, key, true);
        remoteCloudClient->uploadedBytes = 0;
        remoteSystems.Push(packet->guid, remoteCloudClient);
    }
    else
    {
        remoteCloudClient = remoteSystems.ItemAtIndex(remoteSystemsHashIndex);
        bool objectExists;
        // Add to RemoteCloudClient::uploadedKeys if it isn't there already
        unsigned uploadedKeysIndex = remoteCloudClient->uploadedKeys.GetIndexFromKey(key, &objectExists);
        if (!objectExists)
            remoteCloudClient->uploadedKeys.InsertAtIndex(key, uploadedKeysIndex);
    }

    bool cloudDataAlreadyUploaded;
    unsigned dataRepositoryIndex;
    bool dataRepositoryExists;
    CloudDataList *cloudDataList = GetOrAllocateCloudDataList(key, &dataRepositoryExists, dataRepositoryIndex);
    if (!dataRepositoryExists)
    {
        cloudDataList->uploaderCount = 1;
        cloudDataAlreadyUploaded = false;
    }
    else
    {
        cloudDataAlreadyUploaded = cloudDataList->uploaderCount > 0;
        cloudDataList->uploaderCount++;
    }

    bool keyDataListExists;
    unsigned keyDataListIndex = cloudDataList->keyData.GetIndexFromKey(packet->guid, &keyDataListExists);
    CloudData *cloudData = nullptr;
    if (!keyDataListExists)
    {
        if (maxUploadBytesPerClient > 0 && remoteCloudClient->uploadedBytes + dataLengthBytes > maxUploadBytesPerClient)
        {
            // Undo prior insertion of cloudDataList into cloudData if needed
            if (!keyDataListExists)
            {
                delete cloudDataList;
                dataRepository.RemoveAtIndex(dataRepositoryIndex);
            }

            if (remoteCloudClient->IsUnused())
            {
                delete remoteCloudClient;
                remoteSystems.Remove(packet->guid);
            }

            if (dataLengthBytes > CLOUD_SERVER_DATA_STACK_SIZE)
                free(data);
            return;
        }

        cloudData =new CloudData;
        cloudData->dataLengthBytes=dataLengthBytes;
        cloudData->isUploaded=true;
        if (forceAddress!=UNASSIGNED_SYSTEM_ADDRESS)
        {
            cloudData->serverSystemAddress = forceAddress;
            cloudData->serverSystemAddress.SetPortHostOrder(
                    rakPeerInterface->GetExternalID(packet->systemAddress).GetPort());
        }
        else
        {
            cloudData->serverSystemAddress = rakPeerInterface->GetExternalID(packet->systemAddress);
            if (cloudData->serverSystemAddress.IsLoopback())
                cloudData->serverSystemAddress.FromString(rakPeerInterface->GetLocalIP(0));
        }
        if (cloudData->serverSystemAddress.GetPort() == 0)
        {
            // Fix localhost port
            cloudData->serverSystemAddress.SetPortHostOrder(rakPeerInterface->GetSocket(UNASSIGNED_SYSTEM_ADDRESS)->GetBoundAddress().GetPort());
        }
        cloudData->clientSystemAddress = packet->systemAddress;
        cloudData->serverGUID = rakPeerInterface->GetMyGUID();
        cloudData->clientGUID = packet->guid;
        cloudDataList->keyData.Insert(packet->guid, cloudData, true);
    }
    else
    {
        cloudData = cloudDataList->keyData[keyDataListIndex];

        if (!cloudDataAlreadyUploaded)
        {
            if (forceAddress != UNASSIGNED_SYSTEM_ADDRESS)
            {
                cloudData->serverSystemAddress = forceAddress;
                cloudData->serverSystemAddress.SetPortHostOrder(rakPeerInterface->GetExternalID(packet->systemAddress).GetPort());
            }
            else
                cloudData->serverSystemAddress = rakPeerInterface->GetExternalID(packet->systemAddress);

            if (cloudData->serverSystemAddress.GetPort() == 0)
            {
                // Fix localhost port
                cloudData->serverSystemAddress.SetPortHostOrder(rakPeerInterface->GetSocket(UNASSIGNED_SYSTEM_ADDRESS)->GetBoundAddress().GetPort());
            }

            cloudData->clientSystemAddress = packet->systemAddress;
        }

        if (maxUploadBytesPerClient > 0 &&
            remoteCloudClient->uploadedBytes - cloudData->dataLengthBytes + dataLengthBytes > maxUploadBytesPerClient)
        {
            // Undo prior insertion of cloudDataList into cloudData if needed
            if (!dataRepositoryExists)
            {
                delete cloudDataList;
                dataRepository.RemoveAtIndex(dataRepositoryIndex);
            }
            return;
        }
        else
            remoteCloudClient->uploadedBytes -= cloudData->dataLengthBytes; // Subtract already used bytes we are overwriting

        if (cloudData->allocatedData != 0)
            free(cloudData->allocatedData);
    }

    if (dataLengthBytes > CLOUD_SERVER_DATA_STACK_SIZE)
    {
        // Data already allocated
        cloudData->allocatedData = data;
        cloudData->dataPtr = data;
    }
    else
    {
        // Read to stack
        if (dataLengthBytes > 0)
            bsIn.ReadAlignedBytes(cloudData->stackData, dataLengthBytes);
        cloudData->allocatedData = 0;
        cloudData->dataPtr = cloudData->stackData;
    }
    // Update how many bytes were written for this data
    cloudData->dataLengthBytes = dataLengthBytes;
    remoteCloudClient->uploadedBytes += dataLengthBytes;

    if (!cloudDataAlreadyUploaded)
        SendUploadedKeyToServers(cloudDataList->key); // New data field

    // Existing data field changed
    NotifyClientSubscribersOfDataChange(cloudData, cloudDataList->key, cloudData->specificSubscribers, true);
    NotifyClientSubscribersOfDataChange(cloudData, cloudDataList->key, cloudDataList->nonSpecificSubscribers, true);

    // Send update to all remote servers that subscribed to this key
    NotifyServerSubscribersOfDataChange(cloudData, cloudDataList->key, true);

    // I could have also subscribed to a key not yet updated locally
    // This means I have to go through every RemoteClient that wants this key
    // Seems like cloudData->specificSubscribers is unnecessary in that case
}

void CloudServer::OnReleaseRequest(Packet *packet)
{
    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID));

    uint16_t keyCount;
    bsIn.Read(keyCount);

    if (keyCount == 0)
        return;

    DataStructures::HashIndex remoteSystemIndex = remoteSystems.GetIndexOf(packet->guid);
    if (remoteSystemIndex.IsInvalid())
        return;

    RemoteCloudClient *remoteCloudClient = remoteSystems.ItemAtIndex(remoteSystemIndex);

    // Read all in a list first so I can run filter on it
    DataStructures::List<CloudKey> cloudKeys;
    for (uint16_t keyCountIndex = 0; keyCountIndex < keyCount; keyCountIndex++)
    {
        CloudKey key;
        key.Serialize(false, &bsIn);
        cloudKeys.Push(key);
    }

    for (unsigned int filterIndex = 0; filterIndex < queryFilters.Size(); filterIndex++)
    {
        if (!queryFilters[filterIndex]->OnReleaseRequest(packet->guid, packet->systemAddress, cloudKeys))
            return;
    }

    for (uint16_t keyCountIndex = 0; keyCountIndex < keyCount; keyCountIndex++)
    {
        // Serialize in list above so I can run the filter on it
        // key.Serialize(false, &bsIn);
        CloudKey key = cloudKeys[keyCountIndex];

        // Remove remote systems uploaded keys
        bool objectExists;
        unsigned int uploadedKeysIndex = remoteCloudClient->uploadedKeys.GetIndexFromKey(key, &objectExists);
        if (objectExists)
        {
            bool dataRepositoryExists;
            unsigned int dataRepositoryIndex = dataRepository.GetIndexFromKey(key, &dataRepositoryExists);
            CloudDataList *cloudDataList = dataRepository[dataRepositoryIndex];
            RakAssert(cloudDataList);

            CloudData *cloudData;
            bool keyDataListExists;
            unsigned int keyDataListIndex = cloudDataList->keyData.GetIndexFromKey(packet->guid, &keyDataListExists);
            cloudData = cloudDataList->keyData[keyDataListIndex];

            remoteCloudClient->uploadedKeys.RemoveAtIndex(uploadedKeysIndex);
            remoteCloudClient->uploadedBytes -= cloudData->dataLengthBytes;
            cloudDataList->uploaderCount--;

            // Broadcast destruction of this key to subscribers
            NotifyClientSubscribersOfDataChange(cloudData, cloudDataList->key, cloudData->specificSubscribers, false);
            NotifyClientSubscribersOfDataChange(cloudData, cloudDataList->key, cloudDataList->nonSpecificSubscribers,
                                                false);
            NotifyServerSubscribersOfDataChange(cloudData, cloudDataList->key, false);

            cloudData->Clear();

            if (cloudData->IsUnused())
            {
                delete cloudData;
                cloudDataList->keyData.RemoveAtIndex(keyDataListIndex);
                if (cloudDataList->IsNotUploaded())
                {
                    // Tell other servers that this key is no longer uploaded, so they do not request it from us
                    RemoveUploadedKeyFromServers(cloudDataList->key);
                }

                if (cloudDataList->IsUnused())
                {
                    delete cloudDataList;
                    dataRepository.RemoveAtIndex(dataRepositoryIndex);
                }
            }

            if (remoteCloudClient->IsUnused())
            {
                delete remoteCloudClient;
                remoteSystems.RemoveAtIndex(remoteSystemIndex);
                break;
            }
        }
    }
}

void CloudServer::OnGetRequest(Packet *packet)
{
    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID));
    uint16_t specificSystemsCount;

    // Create a new GetRequest
    GetRequest *getRequest;
    getRequest = new GetRequest;
    getRequest->cloudQueryWithAddresses.cloudQuery.Serialize(false, &bsIn);
    getRequest->requestingClient = packet->guid;

    RakNetGUID addressOrGuid;
    bsIn.Read(specificSystemsCount);
    for (uint16_t i = 0; i < specificSystemsCount; i++)
    {
        bsIn.Read(addressOrGuid);
        getRequest->cloudQueryWithAddresses.specificSystems.Push(addressOrGuid);
    }

    if (getRequest->cloudQueryWithAddresses.cloudQuery.keys.Size() == 0)
    {
        delete getRequest;
        return;
    }

    for (unsigned filterIndex = 0; filterIndex < queryFilters.Size(); filterIndex++)
    {
        if (!queryFilters[filterIndex]->OnGetRequest(packet->guid, packet->systemAddress,
                                                     getRequest->cloudQueryWithAddresses.cloudQuery,
                                                     getRequest->cloudQueryWithAddresses.specificSystems))
            return;
    }

    getRequest->requestStartTime = RakNet::GetTime();
    getRequest->requestId = nextGetRequestId++;

    // Send request to servers that have this data
    DataStructures::List<RemoteServer *> remoteServersWithData;
    GetServersWithUploadedKeys(getRequest->cloudQueryWithAddresses.cloudQuery.keys, remoteServersWithData);

    if (remoteServersWithData.Size() == 0)
        ProcessAndTransmitGetRequest(getRequest);
    else
    {
        RakNet::BitStream bsOut;
        bsOut.Write((MessageID) ID_CLOUD_SERVER_TO_SERVER_COMMAND);
        bsOut.Write((MessageID) STSC_PROCESS_GET_REQUEST);
        getRequest->cloudQueryWithAddresses.Serialize(true, &bsOut);
        bsOut.Write(getRequest->requestId);

        for (unsigned remoteServerIndex = 0; remoteServerIndex < remoteServersWithData.Size(); remoteServerIndex++)
        {
            BufferedGetResponseFromServer *bufferedGetResponseFromServer = new BufferedGetResponseFromServer;
            bufferedGetResponseFromServer->serverAddress = remoteServersWithData[remoteServerIndex]->serverAddress;
            bufferedGetResponseFromServer->gotResult = false;
            getRequest->remoteServerResponses.Insert(remoteServersWithData[remoteServerIndex]->serverAddress, bufferedGetResponseFromServer, true);

            SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, remoteServersWithData[remoteServerIndex]->serverAddress, false);
        }

        // Record that this system made this request
        getRequests.Insert(getRequest->requestId, getRequest, true);
    }

    if (getRequest->cloudQueryWithAddresses.cloudQuery.subscribeToResults)
    {
        // Add to key subscription list for the client, which contains a keyId / specificUploaderList pair
        DataStructures::HashIndex remoteSystemsHashIndex = remoteSystems.GetIndexOf(packet->guid);
        RemoteCloudClient *remoteCloudClient;
        if (remoteSystemsHashIndex.IsInvalid())
        {
            remoteCloudClient = new RemoteCloudClient;
            remoteCloudClient->uploadedBytes = 0;
            remoteSystems.Push(packet->guid, remoteCloudClient);
        }
        else
            remoteCloudClient = remoteSystems.ItemAtIndex(remoteSystemsHashIndex);

        for (unsigned keyIndex = 0; keyIndex < getRequest->cloudQueryWithAddresses.cloudQuery.keys.Size(); keyIndex++)
        {
            CloudKey cloudKey = getRequest->cloudQueryWithAddresses.cloudQuery.keys[keyIndex];

            unsigned int keySubscriberIndex;
            bool hasKeySubscriber;
            keySubscriberIndex = remoteCloudClient->subscribedKeys.GetIndexFromKey(cloudKey, &hasKeySubscriber);
            KeySubscriberID *keySubscriberId;
            if (hasKeySubscriber)
            {
                DataStructures::List<RakNetGUID> specificSystems;
                UnsubscribeFromKey(remoteCloudClient, packet->guid, keySubscriberIndex, cloudKey, specificSystems);
            }

            keySubscriberId = new KeySubscriberID;
            keySubscriberId->key = cloudKey;

            for (unsigned systemIndex = 0; systemIndex < getRequest->cloudQueryWithAddresses.specificSystems.Size(); systemIndex++)
            {
                keySubscriberId->specificSystemsSubscribedTo.Insert(
                        getRequest->cloudQueryWithAddresses.specificSystems[systemIndex],
                        getRequest->cloudQueryWithAddresses.specificSystems[systemIndex], true);
            }

            remoteCloudClient->subscribedKeys.InsertAtIndex(keySubscriberId, keySubscriberIndex);

            // Add CloudData in a similar way
            unsigned dataRepositoryIndex;
            bool dataRepositoryExists;
            CloudDataList *cloudDataList = GetOrAllocateCloudDataList(cloudKey, &dataRepositoryExists, dataRepositoryIndex);

            // If this is the first local client to subscribe to this key, call SendSubscribedKeyToServers
            if (cloudDataList->subscriberCount == 0)
                SendSubscribedKeyToServers(cloudKey);

            // If the subscription is specific, may have to also allocate CloudData
            if (getRequest->cloudQueryWithAddresses.specificSystems.Size())
            {
                CloudData *cloudData;
                bool keyDataListExists;

                for (unsigned systemIndex = 0; systemIndex < getRequest->cloudQueryWithAddresses.specificSystems.Size(); systemIndex++)
                {
                    RakNetGUID specificSystem = getRequest->cloudQueryWithAddresses.specificSystems[systemIndex];

                    unsigned keyDataListIndex = cloudDataList->keyData.GetIndexFromKey(specificSystem,
                                                                                           &keyDataListExists);
                    if (!keyDataListExists)
                    {
                        cloudData = new CloudData;
                        cloudData->dataLengthBytes = 0;
                        cloudData->allocatedData = 0;
                        cloudData->isUploaded = false;
                        cloudData->dataPtr = 0;
                        cloudData->serverSystemAddress = UNASSIGNED_SYSTEM_ADDRESS;
                        cloudData->clientSystemAddress = UNASSIGNED_SYSTEM_ADDRESS;
                        cloudData->serverGUID = rakPeerInterface->GetMyGUID();
                        cloudData->clientGUID = specificSystem;
                        cloudDataList->keyData.Insert(specificSystem, cloudData, true);
                    }
                    else
                        cloudData = cloudDataList->keyData[keyDataListIndex];

                    ++cloudDataList->subscriberCount;
                    cloudData->specificSubscribers.Insert(packet->guid, packet->guid, true);
                }
            }
            else
            {
                ++cloudDataList->subscriberCount;
                cloudDataList->nonSpecificSubscribers.Insert(packet->guid, packet->guid, true);

                // Remove packet->guid from CloudData::specificSubscribers among all instances of cloudDataList->keyData
                bool subscribedKeysIndexExists;
                unsigned subscribedKeysIndex = remoteCloudClient->subscribedKeys.GetIndexFromKey(cloudDataList->key, &subscribedKeysIndexExists);
                if (subscribedKeysIndexExists)
                {
                    KeySubscriberID *keySubscriberId = remoteCloudClient->subscribedKeys[subscribedKeysIndex];
                    for (unsigned specificSystemIndex = 0; specificSystemIndex <
                                                  keySubscriberId->specificSystemsSubscribedTo.Size(); specificSystemIndex++)
                    {
                        bool keyDataExists;
                        unsigned keyDataIndex = cloudDataList->keyData.GetIndexFromKey(
                                keySubscriberId->specificSystemsSubscribedTo[specificSystemIndex], &keyDataExists);
                        if (keyDataExists)
                        {
                            CloudData *keyData = cloudDataList->keyData[keyDataIndex];
                            keyData->specificSubscribers.Remove(packet->guid);
                            --cloudDataList->subscriberCount;
                        }
                    }
                }
            }
        }

        if (remoteCloudClient->subscribedKeys.Size() == 0)
        {
            // Didn't do anything
            remoteSystems.Remove(packet->guid);
            delete remoteCloudClient;
        }
    }

    if (remoteServersWithData.Size() == 0)
        delete getRequest;
}

void CloudServer::OnUnsubscribeRequest(Packet *packet)
{
    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID));

    DataStructures::HashIndex remoteSystemIndex = remoteSystems.GetIndexOf(packet->guid);
    if (remoteSystemIndex.IsInvalid())
        return;

    RemoteCloudClient *remoteCloudClient = remoteSystems.ItemAtIndex(remoteSystemIndex);

    DataStructures::List<CloudKey> cloudKeys;
    DataStructures::List<RakNetGUID> specificSystems;

    uint16_t keyCount;
    bsIn.Read(keyCount);
    for (uint16_t index = 0; index < keyCount; index++)
    {
        CloudKey cloudKey;
        cloudKey.Serialize(false, &bsIn);
        cloudKeys.Push(cloudKey);
    }

    RakNetGUID specificSystem;
    uint16_t specificSystemCount;
    bsIn.Read(specificSystemCount);
    for (uint16_t index = 0; index < specificSystemCount; index++)
    {
        bsIn.Read(specificSystem);
        specificSystems.Push(specificSystem);
    }

    for (unsigned filterIndex = 0; filterIndex < queryFilters.Size(); filterIndex++)
    {
        if (!queryFilters[filterIndex]->OnUnsubscribeRequest(packet->guid, packet->systemAddress, cloudKeys, specificSystems))
            return;
    }

//    CloudDataList *cloudDataList;
    bool dataRepositoryExists;
//    unsigned int dataRepositoryIndex;

    for (uint16_t index = 0; index < keyCount; index++)
    {
        CloudKey cloudKey = cloudKeys[index];

        //    dataRepositoryIndex =
        dataRepository.GetIndexFromKey(cloudKey, &dataRepositoryExists);
        if (!dataRepositoryExists)
            continue;
//        cloudDataList = dataRepository[dataRepositoryIndex];

        bool hasKeySubscriber;
        unsigned keySubscriberIndex = remoteCloudClient->subscribedKeys.GetIndexFromKey(cloudKey, &hasKeySubscriber);

        if (!hasKeySubscriber)
            continue;

        UnsubscribeFromKey(remoteCloudClient, packet->guid, keySubscriberIndex, cloudKey, specificSystems);
    }

    if (remoteCloudClient->IsUnused())
    {
        delete remoteCloudClient;
        remoteSystems.RemoveAtIndex(remoteSystemIndex);
    }
}

void CloudServer::OnServerToServerGetRequest(Packet *packet)
{
    bool objectExists;

    remoteServers.GetIndexFromKey(packet->guid, &objectExists);
    if (!objectExists)
        return;

    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID) * 2);

    CloudQueryWithAddresses cloudQueryWithAddresses;
    uint32_t requestId;
    cloudQueryWithAddresses.Serialize(false, &bsIn);
    bsIn.Read(requestId);

    DataStructures::List<CloudData *> cloudDataResultList;
    DataStructures::List<CloudKey> cloudKeyResultList;
    ProcessCloudQueryWithAddresses(cloudQueryWithAddresses, cloudDataResultList, cloudKeyResultList);

    RakNet::BitStream bsOut;
    bsOut.Write((MessageID) ID_CLOUD_SERVER_TO_SERVER_COMMAND);
    bsOut.Write((MessageID) STSC_PROCESS_GET_RESPONSE);
    bsOut.Write(requestId);
    WriteCloudQueryRowFromResultList(cloudDataResultList, cloudKeyResultList, &bsOut);
    SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->guid, false);
}

void CloudServer::OnServerToServerGetResponse(Packet *packet)
{
    bool objectExists;
    unsigned remoteServerIndex = remoteServers.GetIndexFromKey(packet->guid, &objectExists);
    if (!objectExists)
        return;

    RemoteServer *remoteServer = remoteServers[remoteServerIndex];
    if (remoteServer == 0)
        return;

    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID) * 2);

    uint32_t requestId;
    bsIn.Read(requestId);

    // Lookup request id
    bool hasGetRequest;
    unsigned getRequestIndex = getRequests.GetIndexFromKey(requestId, &hasGetRequest);
    if (!hasGetRequest)
        return;
    GetRequest *getRequest = getRequests[getRequestIndex];
    bool hasRemoteServer;
    unsigned remoteServerResponsesIndex = getRequest->remoteServerResponses.GetIndexFromKey(packet->guid, &hasRemoteServer);
    if (!hasRemoteServer)
        return;
    BufferedGetResponseFromServer *bufferedGetResponseFromServer;
    bufferedGetResponseFromServer = getRequest->remoteServerResponses[remoteServerResponsesIndex];
    if (bufferedGetResponseFromServer->gotResult)
        return;
    bufferedGetResponseFromServer->gotResult = true;
    uint32_t numRows;
    bufferedGetResponseFromServer->queryResult.SerializeNumRows(false, numRows, &bsIn);
    bufferedGetResponseFromServer->queryResult.SerializeCloudQueryRows(false, numRows, &bsIn, this);

    // If all results returned, then also process locally, and return to user
    if (getRequest->AllRemoteServersHaveResponded())
    {
        ProcessAndTransmitGetRequest(getRequest);

        getRequest->Clear(this);
        delete getRequest;

        getRequests.RemoveAtIndex(getRequestIndex);
    }
}

void CloudServer::OnClosedConnection(const SystemAddress &systemAddress, RakNetGUID rakNetGUID, PI2_LostConnectionReason lostConnectionReason)
{
    (void) lostConnectionReason;
    (void) systemAddress;

    bool objectExists;
    unsigned remoteServerIndex = remoteServers.GetIndexFromKey(rakNetGUID, &objectExists);
    if (objectExists)
    {
        // Update remoteServerResponses by removing this server and sending the response if it is now complete
        unsigned getRequestIndex = 0;
        while (getRequestIndex < getRequests.Size())
        {
            GetRequest *getRequest = getRequests[getRequestIndex];
            bool waitingForThisServer;
            unsigned remoteServerResponsesIndex = getRequest->remoteServerResponses.GetIndexFromKey(rakNetGUID, &waitingForThisServer);
            if (waitingForThisServer)
            {
                getRequest->remoteServerResponses[remoteServerResponsesIndex]->Clear(this);
                delete getRequest->remoteServerResponses[remoteServerResponsesIndex];
                getRequest->remoteServerResponses.RemoveAtIndex(remoteServerResponsesIndex);

                if (getRequest->AllRemoteServersHaveResponded())
                {
                    ProcessAndTransmitGetRequest(getRequest);
                    getRequest->Clear(this);
                    delete getRequest;

                    getRequests.RemoveAtIndex(getRequestIndex);
                }
                else
                    getRequestIndex++;
            }
            else
                getRequestIndex++;
        }

        delete remoteServers[remoteServerIndex];
        remoteServers.RemoveAtIndex(remoteServerIndex);
    }

    DataStructures::HashIndex remoteSystemIndex = remoteSystems.GetIndexOf(rakNetGUID);
    if (!remoteSystemIndex.IsInvalid())
    {
        RemoteCloudClient *remoteCloudClient = remoteSystems.ItemAtIndex(remoteSystemIndex);
        for (unsigned uploadedKeysIndex = 0; uploadedKeysIndex < remoteCloudClient->uploadedKeys.Size(); uploadedKeysIndex++)
        {
            // Delete keys this system has uploaded
            bool keyDataRepositoryExists;
            unsigned dataRepositoryIndex = dataRepository.GetIndexFromKey(remoteCloudClient->uploadedKeys[uploadedKeysIndex], &keyDataRepositoryExists);
            if (keyDataRepositoryExists)
            {
                CloudDataList *cloudDataList = dataRepository[dataRepositoryIndex];
                bool keyDataExists;
                unsigned keyDataIndex = cloudDataList->keyData.GetIndexFromKey(rakNetGUID, &keyDataExists);
                if (keyDataExists)
                {
                    CloudData *cloudData = cloudDataList->keyData[keyDataIndex];
                    cloudDataList->uploaderCount--;

                    NotifyClientSubscribersOfDataChange(cloudData, cloudDataList->key, cloudData->specificSubscribers, false);
                    NotifyClientSubscribersOfDataChange(cloudData, cloudDataList->key, cloudDataList->nonSpecificSubscribers, false);
                    NotifyServerSubscribersOfDataChange(cloudData, cloudDataList->key, false);

                    cloudData->Clear();

                    if (cloudData->IsUnused())
                    {
                        delete cloudData;
                        cloudDataList->keyData.RemoveAtIndex(keyDataIndex);

                        if (cloudDataList->IsNotUploaded())
                        {
                            // Tell other servers that this key is no longer uploaded, so they do not request it from us
                            RemoveUploadedKeyFromServers(cloudDataList->key);
                        }

                        if (cloudDataList->IsUnused())
                        {
                            // Tell other servers that this key is no longer uploaded, so they do not request it from us
                            RemoveUploadedKeyFromServers(cloudDataList->key);

                            delete cloudDataList;
                            dataRepository.RemoveAtIndex(dataRepositoryIndex);
                        }
                    }
                }
            }
        }

        for (unsigned subscribedKeysIndex = 0; subscribedKeysIndex < remoteCloudClient->subscribedKeys.Size(); subscribedKeysIndex++)
        {
            KeySubscriberID *keySubscriberId;
            keySubscriberId = remoteCloudClient->subscribedKeys[subscribedKeysIndex];

            bool keyDataRepositoryExists;
            unsigned keyDataRepositoryIndex = dataRepository.GetIndexFromKey(
                    remoteCloudClient->subscribedKeys[subscribedKeysIndex]->key, &keyDataRepositoryExists);
            if (keyDataRepositoryExists)
            {
                CloudDataList *cloudDataList = dataRepository[keyDataRepositoryIndex];
                if (keySubscriberId->specificSystemsSubscribedTo.Size() == 0)
                {
                    cloudDataList->nonSpecificSubscribers.Remove(rakNetGUID);
                    --cloudDataList->subscriberCount;
                }
                else
                {
                    for (unsigned systemIndex = 0; systemIndex < keySubscriberId->specificSystemsSubscribedTo.Size(); systemIndex++)
                    {
                        bool keyDataExists;
                        unsigned keyDataIndex = cloudDataList->keyData.GetIndexFromKey(
                                keySubscriberId->specificSystemsSubscribedTo[systemIndex], &keyDataExists);
                        if (keyDataExists)
                        {
                            CloudData *keyData = cloudDataList->keyData[keyDataIndex];
                            keyData->specificSubscribers.Remove(rakNetGUID);
                            --cloudDataList->subscriberCount;
                        }
                    }
                }
            }

            delete keySubscriberId;
        }

        // Delete and remove from remoteSystems
        delete remoteCloudClient;
        remoteSystems.RemoveAtIndex(remoteSystemIndex);
    }
}

void CloudServer::OnRakPeerShutdown(void)
{
    Clear();
}

void CloudServer::Clear(void)
{
    for (unsigned i = 0; i < dataRepository.Size(); i++)
    {
        CloudDataList *cloudDataList = dataRepository[i];
        for (unsigned j = 0; j < cloudDataList->keyData.Size(); j++)
        {
            cloudDataList->keyData[j]->Clear();
            delete cloudDataList->keyData[j];
        }
        delete cloudDataList;
    }
    dataRepository.Clear(false);

    for (unsigned i = 0; i < remoteServers.Size(); i++)
        delete remoteServers[i];

    remoteServers.Clear(false);

    for (unsigned i = 0; i < getRequests.Size(); i++)
    {
        GetRequest *getRequest = getRequests[i];
        getRequest->Clear(this);
        delete getRequests[i];
    }
    getRequests.Clear(false);

    DataStructures::List<RakNetGUID> keyList;
    DataStructures::List<RemoteCloudClient *> itemList;
    remoteSystems.GetAsList(itemList, keyList);
    for (unsigned i = 0; i < itemList.Size(); i++)
    {
        RemoteCloudClient *remoteCloudClient = itemList[i];
        for (unsigned j = 0; j < remoteCloudClient->subscribedKeys.Size(); j++)
            delete remoteCloudClient->subscribedKeys[j];

        delete remoteCloudClient;
    }
    remoteSystems.Clear();
}

void CloudServer::WriteCloudQueryRowFromResultList(DataStructures::List<CloudData *> &cloudDataResultList,
                                                   DataStructures::List<CloudKey> &cloudKeyResultList, BitStream *bsOut)
{
    bsOut->WriteCasted<uint32_t>(cloudKeyResultList.Size());

    for (unsigned i = 0; i < cloudKeyResultList.Size(); i++)
        WriteCloudQueryRowFromResultList(i, cloudDataResultList, cloudKeyResultList, bsOut);
}

void
CloudServer::WriteCloudQueryRowFromResultList(unsigned int i, DataStructures::List<CloudData *> &cloudDataResultList,
                                              DataStructures::List<CloudKey> &cloudKeyResultList, BitStream *bsOut)
{
    CloudQueryRow cloudQueryRow;
    CloudData *cloudData = cloudDataResultList[i];
    cloudQueryRow.key = cloudKeyResultList[i];
    cloudQueryRow.data = cloudData->dataPtr;
    cloudQueryRow.length = cloudData->dataLengthBytes;
    cloudQueryRow.serverSystemAddress = cloudData->serverSystemAddress;
    cloudQueryRow.clientSystemAddress = cloudData->clientSystemAddress;
    cloudQueryRow.serverGUID = cloudData->serverGUID;
    cloudQueryRow.clientGUID = cloudData->clientGUID;
    cloudQueryRow.Serialize(true, bsOut, 0);
}

void CloudServer::NotifyClientSubscribersOfDataChange(CloudData *cloudData, CloudKey &key,
                                                      DataStructures::OrderedList<RakNetGUID, RakNetGUID> &subscribers,
                                                      bool wasUpdated)
{
    RakNet::BitStream bsOut;
    bsOut.Write((MessageID) ID_CLOUD_SUBSCRIPTION_NOTIFICATION);
    bsOut.Write(wasUpdated);
    CloudQueryRow row;
    row.key = key;
    row.data = cloudData->dataPtr;
    row.length = cloudData->dataLengthBytes;
    row.serverSystemAddress = cloudData->serverSystemAddress;
    row.clientSystemAddress = cloudData->clientSystemAddress;
    row.serverGUID = cloudData->serverGUID;
    row.clientGUID = cloudData->clientGUID;
    row.Serialize(true, &bsOut, 0);

    for (unsigned i = 0; i < subscribers.Size(); i++)
        SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, subscribers[i], false);
}

void CloudServer::NotifyClientSubscribersOfDataChange(CloudQueryRow *row,
                                                      DataStructures::OrderedList<RakNetGUID, RakNetGUID> &subscribers,
                                                      bool wasUpdated)
{
    RakNet::BitStream bsOut;
    bsOut.Write((MessageID) ID_CLOUD_SUBSCRIPTION_NOTIFICATION);
    bsOut.Write(wasUpdated);
    row->Serialize(true, &bsOut, 0);

    for (unsigned i = 0; i < subscribers.Size(); i++)
        SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, subscribers[i], false);
}

void CloudServer::NotifyServerSubscribersOfDataChange(CloudData *cloudData, CloudKey &key, bool wasUpdated)
{
    // Find every server that has subscribed
    // Send them change notifications
    RakNet::BitStream bsOut;
    bsOut.Write((MessageID) ID_CLOUD_SERVER_TO_SERVER_COMMAND);
    bsOut.Write((MessageID) STSC_DATA_CHANGED);
    bsOut.Write(wasUpdated);
    CloudQueryRow row;
    row.key = key;
    row.data = cloudData->dataPtr;
    row.length = cloudData->dataLengthBytes;
    row.serverSystemAddress = cloudData->serverSystemAddress;
    row.clientSystemAddress = cloudData->clientSystemAddress;
    row.serverGUID = cloudData->serverGUID;
    row.clientGUID = cloudData->clientGUID;
    row.Serialize(true, &bsOut, 0);

    for (unsigned i = 0; i < remoteServers.Size(); i++)
    {
        if (!remoteServers[i]->gotSubscribedAndUploadedKeys || remoteServers[i]->subscribedKeys.HasData(key))
            SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, remoteServers[i]->serverAddress, false);
    }
}

void CloudServer::AddServer(RakNetGUID systemIdentifier)
{
    ConnectionState cs = rakPeerInterface->GetConnectionState(systemIdentifier);
    if (cs == IS_DISCONNECTED || cs == IS_NOT_CONNECTED)
        return;
    bool objectExists;
    unsigned index = remoteServers.GetIndexFromKey(systemIdentifier, &objectExists);
    if (!objectExists)
    {
        RemoteServer *remoteServer = new RemoteServer;
        remoteServer->gotSubscribedAndUploadedKeys = false;
        remoteServer->serverAddress = systemIdentifier;
        remoteServers.InsertAtIndex(remoteServer, index);

        SendUploadedAndSubscribedKeysToServer(systemIdentifier);
    }
}

void CloudServer::RemoveServer(RakNetGUID systemAddress)
{
    bool objectExists;
    unsigned index = remoteServers.GetIndexFromKey(systemAddress, &objectExists);
    if (objectExists)
    {
        delete remoteServers[index];
        remoteServers.RemoveAtIndex(index);
    }
}

void CloudServer::GetRemoteServers(DataStructures::List<RakNetGUID> &remoteServersOut)
{
    remoteServersOut.Clear(true);

    for (unsigned i = 0; i < remoteServers.Size(); i++)
        remoteServersOut.Push(remoteServers[i]->serverAddress);
}

void CloudServer::ProcessAndTransmitGetRequest(GetRequest *getRequest)
{
    RakNet::BitStream bsOut;
    bsOut.Write((MessageID) ID_CLOUD_GET_RESPONSE);

    //    BufferedGetResponseFromServer getResponse;
    CloudQueryResult cloudQueryResult;
    cloudQueryResult.cloudQuery = getRequest->cloudQueryWithAddresses.cloudQuery;
    cloudQueryResult.subscribeToResults = getRequest->cloudQueryWithAddresses.cloudQuery.subscribeToResults;
    cloudQueryResult.SerializeHeader(true, &bsOut);

    DataStructures::List<CloudData *> cloudDataResultList;
    DataStructures::List<CloudKey> cloudKeyResultList;
    ProcessCloudQueryWithAddresses(getRequest->cloudQueryWithAddresses, cloudDataResultList, cloudKeyResultList);
    bool unlimitedRows = getRequest->cloudQueryWithAddresses.cloudQuery.maxRowsToReturn == 0;

    uint32_t localNumRows = (uint32_t) cloudDataResultList.Size();
    if (!unlimitedRows &&
        localNumRows > getRequest->cloudQueryWithAddresses.cloudQuery.startingRowIndex &&
        localNumRows - getRequest->cloudQueryWithAddresses.cloudQuery.startingRowIndex >
        getRequest->cloudQueryWithAddresses.cloudQuery.maxRowsToReturn)
        localNumRows = getRequest->cloudQueryWithAddresses.cloudQuery.startingRowIndex +
                       getRequest->cloudQueryWithAddresses.cloudQuery.maxRowsToReturn;

    BitSize_t bitStreamOffset = bsOut.GetWriteOffset();
    uint32_t localRowsToWrite;
    unsigned skipRows;
    if (localNumRows > getRequest->cloudQueryWithAddresses.cloudQuery.startingRowIndex)
    {
        localRowsToWrite = localNumRows - getRequest->cloudQueryWithAddresses.cloudQuery.startingRowIndex;
        skipRows = 0;
    }
    else
    {
        localRowsToWrite = 0;
        skipRows = getRequest->cloudQueryWithAddresses.cloudQuery.startingRowIndex - localNumRows;
    }

    cloudQueryResult.SerializeNumRows(true, localRowsToWrite, &bsOut);
    for (unsigned i = getRequest->cloudQueryWithAddresses.cloudQuery.startingRowIndex; i < localNumRows; i++)
        WriteCloudQueryRowFromResultList(i, cloudDataResultList, cloudKeyResultList, &bsOut);

    // Append remote systems for remaining rows
    if (unlimitedRows || getRequest->cloudQueryWithAddresses.cloudQuery.maxRowsToReturn > localRowsToWrite)
    {
        uint32_t remainingRows = 0;
        uint32_t additionalRowsWritten = 0;
        if (!unlimitedRows)
            remainingRows = getRequest->cloudQueryWithAddresses.cloudQuery.maxRowsToReturn - localRowsToWrite;

        for (unsigned remoteServerResponseIndex = 0; remoteServerResponseIndex < getRequest->remoteServerResponses.Size(); remoteServerResponseIndex++)
        {
            BufferedGetResponseFromServer *bufferedGetResponseFromServer = getRequest->remoteServerResponses[remoteServerResponseIndex];
            for (unsigned rowIndex = 0; rowIndex < bufferedGetResponseFromServer->queryResult.rowsReturned.Size(); rowIndex++)
            {
                if (skipRows > 0)
                {
                    --skipRows;
                    continue;
                }
                bufferedGetResponseFromServer->queryResult.rowsReturned[rowIndex]->Serialize(true, &bsOut, this);

                ++additionalRowsWritten;
                if (!unlimitedRows && --remainingRows == 0)
                    break;
            }

            if (!unlimitedRows && remainingRows == 0)
                break;
        }

        if (additionalRowsWritten > 0)
        {
            BitSize_t curOffset = bsOut.GetWriteOffset();
            bsOut.SetWriteOffset(bitStreamOffset);
            localRowsToWrite += additionalRowsWritten;
            cloudQueryResult.SerializeNumRows(true, localRowsToWrite, &bsOut);
            bsOut.SetWriteOffset(curOffset);
        }
    }

    SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, getRequest->requestingClient, false);
}

void CloudServer::ProcessCloudQueryWithAddresses(CloudServer::CloudQueryWithAddresses &queryWithAddresses,
                                                 DataStructures::List<CloudData *> &cloudDataResultList,
                                                 DataStructures::List<CloudKey> &cloudKeyResultList)
{
    CloudQueryResult cloudQueryResult;
    CloudQueryRow cloudQueryRow;


    // If specificSystems list empty, applies to all systems
    // For each of keys in queryWithAddresses, return that data, limited by maxRowsToReturn
    for (unsigned queryIndex = 0; queryIndex < queryWithAddresses.cloudQuery.keys.Size(); queryIndex++)
    {
        const CloudKey &key = queryWithAddresses.cloudQuery.keys[queryIndex];

        bool dataRepositoryExists;
        unsigned int dataRepositoryIndex = dataRepository.GetIndexFromKey(key, &dataRepositoryExists);
        if (dataRepositoryExists)
        {
            CloudDataList *cloudDataList = dataRepository[dataRepositoryIndex];

            if (cloudDataList->uploaderCount > 0)
            {
                // Return all keyData that was uploaded by specificSystems, or all if not specified
                if (queryWithAddresses.specificSystems.Size() > 0)
                {
                    // Return data for matching systems
                    for (unsigned systemIndex = 0; systemIndex < queryWithAddresses.specificSystems.Size(); systemIndex++)
                    {
                        bool uploaderExists;
                        unsigned keyDataIndex = cloudDataList->keyData.GetIndexFromKey(queryWithAddresses.specificSystems[systemIndex], &uploaderExists);
                        if (uploaderExists)
                        {
                            cloudDataResultList.Push(cloudDataList->keyData[keyDataIndex]);
                            cloudKeyResultList.Push(key);
                        }
                    }
                }
                else
                {
                    // Return data for all systems
                    for (unsigned keyDataIndex = 0; keyDataIndex < cloudDataList->keyData.Size(); keyDataIndex++)
                    {
                        cloudDataResultList.Push(cloudDataList->keyData[keyDataIndex]);
                        cloudKeyResultList.Push(key);
                    }
                }
            }
        }
    }
}

void CloudServer::SendUploadedAndSubscribedKeysToServer(RakNetGUID systemAddress)
{
    RakNet::BitStream bsOut;
    bsOut.Write((MessageID) ID_CLOUD_SERVER_TO_SERVER_COMMAND);
    bsOut.Write((MessageID) STSC_ADD_UPLOADED_AND_SUBSCRIBED_KEYS);
    bsOut.WriteCasted<uint16_t>(dataRepository.Size());
    for (unsigned i = 0; i < dataRepository.Size(); i++)
        dataRepository[i]->key.Serialize(true, &bsOut);

    BitSize_t startOffset, endOffset;
    uint16_t subscribedKeyCount = 0;
    startOffset = bsOut.GetWriteOffset();
    bsOut.WriteCasted<uint16_t>(subscribedKeyCount);
    for (unsigned i = 0; i < dataRepository.Size(); i++)
    {
        if (dataRepository[i]->subscriberCount > 0)
        {
            dataRepository[i]->key.Serialize(true, &bsOut);
            subscribedKeyCount++;
        }
    }
    endOffset = bsOut.GetWriteOffset();
    bsOut.SetWriteOffset(startOffset);
    bsOut.WriteCasted<uint16_t>(subscribedKeyCount);
    bsOut.SetWriteOffset(endOffset);

    if (dataRepository.Size() > 0 || subscribedKeyCount > 0)
        SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, systemAddress, false);
}

void CloudServer::SendUploadedKeyToServers(CloudKey &cloudKey)
{
    RakNet::BitStream bsOut;
    bsOut.Write((MessageID) ID_CLOUD_SERVER_TO_SERVER_COMMAND);
    bsOut.Write((MessageID) STSC_ADD_UPLOADED_KEY);
    cloudKey.Serialize(true, &bsOut);
    for (unsigned int i = 0; i < remoteServers.Size(); i++)
        SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, remoteServers[i]->serverAddress, false);
}

void CloudServer::SendSubscribedKeyToServers(CloudKey &cloudKey)
{
    RakNet::BitStream bsOut;
    bsOut.Write((MessageID) ID_CLOUD_SERVER_TO_SERVER_COMMAND);
    bsOut.Write((MessageID) STSC_ADD_SUBSCRIBED_KEY);
    cloudKey.Serialize(true, &bsOut);
    for (unsigned int i = 0; i < remoteServers.Size(); i++)
        SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, remoteServers[i]->serverAddress, false);
}

void CloudServer::RemoveUploadedKeyFromServers(CloudKey &cloudKey)
{
    RakNet::BitStream bsOut;
    bsOut.Write((MessageID) ID_CLOUD_SERVER_TO_SERVER_COMMAND);
    bsOut.Write((MessageID) STSC_REMOVE_UPLOADED_KEY);
    cloudKey.Serialize(true, &bsOut);
    for (unsigned int i = 0; i < remoteServers.Size(); i++)
        SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, remoteServers[i]->serverAddress, false);
}

void CloudServer::RemoveSubscribedKeyFromServers(CloudKey &cloudKey)
{
    RakNet::BitStream bsOut;
    bsOut.Write((MessageID) ID_CLOUD_SERVER_TO_SERVER_COMMAND);
    bsOut.Write((MessageID) STSC_REMOVE_SUBSCRIBED_KEY);
    cloudKey.Serialize(true, &bsOut);
    for (unsigned i = 0; i < remoteServers.Size(); i++)
        SendUnified(&bsOut, HIGH_PRIORITY, RELIABLE_ORDERED, 0, remoteServers[i]->serverAddress, false);
}

void CloudServer::OnSendUploadedAndSubscribedKeysToServer(Packet *packet)
{
    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID) * 2);

    bool objectExists;
    unsigned index = remoteServers.GetIndexFromKey(packet->guid, &objectExists);
    if (!objectExists)
        return;
    RemoteServer *remoteServer = remoteServers[index];
    remoteServer->gotSubscribedAndUploadedKeys = true;

//    unsigned int insertionIndex;
    bool alreadyHasKey;
    uint16_t numUploadedKeys, numSubscribedKeys;
    bsIn.Read(numUploadedKeys);
    for (uint16_t i = 0; i < numUploadedKeys; i++)
    {
        CloudKey cloudKey;
        cloudKey.Serialize(false, &bsIn);

        //    insertionIndex =
        remoteServer->uploadedKeys.GetIndexFromKey(cloudKey, &alreadyHasKey);
        if (!alreadyHasKey)
            remoteServer->uploadedKeys.Insert(cloudKey, cloudKey, true);
    }

    bsIn.Read(numSubscribedKeys);
    for (uint16_t i = 0; i < numSubscribedKeys; i++)
    {
        CloudKey cloudKey;
        cloudKey.Serialize(false, &bsIn);

        //insertionIndex =
        remoteServer->subscribedKeys.GetIndexFromKey(cloudKey, &alreadyHasKey);
        if (!alreadyHasKey)
            remoteServer->subscribedKeys.Insert(cloudKey, cloudKey, true);
    }

    // Potential todo - join servers
    // For each uploaded key that we subscribe to, query it
    // For each subscribed key that we have, send it
}

void CloudServer::OnSendUploadedKeyToServers(Packet *packet)
{
    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID) * 2);

    bool objectExists;
    unsigned index = remoteServers.GetIndexFromKey(packet->guid, &objectExists);
    if (!objectExists)
        return;
    RemoteServer *remoteServer = remoteServers[index];
    CloudKey cloudKey;
    cloudKey.Serialize(false, &bsIn);
//    unsigned int insertionIndex;
    bool alreadyHasKey;
//    insertionIndex =
    remoteServer->uploadedKeys.GetIndexFromKey(cloudKey, &alreadyHasKey);
    if (!alreadyHasKey)
        remoteServer->uploadedKeys.Insert(cloudKey, cloudKey, true);
}

void CloudServer::OnSendSubscribedKeyToServers(Packet *packet)
{
    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID) * 2);

    bool objectExists;
    unsigned index = remoteServers.GetIndexFromKey(packet->guid, &objectExists);
    if (!objectExists)
        return;
    RemoteServer *remoteServer = remoteServers[index];
    CloudKey cloudKey;
    cloudKey.Serialize(false, &bsIn);
//    unsigned int insertionIndex;
    bool alreadyHasKey;
//    insertionIndex =
    remoteServer->subscribedKeys.GetIndexFromKey(cloudKey, &alreadyHasKey);

    // Do not need to send current values, the Get request will do that as the Get request is sent at the same time
    if (!alreadyHasKey)
        remoteServer->subscribedKeys.Insert(cloudKey, cloudKey, true);
}

void CloudServer::OnRemoveUploadedKeyFromServers(Packet *packet)
{
    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID) * 2);

    bool objectExists;
    unsigned index = remoteServers.GetIndexFromKey(packet->guid, &objectExists);
    if (!objectExists)
        return;
    RemoteServer *remoteServer = remoteServers[index];
    CloudKey cloudKey;
    cloudKey.Serialize(false, &bsIn);
    bool alreadyHasKey;
    unsigned insertionIndex = remoteServer->uploadedKeys.GetIndexFromKey(cloudKey, &alreadyHasKey);
    if (alreadyHasKey)
        remoteServer->uploadedKeys.RemoveAtIndex(insertionIndex);
}

void CloudServer::OnRemoveSubscribedKeyFromServers(Packet *packet)
{
    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID) * 2);

    bool objectExists;
    unsigned int index = remoteServers.GetIndexFromKey(packet->guid, &objectExists);
    if (!objectExists)
        return;
    RemoteServer *remoteServer = remoteServers[index];
    CloudKey cloudKey;
    cloudKey.Serialize(false, &bsIn);
    unsigned int insertionIndex;
    bool alreadyHasKey;
    insertionIndex = remoteServer->subscribedKeys.GetIndexFromKey(cloudKey, &alreadyHasKey);
    if (alreadyHasKey)
        remoteServer->subscribedKeys.RemoveAtIndex(insertionIndex);
}

void CloudServer::OnServerDataChanged(Packet *packet)
{
    RakNet::BitStream bsIn(packet->data, packet->length, false);
    bsIn.IgnoreBytes(sizeof(MessageID) * 2);

    bool objectExists;
    remoteServers.GetIndexFromKey(packet->guid, &objectExists);
    if (!objectExists)
        return;

    // Find everyone that cares about this change and relay
    bool wasUpdated = false;
    bsIn.Read(wasUpdated);
    CloudQueryRow row;
    row.Serialize(false, &bsIn, this);

    bool dataRepositoryExists;
    unsigned dataRepositoryIndex = dataRepository.GetIndexFromKey(row.key, &dataRepositoryExists);
    if (!dataRepositoryExists)
    {
        DeallocateRowData(row.data);
        return;
    }
    CloudDataList *cloudDataList = dataRepository[dataRepositoryIndex];
    bool keyDataListExists;
    unsigned keyDataListIndex = cloudDataList->keyData.GetIndexFromKey(row.clientGUID, &keyDataListExists);
    if (keyDataListExists)
    {
        CloudData *cloudData = cloudDataList->keyData[keyDataListIndex];
        NotifyClientSubscribersOfDataChange(&row, cloudData->specificSubscribers, wasUpdated);
    }

    NotifyClientSubscribersOfDataChange(&row, cloudDataList->nonSpecificSubscribers, wasUpdated);
    DeallocateRowData(row.data);
}

void CloudServer::GetServersWithUploadedKeys(DataStructures::List<CloudKey> &keys,
                                             DataStructures::List<CloudServer::RemoteServer *> &remoteServersWithData)
{
    remoteServersWithData.Clear(true);

    for (unsigned i = 0; i < remoteServers.Size(); i++)
        remoteServers[i]->workingFlag = false;

    for (unsigned i = 0; i < remoteServers.Size(); i++)
    {
        if (!remoteServers[i]->workingFlag)
        {
            if (!remoteServers[i]->gotSubscribedAndUploadedKeys)
            {
                remoteServers[i]->workingFlag = true;
                remoteServersWithData.Push(remoteServers[i]);
            }
            else
            {
                remoteServers[i]->workingFlag = false;
                for (unsigned j = 0; j < keys.Size(); j++)
                {
                    if (!remoteServers[i]->workingFlag && remoteServers[i]->uploadedKeys.HasData(keys[j]))
                    {
                        remoteServers[i]->workingFlag = true;
                        remoteServersWithData.Push(remoteServers[i]);
                        break;
                    }
                }
            }
        }
    }
}

CloudServer::CloudDataList *
CloudServer::GetOrAllocateCloudDataList(CloudKey key, bool *dataRepositoryExists, unsigned int &dataRepositoryIndex)
{
    CloudDataList *cloudDataList;

    dataRepositoryIndex = dataRepository.GetIndexFromKey(key, dataRepositoryExists);
    if (!*dataRepositoryExists)
    {
        cloudDataList = new CloudDataList;
        cloudDataList->key = key;
        cloudDataList->uploaderCount = 0;
        cloudDataList->subscriberCount = 0;
        dataRepository.InsertAtIndex(cloudDataList, dataRepositoryIndex);
    }
    else
        cloudDataList = dataRepository[dataRepositoryIndex];

    return cloudDataList;
}

void CloudServer::UnsubscribeFromKey(RemoteCloudClient *remoteCloudClient, RakNetGUID remoteCloudClientGuid,
                                     unsigned int keySubscriberIndex, CloudKey &cloudKey,
                                     DataStructures::List<RakNetGUID> &specificSystems)
{
    KeySubscriberID *keySubscriberId = remoteCloudClient->subscribedKeys[keySubscriberIndex];

    // If removing specific systems, but global subscription, fail
    if (keySubscriberId->specificSystemsSubscribedTo.Size() == 0 && specificSystems.Size() > 0)
        return;

    bool dataRepositoryExists;
    CloudDataList *cloudDataList;
    unsigned dataRepositoryIndex = dataRepository.GetIndexFromKey(cloudKey, &dataRepositoryExists);
    if (!dataRepositoryExists)
        return;

    cloudDataList = dataRepository[dataRepositoryIndex];
    if (specificSystems.Size() == 0)
    {
        // Remove global subscriber. If returns false, have to remove specific subscribers
        if (!cloudDataList->RemoveSubscriber(remoteCloudClientGuid))
        {
            for (unsigned i = 0; i < keySubscriberId->specificSystemsSubscribedTo.Size(); i++)
                RemoveSpecificSubscriber(keySubscriberId->specificSystemsSubscribedTo[i], cloudDataList, remoteCloudClientGuid);
        }
        keySubscriberId->specificSystemsSubscribedTo.Clear(true);
    }
    else
    {
        for (unsigned j = 0; j < specificSystems.Size(); j++)
        {
            bool hasSpecificSystemsSubscribedTo;
            unsigned specificSystemsSubscribedToIndex = keySubscriberId->specificSystemsSubscribedTo.GetIndexFromKey(
                    specificSystems[j], &hasSpecificSystemsSubscribedTo);
            if (hasSpecificSystemsSubscribedTo)
            {
                RemoveSpecificSubscriber(specificSystems[j], cloudDataList, remoteCloudClientGuid);
                keySubscriberId->specificSystemsSubscribedTo.RemoveAtIndex(specificSystemsSubscribedToIndex);
            }
        }
    }

    if (keySubscriberId->specificSystemsSubscribedTo.Size() == 0)
    {
        delete keySubscriberId;
        remoteCloudClient->subscribedKeys.RemoveAtIndex(keySubscriberIndex);
    }

    if (cloudDataList->subscriberCount == 0)
        RemoveSubscribedKeyFromServers(cloudKey);

    if (cloudDataList->IsUnused())
    {
        delete cloudDataList;
        dataRepository.RemoveAtIndex(dataRepositoryIndex);
    }
}

void CloudServer::RemoveSpecificSubscriber(RakNetGUID specificSubscriber, CloudDataList *cloudDataList, RakNetGUID remoteCloudClientGuid)
{
    bool keyDataListExists;
    unsigned int keyDataListIndex = cloudDataList->keyData.GetIndexFromKey(specificSubscriber, &keyDataListExists);
    if (keyDataListExists == false)
        return;

    CloudData *cloudData = cloudDataList->keyData[keyDataListIndex];
    bool hasSpecificSubscriber;
    unsigned int specificSubscriberIndex = cloudData->specificSubscribers.GetIndexFromKey(remoteCloudClientGuid, &hasSpecificSubscriber);

    if (hasSpecificSubscriber)
    {
        cloudData->specificSubscribers.RemoveAtIndex(specificSubscriberIndex);
        cloudDataList->subscriberCount--;

        if (cloudData->IsUnused())
        {
            delete cloudData;
            cloudDataList->keyData.RemoveAtIndex(keyDataListIndex);
        }
    }
}

void CloudServer::ForceExternalSystemAddress(SystemAddress forcedAddress)
{
    forceAddress = forcedAddress;
}

void CloudServer::AddQueryFilter(CloudServerQueryFilter *filter)
{
    if (queryFilters.GetIndexOf(filter) != (unsigned int) -1)
        return;
    queryFilters.Push(filter);
}

void CloudServer::RemoveQueryFilter(CloudServerQueryFilter *filter)
{
    size_t index = queryFilters.GetIndexOf(filter);
    if (index != (size_t) -1)
        queryFilters.RemoveAtIndex(index);
}

void CloudServer::RemoveAllQueryFilters(void)
{
    queryFilters.Clear(true);
}

#endif
