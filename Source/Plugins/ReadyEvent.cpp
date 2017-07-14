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

#if _RAKNET_SUPPORT_ReadyEvent == 1

#include "ReadyEvent.h"
#include "RakPeerInterface.h"
#include "BitStream.h"
#include "MessageIdentifiers.h"
#include "RakAssert.h"

#ifdef _MSC_VER
#pragma warning( push )
#endif

using namespace RakNet;

int RakNet::ReadyEvent::RemoteSystemCompByGuid(const RakNetGUID &key, const RemoteSystem &data)
{
    if (key < data.rakNetGuid)
        return -1;
    else if (key == data.rakNetGuid)
        return 0;
    else
        return 1;
}

int RakNet::ReadyEvent::ReadyEventNodeComp(const unsigned &key, ReadyEvent::ReadyEventNode *const &data)
{
    if (key < data->eventId)
        return -1;
    else if (key == data->eventId)
        return 0;
    else
        return 1;
}

STATIC_FACTORY_DEFINITIONS(ReadyEvent, ReadyEvent)

ReadyEvent::ReadyEvent()
{
    channel = 0;
}

ReadyEvent::~ReadyEvent()
{
    Clear();
}


bool ReadyEvent::SetEvent(unsigned eventId, bool isReady)
{
    bool objectExists;
    unsigned eventIndex = (unsigned) readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (!objectExists)
        CreateNewEvent(eventId, isReady); // Totally new event
    else
        return SetEventByIndex(eventIndex, isReady);

    return true;
}

void ReadyEvent::ForceCompletion(unsigned eventId)
{
    bool objectExists;
    unsigned eventIndex = (unsigned) readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (!objectExists)
    {
        // Totally new event
        CreateNewEvent(eventId, true);
        eventIndex = (unsigned) readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    }

    ReadyEventNode *ren = readyEventNodeList[eventIndex];
    ren->eventStatus = ID_READY_EVENT_FORCE_ALL_SET;
    UpdateReadyStatus(eventIndex);
}

bool ReadyEvent::DeleteEvent(unsigned eventId)
{
    bool objectExists;
    unsigned eventIndex = (unsigned) readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
    {
        delete readyEventNodeList[eventIndex];
        readyEventNodeList.RemoveAtIndex(eventIndex);
        return true;
    }
    return false;
}

bool ReadyEvent::IsEventSet(unsigned eventId)
{
    bool objectExists;
    size_t eventIndex = readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
        return readyEventNodeList[eventIndex]->eventStatus == ID_READY_EVENT_SET ||
                readyEventNodeList[eventIndex]->eventStatus == ID_READY_EVENT_ALL_SET;
    return false;
}

bool ReadyEvent::IsEventCompletionProcessing(unsigned eventId) const
{
    bool objectExists;
    size_t eventIndex = readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
    {
        bool anyAllReady = false;
        bool allAllReady = true;
        ReadyEventNode *ren = readyEventNodeList[eventIndex];
        if (ren->eventStatus == ID_READY_EVENT_FORCE_ALL_SET)
            return false;
        for (size_t i = 0; i < ren->systemList.Size(); i++)
        {
            if (ren->systemList[i].lastReceivedStatus == ID_READY_EVENT_ALL_SET)
                anyAllReady = true;
            else
                allAllReady = false;
        }
        return anyAllReady && !allAllReady;
    }
    return false;
}

bool ReadyEvent::IsEventCompleted(unsigned eventId) const
{
    bool objectExists;
    unsigned eventIndex = (unsigned) readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
        return IsEventCompletedByIndex(eventIndex);

    return false;
}

bool ReadyEvent::HasEvent(unsigned eventId)
{
    return readyEventNodeList.HasData(eventId);
}

size_t ReadyEvent::GetEventListSize(void) const
{
    return readyEventNodeList.Size();
}

int ReadyEvent::GetEventAtIndex(unsigned index) const
{
    return readyEventNodeList[index]->eventId;
}

bool ReadyEvent::AddToWaitList(unsigned eventId, RakNetGUID guid)
{
    bool eventExists;
    unsigned eventIndex = (unsigned) readyEventNodeList.GetIndexFromKey(eventId, &eventExists);
    if (!eventExists)
        eventIndex = CreateNewEvent(eventId, false);

    // Don't do this, otherwise if we are trying to start a 3 player game, it will not allow the 3rd player to hit ready if the first two players have already done so
    //if (IsLocked(eventIndex))
    //    return false; // Not in the list, but event is already completed, or is starting to complete, and adding more waiters would fail this.

    size_t numAdded = 0;
    if (guid == UNASSIGNED_RAKNET_GUID)
    {
        for (size_t i = 0; i < rakPeerInterface->GetMaximumNumberOfPeers(); i++)
        {
            RakNetGUID firstGuid = rakPeerInterface->GetGUIDFromIndex(i);
            if (firstGuid != UNASSIGNED_RAKNET_GUID)
                numAdded += AddToWaitListInternal(eventIndex, firstGuid);
        }
    }
    else
        numAdded = (size_t) AddToWaitListInternal(eventIndex, guid);

    if (numAdded > 0)
        UpdateReadyStatus(eventIndex);
    return numAdded > 0;
}

bool ReadyEvent::RemoveFromWaitList(unsigned eventId, RakNetGUID guid)
{
    bool eventExists;
    unsigned eventIndex = (unsigned) readyEventNodeList.GetIndexFromKey(eventId, &eventExists);
    if (eventExists)
    {
        if (guid == UNASSIGNED_RAKNET_GUID)
        {
            // Remove all waiters
            readyEventNodeList[eventIndex]->systemList.Clear(false);
            UpdateReadyStatus(eventIndex);
        }
        else
        {
            bool systemExists;
            size_t systemIndex = readyEventNodeList[eventIndex]->systemList.GetIndexFromKey(guid, &systemExists);
            if (systemExists)
            {
                bool isCompleted = IsEventCompletedByIndex(eventIndex);
                readyEventNodeList[eventIndex]->systemList.RemoveAtIndex(systemIndex);

                if (!isCompleted && IsEventCompletedByIndex(eventIndex))
                    PushCompletionPacket(readyEventNodeList[eventIndex]->eventId);

                UpdateReadyStatus(eventIndex);

                return true;
            }
        }
    }

    return false;
}

bool ReadyEvent::IsInWaitList(unsigned eventId, RakNetGUID guid)
{
    bool objectExists;
    size_t readyIndex = readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
        return readyEventNodeList[readyIndex]->systemList.HasData(guid);

    return false;
}

size_t ReadyEvent::GetRemoteWaitListSize(unsigned eventId) const
{
    bool objectExists;
    size_t readyIndex = readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
        return readyEventNodeList[readyIndex]->systemList.Size();
    return 0;
}

RakNetGUID ReadyEvent::GetFromWaitListAtIndex(unsigned eventId, unsigned index) const
{
    bool objectExists;
    size_t readyIndex = readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
        return readyEventNodeList[readyIndex]->systemList[index].rakNetGuid;
    return UNASSIGNED_RAKNET_GUID;
}

ReadyEventSystemStatus ReadyEvent::GetReadyStatus(unsigned eventId, RakNetGUID guid)
{
    bool objectExists;
    size_t readyIndex = readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
    {
        ReadyEventNode *ren = readyEventNodeList[readyIndex];
        size_t systemIndex = ren->systemList.GetIndexFromKey(guid, &objectExists);
        if (!objectExists)
            return RES_NOT_WAITING;
        if (ren->systemList[systemIndex].lastReceivedStatus == ID_READY_EVENT_SET)
            return RES_READY;
        if (ren->systemList[systemIndex].lastReceivedStatus == ID_READY_EVENT_UNSET)
            return RES_WAITING;
        if (ren->systemList[systemIndex].lastReceivedStatus == ID_READY_EVENT_ALL_SET)
            return RES_ALL_READY;
    }

    return RES_UNKNOWN_EVENT;
}

void ReadyEvent::SetSendChannel(unsigned char newChannel)
{
    channel = newChannel;
}

PluginReceiveResult ReadyEvent::OnReceive(Packet *packet)
{
    unsigned char packetIdentifier = packet->data[0];

//    bool doPrint = packet->systemAddress.GetPort()==60002 || rakPeerInterface->GetInternalID(UNASSIGNED_SYSTEM_ADDRESS).GetPort()==60002;

    switch (packetIdentifier)
    {
        case ID_READY_EVENT_UNSET:
        case ID_READY_EVENT_SET:
        case ID_READY_EVENT_ALL_SET:
//        if (doPrint) {if (packet->systemAddress.GetPort()==60002)    RAKNET_DEBUG_PRINTF("FROM 60002: "); else if (rakPeerInterface->GetInternalID(UNASSIGNED_SYSTEM_ADDRESS).port==60002)    RAKNET_DEBUG_PRINTF("TO 60002: "); RAKNET_DEBUG_PRINTF("ID_READY_EVENT_SET\n");}
            OnReadyEventPacketUpdate(packet);
            return RR_CONTINUE_PROCESSING;
        case ID_READY_EVENT_FORCE_ALL_SET:
            OnReadyEventForceAllSet(packet);
            return RR_CONTINUE_PROCESSING;
        case ID_READY_EVENT_QUERY:
//        if (doPrint) {if (packet->systemAddress.GetPort()==60002)    RAKNET_DEBUG_PRINTF("FROM 60002: "); else if (rakPeerInterface->GetInternalID(UNASSIGNED_SYSTEM_ADDRESS).port==60002)    RAKNET_DEBUG_PRINTF("TO 60002: "); RAKNET_DEBUG_PRINTF("ID_READY_EVENT_QUERY\n");}
            OnReadyEventQuery(packet);
            return RR_STOP_PROCESSING_AND_DEALLOCATE;
        default:
            return RR_CONTINUE_PROCESSING;
    }

}

bool ReadyEvent::AddToWaitListInternal(unsigned eventIndex, RakNetGUID guid)
{
    ReadyEventNode *ren = readyEventNodeList[eventIndex];
    bool objectExists;
    size_t systemIndex = ren->systemList.GetIndexFromKey(guid, &objectExists);
    if (!objectExists)
    {
        RemoteSystem rs;
        rs.lastReceivedStatus = ID_READY_EVENT_UNSET;
        rs.lastSentStatus = ID_READY_EVENT_UNSET;
        rs.rakNetGuid = guid;
        ren->systemList.InsertAtIndex(rs, systemIndex);

        SendReadyStateQuery(ren->eventId, guid);
        return true;
    }
    return false;
}

void ReadyEvent::OnReadyEventForceAllSet(Packet *packet)
{
    RakNet::BitStream incomingBitStream(packet->data, packet->length, false);
    incomingBitStream.IgnoreBits(8);
    unsigned eventId;
    incomingBitStream.Read(eventId);
    bool objectExists;
    size_t readyIndex = readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
    {
        ReadyEventNode *ren = readyEventNodeList[readyIndex];
        if (ren->eventStatus != ID_READY_EVENT_FORCE_ALL_SET)
        {
            ren->eventStatus = ID_READY_EVENT_FORCE_ALL_SET;
            PushCompletionPacket(ren->eventId);
        }
    }
}

void ReadyEvent::OnReadyEventPacketUpdate(Packet *packet)
{
    RakNet::BitStream incomingBitStream(packet->data, packet->length, false);
    incomingBitStream.IgnoreBits(8);
    unsigned eventId;
    incomingBitStream.Read(eventId);
    bool objectExists;
    unsigned readyIndex = (unsigned) readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
    {
        ReadyEventNode *ren = readyEventNodeList[readyIndex];
        bool systemExists;
        size_t systemIndex = ren->systemList.GetIndexFromKey(packet->guid, &systemExists);
        if (systemExists)
        {
            // Just return if no change
            if (ren->systemList[systemIndex].lastReceivedStatus == packet->data[0])
                return;

            bool wasCompleted = IsEventCompletedByIndex(readyIndex);
            ren->systemList[systemIndex].lastReceivedStatus = packet->data[0];
            // If forced all set, doesn't matter what the new packet is
            if (ren->eventStatus == ID_READY_EVENT_FORCE_ALL_SET)
                return;
            UpdateReadyStatus(readyIndex);
            if (!wasCompleted && IsEventCompletedByIndex(readyIndex))
                PushCompletionPacket(readyIndex);
        }
    }
}

void ReadyEvent::OnReadyEventQuery(Packet *packet)
{
    RakNet::BitStream incomingBitStream(packet->data, packet->length, false);
    incomingBitStream.IgnoreBits(8);
    unsigned eventId;
    incomingBitStream.Read(eventId);
    bool objectExists;
    unsigned readyIndex = (unsigned) readyEventNodeList.GetIndexFromKey(eventId, &objectExists);
    if (objectExists)
    {
        unsigned systemIndex = (unsigned) readyEventNodeList[readyIndex]->systemList.GetIndexFromKey(packet->guid, &objectExists);
        // Force the non-default send, because our initial send may have arrived at a system that didn't yet create the ready event
        if (objectExists)
            SendReadyUpdate(readyIndex, systemIndex, true);
    }
}

void ReadyEvent::OnClosedConnection(const SystemAddress &systemAddress, RakNetGUID rakNetGUID, PI2_LostConnectionReason lostConnectionReason)
{
    (void) systemAddress;
    (void) rakNetGUID;
    (void) lostConnectionReason;

    RemoveFromAllLists(rakNetGUID);
}

void ReadyEvent::OnRakPeerShutdown(void)
{
    Clear();
}

bool ReadyEvent::SetEventByIndex(unsigned eventIndex, bool isReady)
{
    ReadyEventNode *ren = readyEventNodeList[eventIndex];
    if ((ren->eventStatus == ID_READY_EVENT_ALL_SET || ren->eventStatus == ID_READY_EVENT_SET) && isReady)
        return false; // Success - no change
    if (ren->eventStatus == ID_READY_EVENT_UNSET && !isReady)
        return false; // Success - no change
    if (ren->eventStatus == ID_READY_EVENT_FORCE_ALL_SET)
        return false; // Can't change

    if (isReady)
        ren->eventStatus = ID_READY_EVENT_SET;
    else
        ren->eventStatus = ID_READY_EVENT_UNSET;

    UpdateReadyStatus(eventIndex);

    // Check if now completed, and if so, tell the user about it
    if (IsEventCompletedByIndex(eventIndex))
        PushCompletionPacket(ren->eventId);

    return true;
}

bool ReadyEvent::IsEventCompletedByIndex(unsigned eventIndex) const
{
    ReadyEventNode *ren = readyEventNodeList[eventIndex];
    if (ren->eventStatus == ID_READY_EVENT_FORCE_ALL_SET)
        return true;
    if (ren->eventStatus != ID_READY_EVENT_ALL_SET)
        return false;
    for (size_t i = 0; i < ren->systemList.Size(); i++)
        if (ren->systemList[i].lastReceivedStatus != ID_READY_EVENT_ALL_SET)
            return false;
    return true;
}

void ReadyEvent::Clear(void)
{
    for (size_t i = 0; i < readyEventNodeList.Size(); i++)
        delete readyEventNodeList[i];
    readyEventNodeList.Clear(false);
}

unsigned ReadyEvent::CreateNewEvent(unsigned eventId, bool isReady)
{
    ReadyEventNode *ren = new ReadyEventNode;
    ren->eventId = eventId;
    if (!isReady)
        ren->eventStatus = ID_READY_EVENT_UNSET;
    else
        ren->eventStatus = ID_READY_EVENT_SET;
    return (unsigned) readyEventNodeList.Insert(eventId, ren, true);
}

void ReadyEvent::UpdateReadyStatus(unsigned eventIndex)
{
    ReadyEventNode *ren = readyEventNodeList[eventIndex];
    bool anyUnset;

    if (ren->eventStatus == ID_READY_EVENT_SET)
    {
        // If you are set, and no other systems are ID_READY_EVENT_UNSET, then change your status to ID_READY_EVENT_ALL_SET
        anyUnset = false;
        for (size_t i = 0; i < ren->systemList.Size(); i++)
        {
            if (ren->systemList[i].lastReceivedStatus == ID_READY_EVENT_UNSET)
            {
                anyUnset = true;
                break;
            }
        }
        if (!anyUnset)
            ren->eventStatus = ID_READY_EVENT_ALL_SET;
    }
    else if (ren->eventStatus == ID_READY_EVENT_ALL_SET)
    {
        // If you are all set, and any systems are ID_READY_EVENT_UNSET, then change your status to ID_READY_EVENT_SET
        anyUnset = false;
        for (size_t i = 0; i < ren->systemList.Size(); i++)
        {
            if (ren->systemList[i].lastReceivedStatus == ID_READY_EVENT_UNSET)
            {
                anyUnset = true;
                break;
            }
        }
        if (anyUnset)
            ren->eventStatus = ID_READY_EVENT_SET;
    }
    BroadcastReadyUpdate(eventIndex, false);
}

void ReadyEvent::SendReadyUpdate(unsigned eventIndex, unsigned systemIndex, bool forceIfNotDefault)
{
    ReadyEventNode *ren = readyEventNodeList[eventIndex];
    RakNet::BitStream bs;
    // I do this rather than write true or false, so users that do not use BitStreams can still read the data
    if ((ren->eventStatus != ren->systemList[systemIndex].lastSentStatus) ||
        (forceIfNotDefault && ren->eventStatus != ID_READY_EVENT_UNSET))
    {
        bs.Write(ren->eventStatus);
        bs.Write(ren->eventId);
        SendUnified(&bs, HIGH_PRIORITY, RELIABLE_ORDERED, channel, ren->systemList[systemIndex].rakNetGuid, false);

        ren->systemList[systemIndex].lastSentStatus = ren->eventStatus;
    }

}

void ReadyEvent::BroadcastReadyUpdate(unsigned eventIndex, bool forceIfNotDefault)
{
    ReadyEventNode *ren = readyEventNodeList[eventIndex];
    for (unsigned systemIndex = 0; systemIndex < ren->systemList.Size(); systemIndex++)
        SendReadyUpdate(eventIndex, systemIndex, forceIfNotDefault);
}

void ReadyEvent::SendReadyStateQuery(unsigned eventId, RakNetGUID guid)
{
    RakNet::BitStream bs;
    bs.Write((MessageID) ID_READY_EVENT_QUERY);
    bs.Write(eventId);
    SendUnified(&bs, HIGH_PRIORITY, RELIABLE_ORDERED, channel, guid, false);
}

void ReadyEvent::RemoveFromAllLists(RakNetGUID guid)
{
    for (unsigned eventIndex = 0; eventIndex < readyEventNodeList.Size(); eventIndex++)
    {
        bool isCompleted = IsEventCompletedByIndex(eventIndex);
        bool systemExists;
        size_t systemIndex = readyEventNodeList[eventIndex]->systemList.GetIndexFromKey(guid, &systemExists);
        if (systemExists)
            readyEventNodeList[eventIndex]->systemList.RemoveAtIndex(systemIndex);

        UpdateReadyStatus(eventIndex);

        if (!isCompleted && IsEventCompletedByIndex(eventIndex))
            PushCompletionPacket(readyEventNodeList[eventIndex]->eventId);
    }
}

void ReadyEvent::PushCompletionPacket(unsigned eventId)
{
    (void) eventId;
    // Not necessary
    /*
    // Pass a packet to the user that we are now completed, as setting ourselves to signaled was the last thing being waited on
    Packet *p = AllocatePacketUnified(sizeof(MessageID)+sizeof(int));
    RakNet::BitStream bs(p->data, sizeof(MessageID)+sizeof(int), false);
    bs.SetWriteOffset(0);
    bs.Write((MessageID)ID_READY_EVENT_ALL_SET);
    bs.Write(eventId);
    rakPeerInterface->PushBackPacket(p, false);
    */
}

#ifdef _MSC_VER
#pragma warning( pop )
#endif

#endif // _RAKNET_SUPPORT_*
