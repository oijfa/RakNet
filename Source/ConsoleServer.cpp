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

#if _RAKNET_SUPPORT_ConsoleServer == 1

#include "ConsoleServer.h"
#include "TransportInterface.h"
#include "CommandParserInterface.h"
#include <string.h>
#include <stdlib.h>

#define COMMAND_DELINATOR ' '
#define COMMAND_DELINATOR_TOGGLE '"'

#include "Utils/LinuxStrings.h"

using namespace RakNet;

STATIC_FACTORY_DEFINITIONS(ConsoleServer, ConsoleServer)

ConsoleServer::ConsoleServer()
{
    transport = 0;
    password[0] = 0;
    prompt = 0;
}

ConsoleServer::~ConsoleServer()
{
    if (prompt)
        free(prompt);
}

void ConsoleServer::SetTransportProvider(TransportInterface *transportInterface, unsigned short port)
{
    // Replace the current TransportInterface, stopping the old one, if present, and starting the new one.
    if (transportInterface)
    {
        if (transport)
        {
            RemoveCommandParser(transport->GetCommandParser());
            transport->Stop();
        }
        transport = transportInterface;
        transport->Start(port, true);

        unsigned i;
        for (i = 0; i < commandParserList.Size(); i++)
            commandParserList[i]->OnTransportChange(transport);

        //  The transport itself might have a command parser - for example password for the RakNet transport
        AddCommandParser(transport->GetCommandParser());
    }
}

void ConsoleServer::AddCommandParser(CommandParserInterface *commandParserInterface)
{
    if (commandParserInterface == 0)
        return;

    // Non-duplicate insertion
    for (unsigned i = 0; i < commandParserList.Size(); i++)
    {
        if (commandParserList[i] == commandParserInterface)
            return;

        if (_stricmp(commandParserList[i]->GetName(), commandParserInterface->GetName()) == 0)
        {
            // Naming conflict between two command parsers
            RakAssert(0);
            return;
        }
    }

    commandParserList.Insert(commandParserInterface);
    if (transport)
        commandParserInterface->OnTransportChange(transport);
}

void ConsoleServer::RemoveCommandParser(CommandParserInterface *commandParserInterface)
{
    if (commandParserInterface == 0)
        return;

    // Overwrite the element we are removing from the back of the list and delete the back of the list
    for (unsigned i = 0; i < commandParserList.Size(); i++)
    {
        if (commandParserList[i] == commandParserInterface)
        {
            commandParserList[i] = commandParserList[commandParserList.Size() - 1];
            commandParserList.RemoveFromEnd();
            return;
        }
    }
}

void ConsoleServer::Update(void)
{
    RakNet::RegisteredCommand rc;
    RakNet::Packet *packet = transport->Receive();
    RakNet::SystemAddress newOrLostConnectionId = transport->HasNewIncomingConnection();

    if (newOrLostConnectionId != UNASSIGNED_SYSTEM_ADDRESS)
    {
        for (unsigned i = 0; i < commandParserList.Size(); i++)
            commandParserList[i]->OnNewIncomingConnection(newOrLostConnectionId, transport);

        transport->Send(newOrLostConnectionId, "Connected to remote command console.\r\nType 'help' for help.\r\n");
        ListParsers(newOrLostConnectionId);
        ShowPrompt(newOrLostConnectionId);
    }

    newOrLostConnectionId = transport->HasLostConnection();
    if (newOrLostConnectionId != UNASSIGNED_SYSTEM_ADDRESS)
    {
        for (unsigned i = 0; i < commandParserList.Size(); i++)
            commandParserList[i]->OnConnectionLost(newOrLostConnectionId, transport);
    }

    char *parameterList[20]; // Up to 20 parameters
    while (packet)
    {
        bool commandParsed = false;
        char copy[REMOTE_MAX_TEXT_INPUT];
        memcpy(copy, packet->data, packet->length);
        copy[packet->length] = 0;
        unsigned numParameters;
        RakNet::CommandParserInterface::ParseConsoleString((char *) packet->data, COMMAND_DELINATOR,
                                                           COMMAND_DELINATOR_TOGGLE, &numParameters, parameterList,
                                                           20); // Up to 20 parameters
        if (numParameters == 0)
        {
            transport->DeallocatePacket(packet);
            packet = transport->Receive();
            continue;
        }
        if (_stricmp(*parameterList, "help") == 0 && numParameters <= 2)
        {
            // Find the parser specified and display help for it
            if (numParameters == 1)
            {
                transport->Send(packet->systemAddress, "\r\nINSTRUCTIONS:\r\n");
                transport->Send(packet->systemAddress,
                                "Enter commands on your keyboard, using spaces to delineate parameters.\r\n");
                transport->Send(packet->systemAddress, "You can use quotation marks to toggle space delineation.\r\n");
                transport->Send(packet->systemAddress, "You can connect multiple times from the same computer.\r\n");
                transport->Send(packet->systemAddress,
                                "You can direct commands to a parser by prefixing the parser name or number.\r\n");
                transport->Send(packet->systemAddress, "COMMANDS:\r\n");
                transport->Send(packet->systemAddress, "help                                        Show this display.\r\n");
                transport->Send(packet->systemAddress,
                                "help <ParserName>                           Show help on a particular parser.\r\n");
                transport->Send(packet->systemAddress,
                                "help <CommandName>                          Show help on a particular command.\r\n");
                transport->Send(packet->systemAddress,
                                "quit                                        Disconnects from the server.\r\n");
                transport->Send(packet->systemAddress, "[<ParserName>]   <Command> [<Parameters>]   Execute a command\r\n");
                transport->Send(packet->systemAddress, "[<ParserNumber>] <Command> [<Parameters>]   Execute a command\r\n");
                ListParsers(packet->systemAddress);
                //ShowPrompt(packet->systemAddress);
            }
            else // numParameters == 2, including the help tag
            {
                for (unsigned i = 0; i < commandParserList.Size(); i++)
                {
                    if (_stricmp(parameterList[1], commandParserList[i]->GetName()) == 0)
                    {
                        commandParsed = true;
                        commandParserList[i]->SendHelp(transport, packet->systemAddress);
                        transport->Send(packet->systemAddress, "COMMAND LIST:\r\n");
                        commandParserList[i]->SendCommandList(transport, packet->systemAddress);
                        transport->Send(packet->systemAddress, "\r\n");
                        break;
                    }
                }

                if (!commandParsed)
                {
                    // Try again, for all commands for all parsers.
                    for (unsigned i = 0; i < commandParserList.Size(); i++)
                    {
                        if (commandParserList[i]->GetRegisteredCommand(parameterList[1], &rc))
                        {
                            if (rc.parameterCount == RakNet::CommandParserInterface::VARIABLE_NUMBER_OF_PARAMETERS)
                                transport->Send(packet->systemAddress, "(Variable parms): %s %s\r\n", rc.command, rc.commandHelp);
                            else
                                transport->Send(packet->systemAddress, "(%i parms): %s %s\r\n", rc.parameterCount, rc.command, rc.commandHelp);
                            commandParsed = true;
                            break;
                        }
                    }
                }

                if (!commandParsed)
                {
                    // Don't know what to do
                    transport->Send(packet->systemAddress, "Unknown help topic: %s.\r\n", parameterList[1]);
                }
                //ShowPrompt(packet->systemAddress);
            }
        }
        else if (_stricmp(*parameterList, "quit") == 0 && numParameters == 1)
        {
            transport->Send(packet->systemAddress, "Goodbye!\r\n");
            transport->CloseConnection(packet->systemAddress);
        }
        else
        {
            bool tryAllParsers = true;
            bool failed = false;

            if (numParameters >= 2) // At minimum <CommandParserName> <Command>
            {
                unsigned commandParserIndex = (unsigned) -1;
                // Prefixing with numbers directs to a particular parser
                if (**parameterList >= '0' && **parameterList <= '9')
                {
                    commandParserIndex = atoi(*parameterList); // Use specified parser unless it's an invalid number
                    commandParserIndex--; // Subtract 1 since we displayed numbers starting at index+1
                    if (commandParserIndex >= commandParserList.Size())
                    {
                        transport->Send(packet->systemAddress, "Invalid index.\r\n");
                        failed = true;
                    }
                }
                else
                {
                    // // Prefixing with the name of a command parser directs to that parser.  See if the first word matches a parser
                    for (unsigned i = 0; i < commandParserList.Size(); i++)
                    {
                        if (_stricmp(parameterList[0], commandParserList[i]->GetName()) == 0)
                        {
                            commandParserIndex = i; // Matches parser at index i
                            break;
                        }
                    }
                }

                if (!failed)
                {
                    // -1 means undirected, so otherwise this is directed to a target
                    if (commandParserIndex != (unsigned) -1)
                    {
                        // Only this parser should use this command
                        tryAllParsers = false;
                        if (commandParserList[commandParserIndex]->GetRegisteredCommand(parameterList[1], &rc))
                        {
                            commandParsed = true;
                            if (rc.parameterCount == CommandParserInterface::VARIABLE_NUMBER_OF_PARAMETERS ||
                                rc.parameterCount == numParameters - 2)
                                commandParserList[commandParserIndex]->OnCommand(rc.command, numParameters - 2,
                                                                                 parameterList + 2, transport,
                                                                                 packet->systemAddress, copy);
                            else
                                transport->Send(packet->systemAddress, "Invalid parameter count.\r\n(%i parms): %s %s\r\n",
                                                rc.parameterCount, rc.command, rc.commandHelp);
                        }
                    }
                }
            }

            if (!failed && tryAllParsers)
            {
                for (unsigned i = 0; i < commandParserList.Size(); i++)
                {
                    // Undirected command.  Try all the parsers to see if they understand the command
                    // Pass the 1nd element as the command, and the remainder as the parameter list
                    if (commandParserList[i]->GetRegisteredCommand(parameterList[0], &rc))
                    {
                        commandParsed = true;

                        if (rc.parameterCount == CommandParserInterface::VARIABLE_NUMBER_OF_PARAMETERS ||
                            rc.parameterCount == numParameters - 1)
                            commandParserList[i]->OnCommand(rc.command, numParameters - 1, parameterList + 1, transport,
                                                            packet->systemAddress, copy);
                        else
                            transport->Send(packet->systemAddress, "Invalid parameter count.\r\n(%i parms): %s %s\r\n",
                                            rc.parameterCount, rc.command, rc.commandHelp);
                    }
                }
            }
            if (!commandParsed && commandParserList.Size() > 0)
                transport->Send(packet->systemAddress, "Unknown command:  Type 'help' for help.\r\n");

        }

        ShowPrompt(packet->systemAddress);

        transport->DeallocatePacket(packet);
        packet = transport->Receive();
    }
}

void ConsoleServer::ListParsers(SystemAddress systemAddress)
{
    transport->Send(systemAddress, "INSTALLED PARSERS:\r\n");
    for (unsigned i = 0; i < commandParserList.Size(); i++)
        transport->Send(systemAddress, "%i. %s\r\n", i + 1, commandParserList[i]->GetName());
}

void ConsoleServer::ShowPrompt(SystemAddress systemAddress)
{
    transport->Send(systemAddress, prompt);
}

void ConsoleServer::SetPrompt(const char *_prompt)
{
    if (prompt)
        free(prompt);
    if (_prompt && _prompt[0])
    {
        size_t len = strlen(_prompt);
        prompt = (char *) malloc(len + 1);
        strcpy(prompt, _prompt);
    }
    else
        prompt = 0;
}

#endif // _RAKNET_SUPPORT_*
