/*
 *  Copyright (c) 2014, Oculus VR, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant 
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include "FileOperations.h"

#if _RAKNET_SUPPORT_FileOperations == 1

#include <stdio.h>
#include <string.h>
#include <errno.h>

#ifdef _WIN32
// For mkdir
#include <direct.h>
#include <io.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#include "_FindFirst.h"
#endif

#ifndef MAX_PATH
#define MAX_PATH 260
#endif

bool WriteFileWithDirectories(const char *path, char *data, size_t dataLength)
{
    size_t index;
    FILE *fp;
    char pathCopy[MAX_PATH];
    int res;

    if (path == 0 || path[0] == 0)
        return false;

    strcpy(pathCopy, path);

    // Ignore first / if there is one
    if (pathCopy[0])
    {
        index = 1;
        while (pathCopy[index])
        {
            if (pathCopy[index] == '/' || pathCopy[index] == '\\')
            {
                pathCopy[index] = 0;

#ifdef _WIN32
                res = _mkdir( pathCopy );
#else
                res = mkdir(pathCopy, 0744);
#endif

                if (res < 0 && errno != EEXIST && errno != EACCES)
                    return false;

                pathCopy[index] = '/';
            }

            index++;
        }
    }

    if (data)
    {
        fp = fopen(path, "wb");

        if (fp == 0)
            return false;

        fwrite(data, 1, dataLength, fp);

        fclose(fp);
    }
    else
    {

#ifdef _WIN32
        res = _mkdir( pathCopy );
#else
        res = mkdir(pathCopy, 0744);
#endif

        if (res < 0 && errno != EEXIST)
            return false;
    }

    return true;
}

bool IsSlash(unsigned char c)
{
    return c == '/' || c == '\\';
}

void AddSlash(char *input)
{
    if (input == 0 || input[0] == 0)
        return;

    int lastCharIndex = (int) strlen(input) - 1;
    if (input[lastCharIndex] == '\\')
        input[lastCharIndex] = '/';
    else if (input[lastCharIndex] != '/')
    {
        input[lastCharIndex + 1] = '/';
        input[lastCharIndex + 2] = 0;
    }
}

bool DirectoryExists(const char *directory)
{
    _finddata_t fileInfo;
    intptr_t dir;
    char baseDirWithStars[560];
    strcpy(baseDirWithStars, directory);
    AddSlash(baseDirWithStars);
    strcat(baseDirWithStars, "*.*");
    dir = _findfirst(baseDirWithStars, &fileInfo);
    if (dir == -1)
        return false;
    _findclose(dir);
    return true;
}

void QuoteIfSpaces(char *str)
{
    bool hasSpace = false;
    for (unsigned i = 0; str[i]; i++)
    {
        if (str[i] == ' ')
        {
            hasSpace = true;
            break;
        }
    }
    if (hasSpace)
    {
        size_t len = strlen(str);
        memmove(str + 1, str, len);
        str[0] = '\"';
        str[len] = '\"';
        str[len + 1] = 0;
    }
}

long GetFileLength(const char *path)
{
    FILE *fp = fopen(path, "rb");
    if (fp == 0) return 0;
    fseek(fp, 0, SEEK_END);
    long fileLength = ftell(fp);
    fclose(fp);
    return fileLength;
}

#endif // _RAKNET_SUPPORT_FileOperations

