/*
 *  Copyright (c) 2014, Oculus VR, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant 
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include "RakAssert.h"
#include "GridSectorizer.h"
//#include <stdlib.h>
#include <math.h>

GridSectorizer::GridSectorizer()
{
    grid = 0;
}

GridSectorizer::~GridSectorizer()
{
    if (grid)
        delete[] grid;
}

void GridSectorizer::Init(float _maxCellWidth, float _maxCellHeight, float minX, float minY, float maxX, float maxY)
{
    RakAssert(_maxCellWidth > 0.0f && _maxCellHeight > 0.0f);
    if (grid)
        delete[] grid;

    cellOriginX = minX;
    cellOriginY = minY;
    gridWidth = maxX - minX;
    gridHeight = maxY - minY;
    gridCellWidthCount = (unsigned) ceil(gridWidth / _maxCellWidth);
    gridCellHeightCount = (unsigned) ceil(gridHeight / _maxCellHeight);
    // Make the cells slightly smaller, so we allocate an extra unneeded cell if on the edge.
    // This way we don't go outside the array on rounding errors.
    cellWidth = gridWidth / gridCellWidthCount;
    cellHeight = gridHeight / gridCellHeightCount;
    invCellWidth = 1.0f / cellWidth;
    invCellHeight = 1.0f / cellHeight;

#ifdef _USE_ORDERED_LIST
    grid =new DataStructures::OrderedList<void*, void*>;
    DataStructures::OrderedList<void*,void*>::IMPLEMENT_DEFAULT_COMPARISON();
#else
    grid = new DataStructures::List<void *>[gridCellWidthCount * gridCellHeightCount];
#endif
}

void GridSectorizer::AddEntry(void *entry, float minX, float minY, float maxX, float maxY)
{
    RakAssert(cellWidth > 0.0f);
    RakAssert(minX < maxX && minY < maxY);

    unsigned xStart = WorldToCellXOffsetAndClamped(minX);
    unsigned yStart = WorldToCellYOffsetAndClamped(minY);
    unsigned xEnd = WorldToCellXOffsetAndClamped(maxX);
    unsigned yEnd = WorldToCellYOffsetAndClamped(maxY);

    for (unsigned xCur = xStart; xCur <= xEnd; ++xCur)
    {
        for (unsigned yCur = yStart; yCur <= yEnd; ++yCur)
        {
#ifdef _USE_ORDERED_LIST
            grid[yCur*gridCellWidthCount+xCur].Insert(entry,entry, true);
#else
            grid[yCur * gridCellWidthCount + xCur].Insert(entry);
#endif
        }
    }
}

#ifdef _USE_ORDERED_LIST
void GridSectorizer::RemoveEntry(void *entry, float minX, float minY, float maxX, float maxY)
{
    RakAssert(cellWidth>0.0f);
    RakAssert(minX <= maxX && minY <= maxY);

    int xStart, yStart, xEnd, yEnd, xCur, yCur;
    xStart=WorldToCellXOffsetAndClamped(minX);
    yStart=WorldToCellYOffsetAndClamped(minY);
    xEnd=WorldToCellXOffsetAndClamped(maxX);
    yEnd=WorldToCellYOffsetAndClamped(maxY);

    for (xCur=xStart; xCur <= xEnd; ++xCur)
    {
        for (yCur=yStart; yCur <= yEnd; ++yCur)
        {
            grid[yCur*gridCellWidthCount+xCur].RemoveIfExists(entry);
        }
    }
}
void GridSectorizer::MoveEntry(void *entry, float sourceMinX, float sourceMinY, float sourceMaxX, float sourceMaxY,
               float destMinX, float destMinY, float destMaxX, float destMaxY)
{
    RakAssert(cellWidth>0.0f);
    RakAssert(sourceMinX < sourceMaxX && sourceMinY < sourceMaxY);
    RakAssert(destMinX < destMaxX && destMinY < destMaxY);

    if (PositionCrossesCells(sourceMinX, sourceMinY, destMinX, destMinY)==false &&
        PositionCrossesCells(destMinX, destMinY, destMinX, destMinY)==false)
        return;

    int xStartSource, yStartSource, xEndSource, yEndSource;
    int xStartDest, yStartDest, xEndDest, yEndDest;
    int xCur, yCur;
    xStartSource=WorldToCellXOffsetAndClamped(sourceMinX);
    yStartSource=WorldToCellYOffsetAndClamped(sourceMinY);
    xEndSource=WorldToCellXOffsetAndClamped(sourceMaxX);
    yEndSource=WorldToCellYOffsetAndClamped(sourceMaxY);

    xStartDest=WorldToCellXOffsetAndClamped(destMinX);
    yStartDest=WorldToCellYOffsetAndClamped(destMinY);
    xEndDest=WorldToCellXOffsetAndClamped(destMaxX);
    yEndDest=WorldToCellYOffsetAndClamped(destMaxY);

    // Remove source that is not in dest
    for (xCur=xStartSource; xCur <= xEndSource; ++xCur)
    {
        for (yCur=yStartSource; yCur <= yEndSource; ++yCur)
        {
            if (xCur < xStartDest || xCur > xEndDest ||
                yCur < yStartDest || yCur > yEndDest)
            {
                grid[yCur*gridCellWidthCount+xCur].RemoveIfExists(entry);
            }
        }
    }

    // Add dest that is not in source
    for (xCur=xStartDest; xCur <= xEndDest; ++xCur)
    {
        for (yCur=yStartDest; yCur <= yEndDest; ++yCur)
        {
            if (xCur < xStartSource || xCur > xEndSource ||
                yCur < yStartSource || yCur > yEndSource)
            {
                grid[yCur*gridCellWidthCount+xCur].Insert(entry,entry, true);
            }
        }
    }
}
#endif

void GridSectorizer::GetEntries(DataStructures::List<void *> &intersectionList, float minX, float minY, float maxX, float maxY)
{
#ifdef _USE_ORDERED_LIST
    DataStructures::OrderedList<void*, void*>* cell;
#else
    DataStructures::List<void *> *cell;
#endif
    unsigned xStart = WorldToCellXOffsetAndClamped(minX);
    unsigned yStart = WorldToCellYOffsetAndClamped(minY);
    unsigned xEnd = WorldToCellXOffsetAndClamped(maxX);
    unsigned yEnd = WorldToCellYOffsetAndClamped(maxY);

    intersectionList.Clear(true);
    for (unsigned xCur = xStart; xCur <= xEnd; ++xCur)
    {
        for (unsigned yCur = yStart; yCur <= yEnd; ++yCur)
        {
            cell = grid + yCur * gridCellWidthCount + xCur;
            for (unsigned index = 0; index < cell->Size(); ++index)
                intersectionList.Insert(cell->operator[](index));
        }
    }
}

bool GridSectorizer::PositionCrossesCells(float originX, float originY, float destinationX, float destinationY) const
{
    return originX / cellWidth != destinationX / cellWidth || originY / cellHeight != destinationY / cellHeight;
}

unsigned GridSectorizer::WorldToCellX(float input) const
{
    return (unsigned) ((input - cellOriginX) * invCellWidth);
}

unsigned GridSectorizer::WorldToCellY(float input) const
{
    return (unsigned) ((input - cellOriginY) * invCellHeight);
}

unsigned GridSectorizer::WorldToCellXOffsetAndClamped(float input) const
{
    unsigned cell = WorldToCellX(input);
    cell = cell > 0 ? cell : 0; // __max(cell,0);
    cell = gridCellWidthCount - 1 < cell ? gridCellWidthCount - 1 : cell; // __min(gridCellWidthCount-1, cell);
    return cell;
}

unsigned GridSectorizer::WorldToCellYOffsetAndClamped(float input) const
{
    unsigned cell = WorldToCellY(input);
    cell = cell > 0 ? cell : 0; // __max(cell,0);
    cell = gridCellHeightCount - 1 < cell ? gridCellHeightCount - 1 : cell; // __min(gridCellHeightCount-1, cell);
    return cell;
}

void GridSectorizer::Clear(void)
{
    for (unsigned cur = 0; cur < gridCellWidthCount * gridCellHeightCount; cur++)
        grid[cur].Clear(true);
}
