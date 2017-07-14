/*
 *  Copyright (c) 2014, Oculus VR, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant 
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

#include "DS_Table.h"
#include "DS_OrderedList.h"
#include <string.h>
#include "RakAssert.h"
#include "RakAssert.h"
#include "Itoa.h"

using namespace DataStructures;

#ifdef _MSC_VER
#pragma warning( push )
#endif

void ExtendRows(Table::Row *input, size_t index)
{
    (void) index;
    input->cells.Insert(new Table::Cell);
}

void FreeRow(Table::Row *input, size_t index)
{
    (void) index;

    for (size_t i = 0; i < input->cells.Size(); i++)
        delete input->cells[i];
    delete input;
}

Table::Cell::Cell()
{
    isEmpty = true;
    c = 0;
    ptr = 0;
    i = 0;
}

Table::Cell::~Cell()
{
    Clear();
}

Table::Cell &Table::Cell::operator=(const Table::Cell &input)
{
    isEmpty = input.isEmpty;
    i = input.i;
    ptr = input.ptr;
    if (c)
        free(c);
    if (input.c)
    {
        c = (char *) malloc((int) i);
        memcpy(c, input.c, (int) i);
    }
    else
        c = 0;
    return *this;
}

Table::Cell::Cell(const Table::Cell &input)
{
    isEmpty = input.isEmpty;
    i = input.i;
    ptr = input.ptr;
    if (input.c)
    {
        if (c)
            free(c);
        c = (char *) malloc((int) i);
        memcpy(c, input.c, (int) i);
    }
}

void Table::Cell::Set(double input)
{
    Clear();
    i = (size_t) input;
    c = 0;
    ptr = 0;
    isEmpty = false;
}

void Table::Cell::Set(unsigned int input)
{
    Set((int) input);
}

void Table::Cell::Set(int input)
{
    Clear();
    i = (size_t) input;
    c = 0;
    ptr = 0;
    isEmpty = false;
}

void Table::Cell::Set(const char *input)
{
    Clear();

    if (input)
    {
        i = (int) strlen(input) + 1;
        c = (char *) malloc((int) i);
        strcpy(c, input);
    }
    else
    {
        c = 0;
        i = 0;
    }
    ptr = 0;
    isEmpty = false;
}

void Table::Cell::Set(const char *input, size_t inputLength)
{
    Clear();
    if (input)
    {
        c = (char *) malloc(inputLength);
        i = inputLength;
        memcpy(c, input, inputLength);
    }
    else
    {
        c = 0;
        i = 0;
    }
    ptr = 0;
    isEmpty = false;
}

void Table::Cell::SetPtr(void *p)
{
    Clear();
    c = 0;
    ptr = p;
    isEmpty = false;
}

void Table::Cell::Get(int *output)
{
    RakAssert(!isEmpty);
    int o = (int) i;
    *output = o;
}

void Table::Cell::Get(double *output)
{
    RakAssert(!isEmpty);
    *output = i;
}

void Table::Cell::Get(char *output)
{
    RakAssert(!isEmpty);
    strcpy(output, c);
}

void Table::Cell::Get(char *output, size_t *outputLength)
{
    RakAssert(!isEmpty);
    memcpy(output, c, (size_t) i);
    if (outputLength)
        *outputLength = (size_t) i;
}

RakNet::RakString Table::Cell::ToString(ColumnType columnType)
{
    if (isEmpty)
        return RakNet::RakString();

    if (columnType == NUMERIC)
        return RakNet::RakString("%f", i);
    else if (columnType == STRING)
        return RakNet::RakString(c);
    else if (columnType == BINARY)
        return RakNet::RakString("<Binary>");
    else if (columnType == POINTER)
        return RakNet::RakString("%p", ptr);

    return RakNet::RakString();
}

Table::Cell::Cell(double numericValue, char *charValue, void *ptr, ColumnType type)
{
    SetByType(numericValue, charValue, ptr, type);
}

void Table::Cell::SetByType(double numericValue, char *charValue, void *ptr, ColumnType type)
{
    isEmpty = true;
    if (type == NUMERIC)
        Set(numericValue);
    else if (type == STRING)
        Set(charValue);
    else if (type == BINARY)
        Set(charValue, (int) numericValue);
    else if (type == POINTER)
        SetPtr(ptr);
    else
        ptr = (void *) charValue;
}

Table::ColumnType Table::Cell::EstimateColumnType(void) const
{
    if (c)
    {
        if (i != 0.0f)
            return BINARY;
        else
            return STRING;
    }

    if (ptr)
        return POINTER;
    return NUMERIC;
}

void Table::Cell::Clear(void)
{
    if (!isEmpty && c)
    {
        free(c);
        c = 0;
    }
    isEmpty = true;
}

Table::ColumnDescriptor::ColumnDescriptor()
{

}

Table::ColumnDescriptor::~ColumnDescriptor()
{

}

Table::ColumnDescriptor::ColumnDescriptor(const char cn[_TABLE_MAX_COLUMN_NAME_LENGTH], ColumnType ct)
{
    columnType = ct;
    strcpy(columnName, cn);
}

void Table::Row::UpdateCell(size_t columnIndex, double value)
{
    cells[columnIndex]->Clear();
    cells[columnIndex]->Set(value);
}

void Table::Row::UpdateCell(size_t columnIndex, const char *str)
{
    cells[columnIndex]->Clear();
    cells[columnIndex]->Set(str);
}

void Table::Row::UpdateCell(size_t columnIndex, size_t byteLength, const char *data)
{
    cells[columnIndex]->Clear();
    cells[columnIndex]->Set(data, byteLength);
}

Table::Table()
{
}

Table::~Table()
{
    Clear();
}

size_t Table::AddColumn(const char columnName[_TABLE_MAX_COLUMN_NAME_LENGTH], ColumnType columnType)
{
    if (columnName[0] == 0)
        return (size_t) -1;

    // Add this column.
    columns.Insert(Table::ColumnDescriptor(columnName, columnType));

    // Extend the rows by one
    rows.ForEachData(ExtendRows);

    return columns.Size() - 1;
}

void Table::RemoveColumn(size_t columnIndex)
{
    if (columnIndex >= columns.Size())
        return;

    columns.RemoveAtIndex(columnIndex);

    // Remove this index from each row.
    auto *cur = rows.GetListHead();
    while (cur)
    {
        for (size_t i = 0; i < cur->size; i++)
        {
            delete cur->data[i]->cells[columnIndex];
            cur->data[i]->cells.RemoveAtIndex(columnIndex);
        }

        cur = cur->next;
    }
}

size_t Table::ColumnIndex(const char *columnName) const
{
    for (size_t columnIndex = 0; columnIndex < columns.Size(); columnIndex++)
        if (strcmp(columnName, columns[columnIndex].columnName) == 0)
            return columnIndex;
    return (size_t) -1;
}

size_t Table::ColumnIndex(char columnName[_TABLE_MAX_COLUMN_NAME_LENGTH]) const
{
    return ColumnIndex((const char *) columnName);
}

char *Table::ColumnName(size_t index) const
{
    if (index >= columns.Size())
        return 0;
    else
        return (char *) columns[index].columnName;
}

Table::ColumnType Table::GetColumnType(size_t index) const
{
    if (index >= columns.Size())
        return (Table::ColumnType) 0;
    else
        return columns[index].columnType;
}

size_t Table::GetColumnCount(void) const
{
    return columns.Size();
}

size_t Table::GetRowCount(void) const
{
    return rows.Size();
}

Table::Row *Table::AddRow(size_t rowId)
{
    Row *newRow = new Row;
    if (!rows.Insert(rowId, newRow))
    {
        delete newRow;
        return 0; // Already exists
    }
    for (size_t rowIndex = 0; rowIndex < columns.Size(); rowIndex++)
        newRow->cells.Insert(new Table::Cell);
    return newRow;
}

Table::Row *Table::AddRow(size_t rowId, DataStructures::List<Cell> &initialCellValues)
{
    Row *newRow = new Row;
    for (size_t rowIndex = 0; rowIndex < columns.Size(); rowIndex++)
    {
        if (rowIndex < initialCellValues.Size() && !initialCellValues[rowIndex].isEmpty)
        {
            Table::Cell *c;
            c = new Table::Cell;
            c->SetByType(initialCellValues[rowIndex].i, initialCellValues[rowIndex].c, initialCellValues[rowIndex].ptr,
                         columns[rowIndex].columnType);
            newRow->cells.Insert(c);
        }
        else
            newRow->cells.Insert(new Table::Cell);
    }
    rows.Insert(rowId, newRow);
    return newRow;
}

Table::Row *Table::AddRow(size_t rowId, DataStructures::List<Cell *> &initialCellValues, bool copyCells)
{
    Row *newRow = new Row;
    for (size_t rowIndex = 0; rowIndex < columns.Size(); rowIndex++)
    {
        if (rowIndex < initialCellValues.Size() && initialCellValues[rowIndex] &&
            !initialCellValues[rowIndex]->isEmpty)
        {
            if (!copyCells)
                newRow->cells.Insert(new Table::Cell(
                        initialCellValues[rowIndex]->i, initialCellValues[rowIndex]->c,
                        initialCellValues[rowIndex]->ptr, columns[rowIndex].columnType));
            else
            {
                Table::Cell *c = new Table::Cell;
                newRow->cells.Insert(c);
                *c = *(initialCellValues[rowIndex]);
            }
        }
        else
            newRow->cells.Insert(new Table::Cell);
    }
    rows.Insert(rowId, newRow);
    return newRow;
}

Table::Row *Table::AddRowColumns(size_t rowId, Row *row, DataStructures::List<size_t> columnIndices)
{
    Row *newRow = new Row;
    for (size_t columnIndex = 0; columnIndex < columnIndices.Size(); columnIndex++)
    {
        if (!row->cells[columnIndices[columnIndex]]->isEmpty)
        {
            newRow->cells.Insert(new Table::Cell(
                    row->cells[columnIndices[columnIndex]]->i, row->cells[columnIndices[columnIndex]]->c,
                    row->cells[columnIndices[columnIndex]]->ptr, columns[columnIndex].columnType));
        }
        else
            newRow->cells.Insert(new Table::Cell);
    }
    rows.Insert(rowId, newRow);
    return newRow;
}

bool Table::RemoveRow(size_t rowId)
{
    Row *out;
    if (rows.Delete(rowId, out))
    {
        DeleteRow(out);
        return true;
    }
    return false;
}

void Table::RemoveRows(Table *tableContainingRowIDs)
{
    auto *cur = tableContainingRowIDs->GetRows().GetListHead();
    while (cur)
    {
        for (size_t i = 0; i < cur->size; i++)
        {
            rows.Delete(cur->keys[i]);
        }
        cur = cur->next;
    }
    return;
}

bool Table::UpdateCell(size_t rowId, size_t columnIndex, int value)
{
    RakAssert(columns[columnIndex].columnType == NUMERIC);

    Row *row = GetRowByID(rowId);
    if (row)
    {
        row->UpdateCell(columnIndex, value);
        return true;
    }
    return false;
}

bool Table::UpdateCell(size_t rowId, size_t columnIndex, char *str)
{
    RakAssert(columns[columnIndex].columnType == STRING);

    Row *row = GetRowByID(rowId);
    if (row)
    {
        row->UpdateCell(columnIndex, str);
        return true;
    }
    return false;
}

bool Table::UpdateCell(size_t rowId, size_t columnIndex, size_t byteLength, char *data)
{
    RakAssert(columns[columnIndex].columnType == BINARY);

    Row *row = GetRowByID(rowId);
    if (row)
    {
        row->UpdateCell(columnIndex, byteLength, data);
        return true;
    }
    return false;
}

bool Table::UpdateCellByIndex(size_t rowIndex, size_t columnIndex, int value)
{
    RakAssert(columns[columnIndex].columnType == NUMERIC);

    Row *row = GetRowByIndex(rowIndex, 0);
    if (row)
    {
        row->UpdateCell(columnIndex, value);
        return true;
    }
    return false;
}

bool Table::UpdateCellByIndex(size_t rowIndex, size_t columnIndex, char *str)
{
    RakAssert(columns[columnIndex].columnType == STRING);

    Row *row = GetRowByIndex(rowIndex, 0);
    if (row)
    {
        row->UpdateCell(columnIndex, str);
        return true;
    }
    return false;
}

bool Table::UpdateCellByIndex(size_t rowIndex, size_t columnIndex, size_t byteLength, char *data)
{
    RakAssert(columns[columnIndex].columnType == BINARY);

    Row *row = GetRowByIndex(rowIndex, 0);
    if (row)
    {
        row->UpdateCell(columnIndex, byteLength, data);
        return true;
    }
    return false;
}

void Table::GetCellValueByIndex(size_t rowIndex, size_t columnIndex, int *output)
{
    RakAssert(columns[columnIndex].columnType == NUMERIC);

    Row *row = GetRowByIndex(rowIndex, 0);
    if (row)
        row->cells[columnIndex]->Get(output);
}

void Table::GetCellValueByIndex(size_t rowIndex, size_t columnIndex, char *output)
{
    RakAssert(columns[columnIndex].columnType == STRING);

    Row *row = GetRowByIndex(rowIndex, 0);
    if (row)
        row->cells[columnIndex]->Get(output);
}

void Table::GetCellValueByIndex(size_t rowIndex, size_t columnIndex, char *output, size_t *outputLength)
{
    RakAssert(columns[columnIndex].columnType == BINARY);

    Row *row = GetRowByIndex(rowIndex, 0);
    if (row)
        row->cells[columnIndex]->Get(output, outputLength);
}

Table::FilterQuery::FilterQuery()
{
    columnName[0] = 0;
}

Table::FilterQuery::~FilterQuery()
{

}

Table::FilterQuery::FilterQuery(size_t column, Cell *cell, FilterQueryType op)
{
    columnIndex = column;
    cellValue = cell;
    operation = op;
}

Table::Row *Table::GetRowByID(size_t rowId) const
{
    Row *row;
    if (rows.Get(rowId, row))
        return row;
    return 0;
}

Table::Row *Table::GetRowByIndex(size_t rowIndex, size_t *key) const
{
    auto *cur = rows.GetListHead();
    while (cur)
    {
        if (rowIndex < cur->size)
        {
            if (key)
                *key = cur->keys[rowIndex];
            return cur->data[rowIndex];
        }
        if (rowIndex <= cur->size)
            rowIndex -= cur->size;
        else
            return 0;
        cur = cur->next;
    }
    return 0;
}

void Table::QueryTable(size_t *columnIndicesSubset, size_t numColumnSubset, FilterQuery *inclusionFilters,
                       size_t numInclusionFilters, size_t *rowIds, size_t numRowIDs, Table *result)
{
    DataStructures::List<size_t> columnIndicesToReturn;

    // Clear the result table.
    result->Clear();

    if (columnIndicesSubset && numColumnSubset > 0)
    {
        for (size_t i = 0; i < numColumnSubset; i++)
        {
            if (columnIndicesSubset[i] < columns.Size())
                columnIndicesToReturn.Insert(columnIndicesSubset[i]);
        }
    }
    else
    {
        for (size_t i = 0; i < columns.Size(); i++)
            columnIndicesToReturn.Insert(i);
    }

    if (columnIndicesToReturn.Size() == 0)
        return; // No valid columns specified

    for (size_t i = 0; i < columnIndicesToReturn.Size(); i++)
        result->AddColumn(columns[columnIndicesToReturn[i]].columnName, columns[columnIndicesToReturn[i]].columnType);

    // Get the column indices of the filter queries.
    DataStructures::List<size_t> inclusionFilterColumnIndices;
    if (inclusionFilters && numInclusionFilters > 0)
    {
        for (size_t i = 0; i < numInclusionFilters; i++)
        {
            if (inclusionFilters[i].columnName[0])
                inclusionFilters[i].columnIndex = ColumnIndex(inclusionFilters[i].columnName);
            if (inclusionFilters[i].columnIndex < columns.Size())
                inclusionFilterColumnIndices.Insert(inclusionFilters[i].columnIndex);
            else
                inclusionFilterColumnIndices.Insert((size_t) -1);
        }
    }

    if (rowIds == 0 || numRowIDs == 0)
    {
        // All rows
        auto *cur = rows.GetListHead();
        while (cur)
        {
            for (size_t i = 0; i < cur->size; i++)
                QueryRow(inclusionFilterColumnIndices, columnIndicesToReturn, cur->keys[i], cur->data[i], inclusionFilters, result);
            cur = cur->next;
        }
    }
    else
    {
        // Specific rows
        Row *row;
        for (size_t i = 0; i < numRowIDs; i++)
        {
            if (rows.Get(rowIds[i], row))
                QueryRow(inclusionFilterColumnIndices, columnIndicesToReturn, rowIds[i], row, inclusionFilters, result);
        }
    }
}

void Table::QueryRow(DataStructures::List<size_t> &inclusionFilterColumnIndices,
                     DataStructures::List<size_t> &columnIndicesToReturn, size_t key, Table::Row *row,
                     FilterQuery *inclusionFilters, Table *result)
{
    bool pass = false;

    // If no inclusion filters, just add the row
    if (inclusionFilterColumnIndices.Size() == 0)
        result->AddRowColumns(key, row, columnIndicesToReturn);
    else
    {
        // Go through all inclusion filters.  Only add this row if all filters pass.
        for (size_t j = 0; j < inclusionFilterColumnIndices.Size(); j++)
        {
            size_t columnIndex = inclusionFilterColumnIndices[j];
            if (columnIndex != (size_t) -1 && !row->cells[columnIndex]->isEmpty)
            {
                if (columns[inclusionFilterColumnIndices[j]].columnType == STRING &&
                    (row->cells[columnIndex]->c == 0 ||
                     inclusionFilters[j].cellValue->c == 0))
                    continue;

                switch (inclusionFilters[j].operation)
                {
                    case QF_EQUAL:
                        switch (columns[inclusionFilterColumnIndices[j]].columnType)
                        {
                            case NUMERIC:
                                pass = row->cells[columnIndex]->i == inclusionFilters[j].cellValue->i;
                                break;
                            case STRING:
                                pass = strcmp(row->cells[columnIndex]->c, inclusionFilters[j].cellValue->c) == 0;
                                break;
                            case BINARY:
                                pass = row->cells[columnIndex]->i == inclusionFilters[j].cellValue->i &&
                                       memcmp(row->cells[columnIndex]->c, inclusionFilters[j].cellValue->c,
                                              (size_t) row->cells[columnIndex]->i) == 0;
                                break;
                            case POINTER:
                                pass = row->cells[columnIndex]->ptr == inclusionFilters[j].cellValue->ptr;
                                break;
                        }
                        break;
                    case QF_NOT_EQUAL:
                        switch (columns[inclusionFilterColumnIndices[j]].columnType)
                        {
                            case NUMERIC:
                                pass = row->cells[columnIndex]->i != inclusionFilters[j].cellValue->i;
                                break;
                            case STRING:
                                pass = strcmp(row->cells[columnIndex]->c, inclusionFilters[j].cellValue->c) != 0;
                                break;
                            case BINARY:
                                pass = row->cells[columnIndex]->i == inclusionFilters[j].cellValue->i &&
                                       memcmp(row->cells[columnIndex]->c, inclusionFilters[j].cellValue->c,
                                              (size_t) row->cells[columnIndex]->i) == 0;
                                break;
                            case POINTER:
                                pass = row->cells[columnIndex]->ptr != inclusionFilters[j].cellValue->ptr;
                                break;
                        }
                        break;
                    case QF_GREATER_THAN:
                        switch (columns[inclusionFilterColumnIndices[j]].columnType)
                        {
                            case NUMERIC:
                                pass = row->cells[columnIndex]->i > inclusionFilters[j].cellValue->i;
                                break;
                            case STRING:
                                pass = strcmp(row->cells[columnIndex]->c, inclusionFilters[j].cellValue->c) > 0;
                                break;
                            case BINARY:
                                break;
                            case POINTER:
                                pass = row->cells[columnIndex]->ptr > inclusionFilters[j].cellValue->ptr;
                                break;
                        }
                        break;
                    case QF_GREATER_THAN_EQ:
                        switch (columns[inclusionFilterColumnIndices[j]].columnType)
                        {
                            case NUMERIC:
                                pass = row->cells[columnIndex]->i >= inclusionFilters[j].cellValue->i;
                                break;
                            case STRING:
                                pass = strcmp(row->cells[columnIndex]->c, inclusionFilters[j].cellValue->c) >= 0;
                                break;
                            case BINARY:
                                break;
                            case POINTER:
                                pass = row->cells[columnIndex]->ptr >= inclusionFilters[j].cellValue->ptr;
                                break;
                        }
                        break;
                    case QF_LESS_THAN:
                        switch (columns[inclusionFilterColumnIndices[j]].columnType)
                        {
                            case NUMERIC:
                                pass = row->cells[columnIndex]->i < inclusionFilters[j].cellValue->i;
                                break;
                            case STRING:
                                pass = strcmp(row->cells[columnIndex]->c, inclusionFilters[j].cellValue->c) < 0;
                                break;
                            case BINARY:
                                break;
                            case POINTER:
                                pass = row->cells[columnIndex]->ptr < inclusionFilters[j].cellValue->ptr;
                                break;
                        }
                        break;
                    case QF_LESS_THAN_EQ:
                        switch (columns[inclusionFilterColumnIndices[j]].columnType)
                        {
                            case NUMERIC:
                                pass = row->cells[columnIndex]->i <= inclusionFilters[j].cellValue->i;
                                break;
                            case STRING:
                                pass = strcmp(row->cells[columnIndex]->c, inclusionFilters[j].cellValue->c) <= 0;
                                break;
                            case BINARY:
                                break;
                            case POINTER:
                                pass = row->cells[columnIndex]->ptr <= inclusionFilters[j].cellValue->ptr;
                                break;
                        }
                        break;
                    case QF_IS_EMPTY:
                        pass = false;
                        break;
                    case QF_NOT_EMPTY:
                        pass = true;
                        break;
                    default:
                        pass = false;
                        RakAssert(0);
                        break;
                }
            }
            else
                pass = inclusionFilters[j].operation == QF_IS_EMPTY;

            if (!pass)
                break;
        }

        if (pass)
            result->AddRowColumns(key, row, columnIndicesToReturn);
    }
}

static Table::SortQuery *_sortQueries;
static size_t _numSortQueries;
static DataStructures::List<size_t> *_columnIndices;
static DataStructures::List<Table::ColumnDescriptor> *_columns;

// first is the one inserting, second is the one already there.
int RowSort(Table::Row *const &first, Table::Row *const &second)
{
    for (size_t i = 0; i < _numSortQueries; i++)
    {
        size_t columnIndex = (*_columnIndices)[i];
        if (columnIndex == (size_t) -1)
            continue;

        if (first->cells[columnIndex]->isEmpty && !second->cells[columnIndex]->isEmpty)
            return 1; // Empty cells always go at the end

        if (!first->cells[columnIndex]->isEmpty && second->cells[columnIndex]->isEmpty)
            return -1; // Empty cells always go at the end

        if (_sortQueries[i].operation == Table::QS_INCREASING_ORDER)
        {
            if ((*_columns)[columnIndex].columnType == Table::NUMERIC)
            {
                if (first->cells[columnIndex]->i > second->cells[columnIndex]->i)
                    return 1;
                if (first->cells[columnIndex]->i < second->cells[columnIndex]->i)
                    return -1;
            }
            else
            {
                // String
                if (strcmp(first->cells[columnIndex]->c, second->cells[columnIndex]->c) > 0)
                    return 1;
                if (strcmp(first->cells[columnIndex]->c, second->cells[columnIndex]->c) < 0)
                    return -1;
            }
        }
        else
        {
            if ((*_columns)[columnIndex].columnType == Table::NUMERIC)
            {
                if (first->cells[columnIndex]->i < second->cells[columnIndex]->i)
                    return 1;
                if (first->cells[columnIndex]->i > second->cells[columnIndex]->i)
                    return -1;
            }
            else
            {
                // String
                if (strcmp(first->cells[columnIndex]->c, second->cells[columnIndex]->c) < 0)
                    return 1;
                if (strcmp(first->cells[columnIndex]->c, second->cells[columnIndex]->c) > 0)
                    return -1;
            }
        }
    }

    return 0;
}

void Table::SortTable(Table::SortQuery *sortQueries, size_t numSortQueries, Table::Row **out)
{
    size_t outLength;
    DataStructures::List<size_t> columnIndices;
    _sortQueries = sortQueries;
    _numSortQueries = numSortQueries;
    _columnIndices = &columnIndices;
    _columns = &columns;
    bool anyValid = false;

    for (size_t i = 0; i < numSortQueries; i++)
    {
        if (sortQueries[i].columnIndex < columns.Size() && columns[sortQueries[i].columnIndex].columnType != BINARY)
        {
            columnIndices.Insert(sortQueries[i].columnIndex);
            anyValid = true;
        }
        else
            columnIndices.Insert((size_t) -1); // Means don't check this column
    }

    
    auto *cur = rows.GetListHead();
    if (!anyValid)
    {
        outLength = 0;
        while (cur)
        {
            for (size_t i = 0; i < cur->size; i++)
                out[(outLength)++] = cur->data[i];
            cur = cur->next;
        }
        return;
    }

    // Start adding to ordered list.
    DataStructures::OrderedList<Row *, Row *, RowSort> orderedList;
    while (cur)
    {
        for (size_t i = 0; i < cur->size; i++)
        {
            RakAssert(cur->data[i]);
            orderedList.Insert(cur->data[i], cur->data[i], true);
        }
        cur = cur->next;
    }

    outLength = 0;
    for (size_t i = 0; i < orderedList.Size(); i++)
        out[(outLength)++] = orderedList[i];
}

void Table::PrintColumnHeaders(char *out, size_t outLength, char columnDelineator) const
{
    if (outLength <= 0)
        return;
    if (outLength == 1)
    {
        *out = 0;
        return;
    }

    out[0] = 0;
    size_t len;
    for (size_t i = 0; i < columns.Size(); i++)
    {
        if (i != 0)
        {
            len = strlen(out);
            if (len < outLength - 1)
                sprintf(out + len, "%c", columnDelineator);
            else
                return;
        }

        len = strlen(out);
        if (len < outLength - strlen(columns[i].columnName))
            sprintf(out + len, "%s", columns[i].columnName);
        else
            return;
    }
}

void Table::PrintRow(char *out, size_t outLength, char columnDelineator, bool printDelineatorForBinary, Table::Row *inputRow) const
{
    if (outLength <= 0)
        return;
    if (outLength == 1)
    {
        *out = 0;
        return;
    }

    if (inputRow->cells.Size() != columns.Size())
    {
        strncpy(out, "Cell width does not match column width.\n", outLength);
        out[outLength - 1] = 0;
        return;
    }

    char buff[512];
    size_t len;
    out[0] = 0;
    for (size_t i = 0; i < columns.Size(); i++)
    {
        if (columns[i].columnType == NUMERIC)
        {
            if (!inputRow->cells[i]->isEmpty)
            {
                sprintf(buff, "%ld", inputRow->cells[i]->i);
                len = strlen(buff);
            }
            else
                len = 0;
            if (i + 1 != columns.Size())
                buff[len++] = columnDelineator;
            buff[len] = 0;
        }
        else if (columns[i].columnType == STRING)
        {
            if (!inputRow->cells[i]->isEmpty && inputRow->cells[i]->c)
            {
                strncpy(buff, inputRow->cells[i]->c, 512 - 2);
                buff[512 - 2] = 0;
                len = (int) strlen(buff);
            }
            else
                len = 0;
            if (i + 1 != columns.Size())
                buff[len++] = columnDelineator;
            buff[len] = 0;
        }
        else if (columns[i].columnType == POINTER)
        {
            if (!inputRow->cells[i]->isEmpty && inputRow->cells[i]->ptr)
            {
                sprintf(buff, "%p", inputRow->cells[i]->ptr);
                len = (int) strlen(buff);
            }
            else
                len = 0;
            if (i + 1 != columns.Size())
                buff[len++] = columnDelineator;
            buff[len] = 0;
        }
        else
        {
            if (printDelineatorForBinary)
            {
                if (i + 1 != columns.Size())
                    buff[0] = columnDelineator;
                buff[1] = 0;
            }
            else
                buff[0] = 0;

        }

        len = (int) strlen(out);
        if (outLength == len + 1)
            break;
        strncpy(out + len, buff, outLength - len);
        out[outLength - 1] = 0;
    }
}

void Table::Clear(void)
{
    rows.ForEachData(FreeRow);
    rows.Clear();
    columns.Clear(true);
}

const List<Table::ColumnDescriptor> &Table::GetColumns(void) const
{
    return columns;
}

const Table::RowType &Table::GetRows(void) const
{
    return rows;
}

DataStructures::Page<size_t, DataStructures::Table::Row *, _TABLE_BPLUS_TREE_ORDER> *Table::GetListHead(void)
{
    return rows.GetListHead();
}

size_t Table::GetAvailableRowId(void) const
{
    bool setKey = false;
    size_t key = 0;
    PageType *cur = rows.GetListHead();

    while (cur)
    {
        for (size_t i = 0; i < cur->size; i++)
        {
            if (!setKey)
            {
                key = cur->keys[i] + 1;
                setKey = true;
            }
            else
            {
                if (key != cur->keys[i])
                    return key;
                key++;
            }
        }

        cur = cur->next;
    }
    return key;
}

void Table::DeleteRow(Table::Row *row)
{
    for (size_t rowIndex = 0; rowIndex < row->cells.Size(); rowIndex++)
        delete row->cells[rowIndex];
    delete row;
}

Table &Table::operator=(const Table &input)
{
    Clear();

    for (size_t i = 0; i < input.GetColumnCount(); i++)
        AddColumn(input.ColumnName(i), input.GetColumnType(i));

    auto *cur = input.GetRows().GetListHead();
    while (cur)
    {
        for (size_t i = 0; i < cur->size; i++)
            AddRow(cur->keys[i], cur->data[i]->cells, false);
        cur = cur->next;
    }

    return *this;
}

#ifdef _MSC_VER
#pragma warning( pop )
#endif
