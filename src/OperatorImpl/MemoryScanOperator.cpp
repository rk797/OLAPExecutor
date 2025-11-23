#include "pch.h"
#include "MemoryScanOperator.h"

MemoryScanOperator::MemoryScanOperator(const std::vector<DataChunk>& Chunks)
    : SourceChunks(Chunks), CurrentIndex(0)
{
}

DataChunk MemoryScanOperator::Next()
{
    if (this->CurrentIndex >= this->SourceChunks.size())
    {
        return nullptr;
    }

    // return chunk and advance index
    return this->SourceChunks[this->CurrentIndex++];
}