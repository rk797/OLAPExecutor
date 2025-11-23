#pragma once
#include "Operator.h"
#include <vector>

class MemoryScanOperator : public Operator
{
public:
    MemoryScanOperator(const std::vector<DataChunk>& Chunks);

    DataChunk Next() override;

private:
    const std::vector<DataChunk>& SourceChunks;
    size_t CurrentIndex;
};