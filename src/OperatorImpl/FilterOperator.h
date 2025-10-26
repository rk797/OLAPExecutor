#pragma once

#include "Operator.h"

class FilterOperator : public Operator
{
public:
    enum class FilterMode
    {
        Avx,
        Scalar
    };

    FilterOperator(std::unique_ptr<Operator> Child, int FilterValue, FilterMode Mode);

    DataChunk Next() override;

private:
    DataChunk ApplyAvxFilter(const DataChunk& InputChunk);
    DataChunk ApplyScalarFilter(const DataChunk& InputChunk);
    FilterMode CurrentMode;
    std::unique_ptr<Operator> ChildOperator;
    int ValueToCompare;
};

