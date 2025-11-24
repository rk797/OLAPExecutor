#pragma once
#include "../Operator.h"
#include <arrow/builder.h>

class MinOperator : public Operator
{
public:
    enum class MinMode { Avx, SCALAR };

    // Child operator here is typically a ScanOperator (I/O bound) or MemoryScanOperator
    SumOperator(std::unique_ptr<Operator> Child, SumMode Mode);
    DataChunk Next() override;

private:
    std::unique_ptr<Operator> ChildOperator;
    MinMode CurrentMode;
    bool bFinished;

    long long CalculateAvx(const int32_t* Data, int64_t Length);
    long long CalculateScalarSum(const int32_t* Data, int64_t Length);

    // returns the horizontal sum of a 256-bit register containing 8 int32_t values
    int32_t HSum256(__m256i V);
};