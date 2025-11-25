#pragma once
#include "../Operator.h"
#include <arrow/builder.h>

class SumOperator : public Operator
{
public:

	// Child operator here is typically a ScanOperator (I/O bound) or MemoryScanOperator
    SumOperator(std::unique_ptr<Operator> child, ExecutionMode mode)
        : Operator(mode)
    {
        this->ChildOperator = std::move(child);
        this->bFinished = false;
    }


    DataChunk Next() override;

private:
    std::unique_ptr<Operator> ChildOperator;

    long long CalculateAvx2Sum(const int32_t* Data, int64_t Length);
    long long CalculateScalarSum(const int32_t* Data, int64_t Length);

	// returns the horizontal sum of a 256-bit register containing 8 int32_t values
    int32_t HSum256(__m256i V);
};