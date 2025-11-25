#pragma once
#include "../Operator.h"
#include <arrow/builder.h>
#include <climits>

class MinOperator : public Operator
{
public:
    MinOperator(std::unique_ptr<Operator> Child, ExecutionMode Mode);
    DataChunk Next() override;

private:
    std::unique_ptr<Operator> ChildOperator;

    int32_t CalculateAvxMin(const int32_t* Data, int64_t Length);
    int32_t CalculateScalarMin(const int32_t* Data, int64_t Length);

    // Horizontal Min Helper
    int32_t HMin256(__m256i V);
};