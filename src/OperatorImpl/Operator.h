#pragma once
#include <memory> 
#include <arrow/record_batch.h>

// a "vector" or batch of rows instead of just 1 row
using DataChunk = std::shared_ptr<arrow::RecordBatch>;

enum class ExecutionMode
{
    SCALAR,
    AVX2,
    AVX512
};
class Operator
{
public:

    Operator(ExecutionMode mode = ExecutionMode::SCALAR)
        : CurrentMode(mode)
    {
    }

    virtual ~Operator() = default;

    virtual DataChunk Next() = 0;  // Every operator must implement Next()

protected:
    ExecutionMode CurrentMode;
    bool bFinished = false;
};