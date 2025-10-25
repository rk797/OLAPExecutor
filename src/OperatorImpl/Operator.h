#pragma once
#include <memory> 
#include <arrow/record_batch.h>

// a "vector" or batch of rows instead of just 1 row
using DataChunk = std::shared_ptr<arrow::RecordBatch>;

class Operator
{
public:
    virtual ~Operator() = default;

    
    virtual DataChunk Next() = 0;  // Every operator must implement Next()
};