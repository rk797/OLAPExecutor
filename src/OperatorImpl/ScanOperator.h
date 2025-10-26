#pragma once
#include "Operator.h"
#include <string>

namespace parquet { namespace arrow { class FileReader; } }
namespace arrow { class RecordBatchReader; }

class ScanOperator : public Operator
{
public:
    ScanOperator(const std::string& Filepath); // parquet file path
    ~ScanOperator(); 

	DataChunk Next() override;

private:
    std::unique_ptr<parquet::arrow::FileReader> ArrowReader;
    std::shared_ptr<arrow::RecordBatchReader> BatchReader;
};