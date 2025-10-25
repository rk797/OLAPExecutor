#include "pch.h"

#include "scan_operator.h"
#include "parquet/arrow/reader.h"

ScanOperator::ScanOperator(const std::string& Filepath)
{
	// nothing for now
}

ScanOperator::~ScanOperator()
{
}

DataChunk ScanOperator::Next()
{
}