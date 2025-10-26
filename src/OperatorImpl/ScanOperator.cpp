#include "pch.h"

#include "ScanOperator.h"
#include "parquet/arrow/reader.h"

// This function is used by the engine to physically interface with the file on the disk
// It reads the file in chunks and returns DataChunks to the engine
ScanOperator::ScanOperator(const std::string& Filepath)
{
    arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> InFileResult = arrow::io::ReadableFile::Open(Filepath);

    PARQUET_THROW_NOT_OK(InFileResult.status());

    std::shared_ptr<arrow::io::ReadableFile> InFile = InFileResult.ValueOrDie();

    arrow::Result<std::unique_ptr<parquet::arrow::FileReader>> ReaderResult = parquet::arrow::OpenFile(InFile, arrow::default_memory_pool());
    
    PARQUET_THROW_NOT_OK(ReaderResult.status());
    
    this->ArrowReader = std::move(ReaderResult.ValueOrDie());

    PARQUET_THROW_NOT_OK(
        this->ArrowReader->GetRecordBatchReader(&this->BatchReader)
    );
}

ScanOperator::~ScanOperator()
{

}

DataChunk ScanOperator::Next()
{
    std::shared_ptr<arrow::RecordBatch> Batch;
    PARQUET_THROW_NOT_OK(this->BatchReader->ReadNext(&Batch));
    return Batch;
}