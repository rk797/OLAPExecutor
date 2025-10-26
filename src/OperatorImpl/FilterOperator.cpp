#include "pch.h"
#include "FilterOperator.h"
#include "arrow/builder.h"

FilterOperator::FilterOperator(std::unique_ptr<Operator> Child, int FilterValue, FilterMode Mode)
{
    this->ChildOperator = std::move(Child);
    this->ValueToCompare = FilterValue;
    this->CurrentMode = Mode;
}


DataChunk FilterOperator::Next()
{
    DataChunk InputChunk = this->ChildOperator->Next();
    if (InputChunk == nullptr)
    {
        return nullptr;
    }

    if (this->CurrentMode == FilterMode::Avx)
    {
        return this->ApplyAvxFilter(InputChunk);
    }
    else
    {
        return this->ApplyScalarFilter(InputChunk);
    }
}

// Quick breakdown of instrinics
// _mm256_cmpgt_epi32
// _mm256 -> Operates on 256-bit wide vectors (8 x 32-bit integers)
// _cmpgt -> Operation: compare greater than
// _epi32 -> Data type: e: "extended", p: operates on all elements in the vector, i32: 32-bit signed integers

// Processes 8 integers at a time to find values that are greater than ValueToCompare
// TODO: Fix the slow SIMD gather
DataChunk FilterOperator::ApplyAvxFilter(const DataChunk& InputChunk)
{
    // get the input column (just the first column for now since we are testing)
    std::shared_ptr<arrow::Int32Array> Column = std::static_pointer_cast<arrow::Int32Array>(InputChunk->column(0));
    const int32_t* InputData = Column->raw_values();
    int64_t InputLength = Column->length();
    arrow::Int32Builder Builder;

    
    // We will write to this buffer inside the loops, then do one
    // bulk append to the Arrow builder at the end. This avoids
    // the high overhead of calling Builder.Append() repeatedly.
    std::unique_ptr<int32_t[]> OutputBuffer(new int32_t[InputLength]);
    int64_t OutputCount = 0;

    // Fill the CompareVector with the ValueToCompare 8 times
    // [v0 v0 v0 v0 v0 v0 v0 v0] where v0 = ValueToCompare
    __m256i CompareVector = _mm256_set1_epi32(this->ValueToCompare);
    alignas(32) int32_t TmpBuffer[8];
    // process the data in chunks of 8 integers
    int i = 0;
    for (; i <= InputLength - 8; i += 8)
    {
        __m256i DataVector = _mm256_loadu_si256((__m256i*)(InputData + i)); // unaligned load (which is safer)
        // compare each element in DataVector with CompareVector
        // generates a mask where each element is all 1s (0xFFFFFFFF) if the condition is true, or all 0s if false
        // example: [0xFFFFFFFF 0 0xFFFFFFFF 0 0 0xFFFFFFFF 0 0] for a comparison result of [true, false, true, false, false, true, false, false]
        __m256i MaskVector = _mm256_cmpgt_epi32(DataVector, CompareVector);
        // take the top bit from each mask and create an 8-bit integer mask
        int Mask = _mm256_movemask_ps(_mm256_castsi256_ps(MaskVector));

        // OPTIMIZATION: Add fast-paths for all-fail or all-pass
        if (Mask == 0)
        {
            // all 8 elements failed the check. Skip.
            continue;
        }
        else if (Mask == 0xFF)
        {
            // all 8 ints passed the check, do a fast bulk copy
            std::memcpy(OutputBuffer.get() + OutputCount, InputData + i, 8 * sizeof(int32_t));
            OutputCount += 8;
        }
        else
        {
            // Some elements passed. This is the original "slow gather" path,
            // atleast 1 value passed the check, store the data to TmpBuffer
            _mm256_storeu_si256((__m256i*)TmpBuffer, DataVector);
            for (int j = 0; j < 8; ++j)
            {
                // find which value passed the check using the Mask
                if ((Mask >> j) & 1)
                {
                    OutputBuffer[OutputCount++] = TmpBuffer[j];
                }
            }
        }
    }
    // scalar processing for remaining elements that didn't fit into a vector of 8 ints
    for (; i < InputLength; ++i)
    {
        if (InputData[i] > this->ValueToCompare)
        {
            
            OutputBuffer[OutputCount++] = InputData[i];
        }
    }

    // fast bulk append to the builder
    PARQUET_THROW_NOT_OK(Builder.AppendValues(OutputBuffer.get(), OutputCount));

    std::shared_ptr<arrow::Array> FilteredArray;
    PARQUET_THROW_NOT_OK(Builder.Finish(&FilteredArray));
    return arrow::RecordBatch::Make(InputChunk->schema(), FilteredArray->length(), { FilteredArray });
}
// Scalar version for comparison
DataChunk FilterOperator::ApplyScalarFilter(const DataChunk& InputChunk)
{
    std::shared_ptr<arrow::Int32Array> Column = std::static_pointer_cast<arrow::Int32Array>(InputChunk->column(0));

    const int* InputData = Column->raw_values();
    int64_t InputLength = Column->length();

    arrow::Int32Builder Builder;

    for (int i = 0; i < InputLength; ++i)
    {
        if (InputData[i] > this->ValueToCompare)
        {
            PARQUET_THROW_NOT_OK(Builder.Append(InputData[i]));
        }
    }

    std::shared_ptr<arrow::Array> FilteredArray;
    PARQUET_THROW_NOT_OK(Builder.Finish(&FilteredArray));

    return arrow::RecordBatch::Make(InputChunk->schema(), FilteredArray->length(), { FilteredArray });
}
