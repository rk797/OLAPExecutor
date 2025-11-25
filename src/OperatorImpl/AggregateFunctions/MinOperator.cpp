#include "pch.h"
#include "MinOperator.h"
#include <immintrin.h>
#include <algorithm>

MinOperator::MinOperator(std::unique_ptr<Operator> Child, ExecutionMode Mode)
    : Operator(Mode)
{
    this->ChildOperator = std::move(Child);
    this->bFinished = false;
}

int32_t MinOperator::HMin256(__m256i v)
{
    // split 256-bit into two 128-bit halves
    __m128i lo128 = _mm256_castsi256_si128(v);
    __m128i hi128 = _mm256_extracti128_si256(v, 1);

    // find min between the two halves (element wise min)
    __m128i min128 = _mm_min_epi32(lo128, hi128);

	// spread hi64 to all lanes
    __m128i hi64 = _mm_unpackhi_epi64(min128, min128);
	__m128i min64 = _mm_min_epi32(min128, hi64); // find the min between the two 64-bit halves

    
    __m128i hi32 = _mm_shuffle_epi32(min64, _MM_SHUFFLE(1, 1, 1, 1)); // shuffle to all lanes
    __m128i result = _mm_min_epi32(min64, hi32);

	return _mm_cvtsi128_si32(result); // extract the lowest slot which contains the horizontal min
}

int32_t MinOperator::CalculateAvxMin(const int32_t* Data, int64_t Length)
{
    // Initialize with INT_MAX so any real number is smaller
    __m256i MinVector = _mm256_set1_epi32(INT_MAX);
    int64_t i = 0;

    for (; i <= Length - 8; i += 8)
    {
        __m256i DataVector = _mm256_loadu_si256((__m256i*)(Data + i));
        // element wise minimum
        MinVector = _mm256_min_epi32(MinVector, DataVector);
    }

	// horizontal reduction to get the minimum value from the vector
    int32_t GlobalMin = this->HMin256(MinVector);

    // scalar cleanup
    for (; i < Length; ++i)
    {
        if (Data[i] < GlobalMin)
        {
            GlobalMin = Data[i];
        }
    }

    return GlobalMin;
}

int32_t MinOperator::CalculateScalarMin(const int32_t* Data, int64_t Length)
{
    int32_t MinVal = INT_MAX;
    for (int64_t i = 0; i < Length; ++i)
    {
        if (Data[i] < MinVal)
        {
            MinVal = Data[i];
        }
    }
    return MinVal;
}

DataChunk MinOperator::Next()
{
    if (this->bFinished) return nullptr;

    int32_t GlobalMin = INT_MAX;

    while (true)
    {
        DataChunk Chunk = this->ChildOperator->Next();
        if (Chunk == nullptr) break;

        std::shared_ptr<arrow::Int32Array> Column = std::static_pointer_cast<arrow::Int32Array>(Chunk->column(0));
        const int32_t* RawValues = Column->raw_values();
        int32_t BatchMin;

        if (this->CurrentMode == ExecutionMode::AVX2)
        {
            BatchMin = this->CalculateAvxMin(RawValues, Column->length());
        }
        else
        {
            BatchMin = this->CalculateScalarMin(RawValues, Column->length());
        }

        if (BatchMin < GlobalMin)
        {
            GlobalMin = BatchMin;
        }
    }

    this->bFinished = true;

    // Return result
    arrow::Int32Builder Builder;
    PARQUET_THROW_NOT_OK(Builder.Append(GlobalMin));
    std::shared_ptr<arrow::Array> ResultArray;
    PARQUET_THROW_NOT_OK(Builder.Finish(&ResultArray));

    auto ResultSchema = arrow::schema({ arrow::field("min", arrow::int32()) });
    return arrow::RecordBatch::Make(ResultSchema, 1, { ResultArray });
}