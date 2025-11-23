#include "pch.h"
#include "SumOperator.h"
#include <immintrin.h>

SumOperator::SumOperator(std::unique_ptr<Operator> Child, SumMode Mode)
{
    this->ChildOperator = std::move(Child);
    this->CurrentMode = Mode;
    this->bFinished = false;
}

// returns the horizontal sum of a 256-bit register containing 8 int32_t values
int32_t SumOperator::HSum256(__m256i v)
{
	// split the 256-bit vector into two 128-bit halves
    __m128i Low128 = _mm256_castsi256_si128(v);
    __m128i High128 = _mm256_extracti128_si256(v, 1);

    // add high and low 128 bits together
    __m128i Sum128 = _mm_add_epi32(Low128, High128);

    // [ A, B, C, D ] -> [ C, D, C, D ]
    __m128i High64 = _mm_unpackhi_epi64(Sum128, Sum128);

    //   [ A, B, C, D ]
    // + [ C, D, C, D ]
	// = [ A+C, B+D, C+C, D+D ] Only the first 2 are useful
    __m128i Sum64 = _mm_add_epi32(Sum128, High64);

    __m128i High32 = _mm_shuffle_epi32(Sum64, _MM_SHUFFLE(1, 1, 1, 1)); // shuffle the value of index 1 to all positions
	__m128i Result = _mm_add_epi32(Sum64, High32); // add them together

	return _mm_cvtsi128_si32(Result); // result is in idx 0. Extract the lowest slot with cvtsi128_si32
}

long long SumOperator::CalculateAvxSum(const int32_t* Data, int64_t Length)
{
    // Initialize a vector of zeros [0, 0, 0, 0, 0, 0, 0, 0]
    __m256i SumVector = _mm256_setzero_si256();

    int64_t i = 0;
    // process in batches of 8 and iteratively add to SumVector (128 bit vector addition)
    for (; i <= Length - 8; i += 8)
    {
        __m256i DataVector = _mm256_loadu_si256((__m256i*)(Data + i));
        // perform element wise vector addition
        SumVector = _mm256_add_epi32(SumVector, DataVector);
    }

	// reduce the final vector to a single scalar sum
    long long TotalSum = this->HSum256(SumVector);

    for (; i < Length; ++i)
    {
        TotalSum += Data[i];
    }

    return TotalSum;
}

long long SumOperator::CalculateScalarSum(const int32_t* Data, int64_t Length)
{
    long long Sum = 0;
    // Standard sequential accumulation
    for (int64_t i = 0; i < Length; ++i)
    {
        Sum += Data[i];
    }
    return Sum;
}
DataChunk SumOperator::Next()
{
    if (this->bFinished)
    {
        return nullptr;
    }

    long long GrandTotal = 0;

    
    while (true)
    {
        DataChunk Chunk = this->ChildOperator->Next();
        if (Chunk == nullptr)
        {
            break;
        }

        std::shared_ptr<arrow::Int32Array> Column = std::static_pointer_cast<arrow::Int32Array>(Chunk->column(0));
        const int32_t* RawValues = Column->raw_values();

        if (this->CurrentMode == SumMode::Avx)
        {
            GrandTotal += this->CalculateAvxSum(RawValues, Column->length());
        }
        else
        {
            GrandTotal += this->CalculateScalarSum(RawValues, Column->length());
        }
    }

    this->bFinished = true;

    // Return single row result
    arrow::Int64Builder Builder;
    PARQUET_THROW_NOT_OK(Builder.Append(GrandTotal));
    std::shared_ptr<arrow::Array> ResultArray;
    PARQUET_THROW_NOT_OK(Builder.Finish(&ResultArray));

    auto ResultSchema = arrow::schema({ arrow::field("sum", arrow::int64()) });
    return arrow::RecordBatch::Make(ResultSchema, 1, { ResultArray });
}