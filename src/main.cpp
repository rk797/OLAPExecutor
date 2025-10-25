#include "pch.h"
#include "Logger.h"
#include <immintrin.h>
#include <vector>
#include <numeric>
#include <random>
#include <chrono>

void ScalarAdd(int* Result, const int* A, const int* B, int Count)
{
    for (int i = 0; i < Count; ++i)
    {
        Result[i] = A[i] + B[i];
    }
}

void SseAdd(int* Result, const int* A, const int* B, int Count)
{
    int i = 0;

    for (; i <= Count - 4; i += 4)
    {
        __m128i TrayA = _mm_loadu_si128((__m128i*)(A + i));
        __m128i TrayB = _mm_loadu_si128((__m128i*)(B + i));

        __m128i TrayResult = _mm_add_epi32(TrayA, TrayB);

        _mm_storeu_si128((__m128i*)(Result + i), TrayResult);
    }

    for (; i < Count; ++i)
    {
        Result[i] = A[i] + B[i];
    }
}


int main() {

#if defined(__AVX512F__)
    std::cout << "AVX-512 Foundation instructions are enabled" << std::endl;
#else
    std::cout << "AVX-512F not enabled. Check compiler flags." << std::endl;
#endif
    Logger::Init();

    const int DataSize = 10000000;
    const int NumIterations = 100;

    std::vector<int> VectorA(DataSize);
    std::vector<int> VectorB(DataSize);
    std::vector<int> VectorResult(DataSize);

    std::mt19937 Rng(1234);
    std::uniform_int_distribution<int> Dist(0, 1000);

    for (int i = 0; i < DataSize; ++i)
    {
        VectorA[i] = Dist(Rng);
        VectorB[i] = Dist(Rng);
    }

    std::cout << "Starting benchmarks with " << DataSize << " elements..." << std::endl;

    auto StartTime = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NumIterations; ++i)
    {
        ScalarAdd(VectorResult.data(), VectorA.data(), VectorB.data(), DataSize);
    }

    auto EndTime = std::chrono::high_resolution_clock::now();
    auto DurationScalar = std::chrono::duration_cast<std::chrono::milliseconds>(EndTime - StartTime).count();

    volatile int ChecksumScalar = VectorResult[0];

    StartTime = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NumIterations; ++i)
    {
        SseAdd(VectorResult.data(), VectorA.data(), VectorB.data(), DataSize);
    }

    EndTime = std::chrono::high_resolution_clock::now();
    auto DurationSse = std::chrono::duration_cast<std::chrono::milliseconds>(EndTime - StartTime).count();

    volatile int ChecksumSse = VectorResult[0];

    long long TotalElements = (long long)DataSize * NumIterations;
    long long TotalBytes = TotalElements * sizeof(int) * 2;

    double ThroughputScalar = (double)TotalBytes / (DurationScalar / 1000.0) / (1024 * 1024 * 1024);
    double ThroughputSse = (double)TotalBytes / (DurationSse / 1000.0) / (1024 * 1024 * 1024);

    std::cout << "[ RESULTS ]" << std::endl;
    std::cout << "ScalarAdd Time: " << DurationScalar << " ms" << std::endl;
    std::cout << "ScalarAdd Throughput: " << ThroughputScalar << " GB/s" << std::endl;
    std::cout << std::endl;
    std::cout << "SseAdd Time:    " << DurationSse << " ms" << std::endl;
    std::cout << "SseAdd Throughput:    " << ThroughputSse << " GB/s" << std::endl;

    std::cin.get();
    return 0;
}