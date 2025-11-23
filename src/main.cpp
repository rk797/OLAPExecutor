#include "pch.h"
#include <iostream>
#include <vector>
#include "Misc/Logger.h"
#include "OperatorImpl/ScanOperator.h"
#include "OperatorImpl/MemoryScanOperator.h"
#include "OperatorImpl/AggregateFunctions/SumOperator.h"
#include "OperatorImpl/FilterOperator.h"
#include "Benchmarking/BenchmarkRunner.h"


/**
    Move all data from disk into RAM
*/
std::vector<DataChunk> PreloadData(const std::string& FileName)
{
    LOG_TITLE("SETUP", "Pre-loading data into RAM..");
    std::vector<DataChunk> Chunks;
    ScanOperator Scan(FileName);

    DataChunk Chunk; // batch of rows
    while ((Chunk = Scan.Next()) != nullptr)
    {
        Chunks.push_back(Chunk);
    }
    LOG_MESSAGEF("Loaded %zu chunks into memory.", Chunks.size());
    return Chunks;
}
// TODO:
// - Change benchmarking logic to only measure CPU time
// - Implement more Aggregate functions (AVG, COUNT, MIN, MAX)
int main()
{
#ifdef _WIN32
    SetPriorityClass(GetCurrentProcess(), HIGH_PRIORITY_CLASS);
    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
#endif

    Logger::Init();

    std::string TestFile = "TEST_DATA.parquet";
    BenchmarkRunner Runner(10);

    try
    {
        std::vector<DataChunk> InMemoryData = PreloadData(TestFile);

        std::cout << "============================================================" << std::endl;
        std::cout << "BENCHMARK SUITE: IN-MEMORY (CPU BOUND)" << std::endl;
        std::cout << "============================================================" << std::endl;


        auto ScalarFilterPlan = [&]() -> std::unique_ptr<Operator> 
        {
            auto Scan = std::make_unique<MemoryScanOperator>(InMemoryData);
            return std::make_unique<FilterOperator>(std::move(Scan), 5000, FilterOperator::FilterMode::Scalar);
		};

        auto AvxFilterPlan = [&]() -> std::unique_ptr<Operator> 
        {
            auto Scan = std::make_unique<MemoryScanOperator>(InMemoryData);
            return std::make_unique<FilterOperator>(std::move(Scan), 5000, FilterOperator::FilterMode::Avx);
		};

		BenchmarkResult ScalarFilterRes = Runner.Run("Scalar Filter", ScalarFilterPlan);
		BenchmarkResult AvxFilterRes = Runner.Run("AVX Filter", AvxFilterPlan);
		BenchmarkRunner::PrintComparison("Scalar Filter", ScalarFilterRes.Stats, "AVX Filter", AvxFilterRes.Stats);


        // Returns a unique pointer to the root of the operator tree
        auto ScalarSumPlan = [&]() -> std::unique_ptr<Operator> 
        {
            auto Scan = std::make_unique<MemoryScanOperator>(InMemoryData);
            return std::make_unique<SumOperator>(std::move(Scan), SumOperator::SumMode::Scalar);
        };

        auto AvxSumPlan = [&]() -> std::unique_ptr<Operator> 
        {
            // Define the Source Operator (lowest in the pipeline)
            // The 'MemoryScanOperator' is used here specifically to read the data
            // from the pre-loaded 'InMemoryData' vector (captured by reference '[&]').
            // This isolates the benchmark to measure only the CPU computation time,
            // excluding slow disk I/O and file parsing overhead.
            auto Scan = std::make_unique<MemoryScanOperator>(InMemoryData);

            // Define the Aggregation Operator (the root of this simple pipeline)
            // This creates the SumOperator, which will pull data from the ScanOperator.
            // std::move(Scan) it transfers ownership of the 'Scan' operator
            // to the 'SumOperator', forming the single-path pipeline (Scan -> Sum).
            // SumOperator::SumMode::Avx explicitly enables your vectorized (SIMD)
            // summation logic, which is what we are trying to benchmark.
            return std::make_unique<SumOperator>(
                std::move(Scan),
                SumOperator::SumMode::Avx
            );
        };

        BenchmarkResult ScalarSumRes = Runner.Run("Scalar Sum", ScalarSumPlan);
        BenchmarkResult AvxSumRes = Runner.Run("AVX Sum", AvxSumPlan);

        BenchmarkRunner::PrintComparison("Scalar Sum", ScalarSumRes.Stats, "AVX Sum", AvxSumRes.Stats);
        BenchmarkRunner::Verify(ScalarSumRes.ResultChunks, AvxSumRes.ResultChunks);

    }
    catch (const std::exception& e)
    {
        LOG_TITLE("CRITICAL ERROR", e.what());
        return 1;
    }

    std::cin.get();
    return 0;
}