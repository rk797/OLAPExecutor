#include "pch.h"
#include <chrono> 
#include "Misc/Logger.h"
#include "OperatorImpl/ScanOperator.h"
#include "OperatorImpl/FilterOperator.h"

int main() {

#if defined(__AVX2__)
    std::cout << "AVX2 instructions are enabled" << std::endl;
#elif defined(__AVX512F__)
    std::cout << "AVX-512 Foundation instructions are enabled (but AVX2 requested)" << std::endl;
#else
    std::cout << "AVX2 not enabled. Check compiler flags." << std::endl;
#endif

    Logger::Init();

    std::string TestFile = "TEST_DATA.parquet";
    int FilterValue = 100;

    std::cout << "Building query: SELECT * FROM " << TestFile << " WHERE IntColumn > " << FilterValue << std::endl;

    long long AvxRowCount = 0;
    long long ScalarRowCount = 0;

    // avx benchmark
    // check FilterOperator.h for implementation
    try
    {
        LOG_TITLE("BENCHMARK", "STARTING AVX RUN...");
        auto StartTime = std::chrono::high_resolution_clock::now();

        std::unique_ptr<Operator> ScanOp = std::make_unique<ScanOperator>(TestFile);
        std::unique_ptr<Operator> FilterOp = std::make_unique<FilterOperator>(std::move(ScanOp), FilterValue, FilterOperator::FilterMode::Avx);

        DataChunk ResultChunk;
        while ((ResultChunk = FilterOp->Next()) != nullptr)
        {
            AvxRowCount += ResultChunk->num_rows();
        }

        auto EndTime = std::chrono::high_resolution_clock::now();
        auto Duration = std::chrono::duration_cast<std::chrono::microseconds>(EndTime - StartTime);

        LOG_TITLE("BENCHMARK", "AVX RUN FINISHED");
        LOG_MESSAGEF("AVX Time (microseconds): %d", Duration.count());
        LOG_MESSAGEF("AVX Result Rows: %d", AvxRowCount);
    }
    catch (const std::exception& e)
    {
        LOG_TITLE("STATUS", "AVX QUERY FAILED");
        LOG_TITLE("STATUS", e.what());
    }

    std::cout << "------------------------------------" << std::endl;

    // scalar run
    try
    {
        LOG_TITLE("BENCHMARK", "STARTING SCALAR RUN...");
        auto StartTime = std::chrono::high_resolution_clock::now();

        std::unique_ptr<Operator> ScanOp = std::make_unique<ScanOperator>(TestFile);
        std::unique_ptr<Operator> FilterOp = std::make_unique<FilterOperator>(std::move(ScanOp), FilterValue, FilterOperator::FilterMode::Scalar);

        DataChunk ResultChunk;
        while ((ResultChunk = FilterOp->Next()) != nullptr)
        {
            ScalarRowCount += ResultChunk->num_rows();
        }

        auto EndTime = std::chrono::high_resolution_clock::now();
        auto Duration = std::chrono::duration_cast<std::chrono::microseconds>(EndTime - StartTime);

        LOG_TITLE("BENCHMARK", "SCALAR RUN FINISHED");
        LOG_MESSAGEF("Scalar Time (microseconds): %d", Duration.count());
        LOG_MESSAGEF("Scalar Result Rows: %d", ScalarRowCount);
    }
    catch (const std::exception& e)
    {
        LOG_TITLE("STATUS", "SCALAR QUERY FAILED");
        LOG_TITLE("STATUS", e.what());
    }

    std::cin.get();
    return 0;
}

