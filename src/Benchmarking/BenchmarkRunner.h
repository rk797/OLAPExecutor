#pragma once
#include <vector>
#include <string>
#include <functional>
#include <memory>
#include "../OperatorImpl/Operator.h"
struct BenchmarkStats
{
    double Mean;
    double Median;
    double StdDev;
    long long Min;
    long long Max;
    long long RowCount;

	// E (Joules) = P (Watts) * T (seconds)
    // 
    // - Static Power (Leakage/Background): The RAM and Motherboard consume constant power (e.g., 20W) just by being on.
    // - Dynamic Power (Switching): The CPU consumes extra power (e.g., +10W) to run AVX instructions.
    // 
    // Scenario A (Low Throughput/Scalar):
    //   Low Power (20W) * Long Time (10s) = 200 Joules
    // 
    // Scenario B (High Throughput/AVX):
    //   High Power (30W) * Short Time (1s) = 30 Joules
    // 
    // therefore, maximizing Throughput (GB/s) minimizes `Time`, which usually minimizes Total Energy
    // by paying off the constant Static Power cost over more data per second.
    double ThroughputGBps;
};

struct BenchmarkResult
{
    BenchmarkStats Stats;
    std::vector<DataChunk> ResultChunks; 
};

class BenchmarkRunner
{
public:
    // Defines a function that returns a new Operator tree (Query Plan)
    using PlanFactory = std::function<std::unique_ptr<Operator>()>;

    BenchmarkRunner(int NumRuns = 50);

    BenchmarkResult Run(const std::string& TaskName, PlanFactory Factory, long long InputRowCount);

    static void PrintComparison(const std::string& BaselineName, const BenchmarkStats& Baseline, const std::string& CandidateName, const BenchmarkStats& Candidate);

    static void Verify(const std::vector<DataChunk>& Expected, const std::vector<DataChunk>& Actual);

private:
    int NumRuns;

    void WarmupCpu();
    BenchmarkStats CalculateStats(std::vector<long long>& Times, long long OutputRowCount, long long InputRowCount);
};