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

    BenchmarkResult Run(const std::string& TaskName, PlanFactory Factory);

    static void PrintComparison(const std::string& BaselineName, const BenchmarkStats& Baseline, const std::string& CandidateName, const BenchmarkStats& Candidate);

    static void Verify(const std::vector<DataChunk>& Expected, const std::vector<DataChunk>& Actual);

private:
    int NumRuns;

    void WarmupCpu();
    BenchmarkStats CalculateStats(std::vector<long long>& Times, long long RowCount);
};