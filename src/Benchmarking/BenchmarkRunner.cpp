#include "pch.h"
#include "BenchmarkRunner.h"
#include <chrono>
#include <numeric>
#include <algorithm>
#include <cmath>
#include <iostream>
#include <iomanip>
#include "../Misc/Logger.h"


BenchmarkRunner::BenchmarkRunner(int NumRuns)
    : NumRuns(NumRuns)
{

}

void BenchmarkRunner::WarmupCpu()
{
    volatile int sum = 0;
    for (int i = 0; i < 10000000; ++i)
    {
        sum += i * i;
    }
}

BenchmarkResult BenchmarkRunner::Run(const std::string& TaskName, PlanFactory Factory)
{
    std::vector<long long> Times;
    std::vector<DataChunk> FirstRunResults;
    long long TotalRowCount = 0;

    LOG_TITLE("BENCHMARK", "RUNNING: " + TaskName);

    for (int i = 0; i < this->NumRuns; ++i)
    {
        this->WarmupCpu();

        long long IterationRowCount = 0;
        std::vector<DataChunk> CurrentResults;

        // Start Timer
        auto StartTime = std::chrono::high_resolution_clock::now();

        // Execute the query
        std::unique_ptr<Operator> Op = Factory();
        DataChunk Chunk;
        while ((Chunk = Op->Next()) != nullptr)
        {
            IterationRowCount += Chunk->num_rows();
            if (i == 0)
            {
                CurrentResults.push_back(Chunk);
            }
        }

        // Stop Timer
        auto EndTime = std::chrono::high_resolution_clock::now();
        long long Duration = std::chrono::duration_cast<std::chrono::nanoseconds>(EndTime - StartTime).count();

        Times.push_back(Duration);
        LOG_MESSAGEF("[ %s Run %d ] %lld ns (%lld rows)", TaskName.c_str(), i + 1, Duration, IterationRowCount);

        if (i == 0)
        {
            FirstRunResults = std::move(CurrentResults);
            TotalRowCount = IterationRowCount;
        }
    }

    BenchmarkStats Stats = this->CalculateStats(Times, TotalRowCount);

    std::cout << std::fixed << std::setprecision(2);
    LOG_MESSAGEF("   Mean: %.2f ns", Stats.Mean);
    LOG_MESSAGEF("   Median: %.2f ns", Stats.Median);
    LOG_MESSAGEF("   StdDev: %.2f ns", Stats.StdDev);

    return { Stats, FirstRunResults };
}

BenchmarkStats BenchmarkRunner::CalculateStats(std::vector<long long>& Times, long long RowCount)
{
    std::sort(Times.begin(), Times.end());

    BenchmarkStats Stats;
    Stats.RowCount = RowCount;
    Stats.Min = Times.front();
    Stats.Max = Times.back();
    Stats.Median = Times[Times.size() / 2];
    Stats.Mean = std::accumulate(Times.begin(), Times.end(), 0.0) / Times.size();

    double VarianceSum = 0.0;
    for (auto Time : Times)
    {
        double Diff = Time - Stats.Mean;
        VarianceSum += Diff * Diff;
    }
    Stats.StdDev = std::sqrt(VarianceSum / Times.size());

    return Stats;
}

void BenchmarkRunner::PrintComparison(const std::string& BaselineName, const BenchmarkStats& Baseline,
    const std::string& CandidateName, const BenchmarkStats& Candidate)
{
    LOG_TITLE("PERFORMANCE COMPARISON", "");
    double SpeedupRatio = Baseline.Median / Candidate.Median;

    if (SpeedupRatio > 1.0)
    {
        LOG_MESSAGEF("%s is %.2fx FASTER than %s (%.2f%% speedup)",
            CandidateName.c_str(), SpeedupRatio, BaselineName.c_str(), (SpeedupRatio - 1.0) * 100.0);
    }
    else
    {
        LOG_MESSAGEF("%s is %.2fx FASTER than %s (%.2f%% regression)",
            BaselineName.c_str(), 1.0 / SpeedupRatio, CandidateName.c_str(), ((1.0 / SpeedupRatio) - 1.0) * 100.0);
    }
}

void BenchmarkRunner::Verify(const std::vector<DataChunk>& Expected, const std::vector<DataChunk>& Actual)
{
    LOG_TITLE("VERIFICATION", "");

    if (Expected.size() != Actual.size())
    {
        LOG_MESSAGEF("FAILED: Chunk count mismatch (Expected=%zu, Actual=%zu)", Expected.size(), Actual.size());
        return;
    }

    for (size_t i = 0; i < Expected.size(); ++i)
    {
        // Assuming DataChunk (RecordBatch) has an Equals method or you use the Arrow Equals
        if (!Expected[i]->Equals(*Actual[i]))
        {
            LOG_MESSAGEF("FAILED: Data mismatch in chunk %zu", i);
            return;
        }
    }

    LOG_MESSAGE("PASSED: Results are identical.");
}