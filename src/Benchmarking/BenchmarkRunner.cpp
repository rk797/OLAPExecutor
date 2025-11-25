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

BenchmarkResult BenchmarkRunner::Run(const std::string& TaskName, PlanFactory Factory, long long InputRowCount)
{
    std::vector<long long> Times;
    std::vector<DataChunk> FirstRunResults;
    long long OutputRowCount = 0;

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
            OutputRowCount = IterationRowCount;
        }
    }

    BenchmarkStats Stats = this->CalculateStats(Times, OutputRowCount, InputRowCount);

    std::cout << std::fixed << std::setprecision(2);
    LOG_MESSAGEF("   Mean: %.2f ns", Stats.Mean);
    LOG_MESSAGEF("   Median: %.2f ns", Stats.Median);
    LOG_MESSAGEF("   StdDev: %.2f ns", Stats.StdDev);
    std::cout << std::fixed << std::setprecision(2);
    LOG_MESSAGEF("   Mean: %.2f ns", Stats.Mean);
    LOG_MESSAGEF("   Bandwidth: %.2f GB/s", Stats.ThroughputGBps);

    return { Stats, FirstRunResults };
}

BenchmarkStats BenchmarkRunner::CalculateStats(std::vector<long long>& Times, long long OutputRowCount, long long InputRowCount)
{
    std::sort(Times.begin(), Times.end());

    BenchmarkStats Stats;
    Stats.RowCount = OutputRowCount;
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

    // TODO: Ask the operator for the data width
    double TotalBytes = (double)InputRowCount * sizeof(int32_t);

    double TotalGB = TotalBytes / 1000000000.0;
	double TimeSeconds = Stats.Mean / 1000000000.0; // Convert ns to seconds

    Stats.ThroughputGBps = TotalGB / TimeSeconds;

    return Stats;
}

void BenchmarkRunner::PrintComparison(const std::string& BaselineName, const BenchmarkStats& Baseline, const std::string& CandidateName, const BenchmarkStats& Candidate)
{
    LOG_TITLE("PERFORMANCE COMPARISON", "");
    double SpeedupRatio = Baseline.Median / Candidate.Median;

    if (SpeedupRatio > 1.0)
    {
        LOG_MESSAGEF("%s is %.2fx FASTER than %s", CandidateName.c_str(), SpeedupRatio, BaselineName.c_str());
    }
    else
    {
        LOG_MESSAGEF("%s is %.2fx FASTER than %s", BaselineName.c_str(), 1.0 / SpeedupRatio, CandidateName.c_str());
    }

    LOG_MESSAGEF("   %s Bandwidth: %.2f GB/s", BaselineName.c_str(), Baseline.ThroughputGBps);
    LOG_MESSAGEF("   %s Bandwidth: %.2f GB/s", CandidateName.c_str(), Candidate.ThroughputGBps);
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