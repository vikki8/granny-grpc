//COMP70073

#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/batch.h>

#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>

namespace faabric::batch_scheduler {

// Thresholds are configured via .env:
//   PERF_LAT_P50_US     - p50 round-trip above this is unhealthy
//   PERF_MIN_RPS        - rps below this is unhealthy
//   PERF_CPU_PCT        - CPU% line: below => scale-in, at/above => scale-out 
//   PERF_SUSTAIN_WINDOWS- consecutive unhealthy windows before acting 
//   PERF_MAX_ATTEMPTS   - non-improving migrations before giving up 
//   PERF_COOLDOWN_MS    - settle/min-gap between migrations of one app 

class PerfAdaptiveScheduler final : public BatchScheduler
{
  public:
    PerfAdaptiveScheduler();

    std::shared_ptr<SchedulingDecision> makeSchedulingDecision(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

  private:
    // Which way the policy moved (or would move) an app.
    enum class Direction
    {
        None,
        ScaleIn,
        ScaleOut
    };

    bool isFirstDecisionBetter(
      std::shared_ptr<SchedulingDecision> decisionA,
      std::shared_ptr<SchedulingDecision> decisionB) override;

    std::vector<Host> getSortedHosts(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const DecisionType& decisionType) override;

    // Thresholds, read from the environment once at construction.
    double latP50HiUs;
    double minRps;
    double cpuThreshPct;
    int sustainWindows;
    int maxAttempts;
    int64_t cooldownMs;
    // Minimum fractional p50 improvement for a migration to "count" as helping.
    double improveFrac;

    // Rolling per-app decision state (cooldown, sustained-crossing history,
    // and give-up bookkeeping).
    struct AppPerfState
    {
        // Trailing "was latency high?" flags, newest at the back, capped at
        // sustainWindows entries.
        std::deque<bool> latHighHist;
        // Wall time of the last emitted migration.
        std::chrono::steady_clock::time_point lastMigration{};
        bool hasMigrated = false;
        // p50 observed at the moment of the last migration (for improvement
        // check).
        double p50AtLastMigration = 0.0;
        // Whether we are still waiting to see if the last migration helped.
        bool pendingEval = false;
        // Direction of the last emitted migration (the one pendingEval judges).
        Direction lastDirection = Direction::None;
        // Consecutive non-improving migrations, counted per direction so that
        // reversing course is not charged for the other direction's failures.
        int failScaleIn = 0;
        int failScaleOut = 0;
        // Latched once both directions are exhausted.
        bool gaveUp = false;
    };

    std::mutex stateMx;
    std::map<int32_t, AppPerfState> appState;
};
}
