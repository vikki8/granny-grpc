#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <utility>

namespace faabric::util {

// Immutable summary of an app's recent performance, computed over a trailing
// time window.
struct PerfSnapshot
{
    bool valid = false;
    double p50RttUs = 0.0;
    double p99RttUs = 0.0;
    double rps = 0.0;
    double cpuPct = 0.0;
    int sampleCount = 0;
};

// Process-wide, lock-guarded rolling window of client-observed RPC latencies
// plus the latest CPU-utilisation sample. It is fed by the gRPC client path
// (per-call RTTs) and by the host-utilisation sampler (CPU %), and read by the
// scheduler when a migration point is reached so the perf-adaptive policy can
// react to live performance instead of only slot occupancy.
//
// The monitor is intentionally lightweight (a bounded deque + a mutex) so that
// recording a sample adds negligible overhead to the hot RPC path.
class PerfMonitor
{
  public:
    static constexpr int64_t kDefaultWindowMs = 2000;

    static PerfMonitor& instance();

    void recordRtt(int64_t rttNs, int64_t sendTsNs);

    void setCpuPct(double pct);

    PerfSnapshot snapshot(int64_t windowMs = kDefaultWindowMs) const;

    void reset();

  private:
    PerfMonitor() = default;

    mutable std::mutex mx;
    std::deque<std::pair<int64_t, int64_t>> samples;
    double lastCpuPct = 0.0;

    static constexpr std::size_t kMaxSamples = 65536;
};

}
