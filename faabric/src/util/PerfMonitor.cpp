//COMP70073

#include <faabric/util/perf_monitor.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <vector>

namespace faabric::util {

static int64_t steadyNowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

PerfMonitor& PerfMonitor::instance()
{
    static PerfMonitor inst;
    return inst;
}

void PerfMonitor::recordRtt(int64_t rttNs, int64_t sendTsNs)
{
    if (rttNs < 0) {
        return;
    }

    std::lock_guard<std::mutex> lock(mx);
    samples.emplace_back(sendTsNs, rttNs);

    while (samples.size() > kMaxSamples) {
        samples.pop_front();
    }
}

void PerfMonitor::setCpuPct(double pct)
{
    std::lock_guard<std::mutex> lock(mx);
    lastCpuPct = pct;
}

PerfSnapshot PerfMonitor::snapshot(int64_t windowMs) const
{
    const int64_t nowNs = steadyNowNs();
    const int64_t windowNs = windowMs * 1000000LL;
    const int64_t cutoff = nowNs - windowNs;

    std::vector<int64_t> recent;
    double cpuPct = 0.0;
    {
        std::lock_guard<std::mutex> lock(mx);
        cpuPct = lastCpuPct;
        recent.reserve(samples.size());
        for (const auto& [sendTsNs, rttNs] : samples) {
            if (sendTsNs >= cutoff) {
                recent.push_back(rttNs);
            }
        }
    }

    PerfSnapshot snap;
    snap.cpuPct = cpuPct;
    snap.sampleCount = static_cast<int>(recent.size());
    if (recent.empty()) {
        snap.valid = false;
        return snap;
    }

    std::sort(recent.begin(), recent.end());

    auto percentileUs = [&recent](double q) -> double {
        // Nearest-rank percentile, converted from ns to us.
        auto idx = static_cast<std::size_t>(std::ceil(q * recent.size()));
        if (idx > 0) {
            idx -= 1;
        }
        idx = std::min(idx, recent.size() - 1);
        return static_cast<double>(recent[idx]) / 1000.0;
    };

    snap.valid = true;
    snap.p50RttUs = percentileUs(0.50);
    snap.p99RttUs = percentileUs(0.99);
    snap.rps =
      static_cast<double>(recent.size()) / (static_cast<double>(windowMs) / 1000.0);
    return snap;
}

void PerfMonitor::reset()
{
    std::lock_guard<std::mutex> lock(mx);
    samples.clear();
    lastCpuPct = 0.0;
}

}
