#include <faabric/batch-scheduler/PerfAdaptiveScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/batch.h>
#include <faabric/util/environment.h>
#include <faabric/util/logging.h>

#include <fmt/format.h>

#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace faabric::batch_scheduler {

// Env helpers
static double envDouble(const std::string& key, double deflt)
{
    std::string v = faabric::util::getEnvVar(key, "");
    if (v.empty()) {
        return deflt;
    }
    try {
        return std::stod(v);
    } catch (...) {
        return deflt;
    }
}

static int64_t envInt64(const std::string& key, int64_t deflt)
{
    std::string v = faabric::util::getEnvVar(key, "");
    if (v.empty()) {
        return deflt;
    }
    try {
        return static_cast<int64_t>(std::stoll(v));
    } catch (...) {
        return deflt;
    }
}

// Placement helpers (mirroring the Compact scheduler)
static std::map<std::string, int> getHostFreqCount(
  std::shared_ptr<SchedulingDecision> decision)
{
    std::map<std::string, int> hostFreqCount;
    for (const auto& host : decision->hosts) {
        hostFreqCount[host] += 1;
    }
    return hostFreqCount;
}

static std::shared_ptr<SchedulingDecision> minimiseNumOfMigrations(
  std::shared_ptr<SchedulingDecision> newDecision,
  std::shared_ptr<SchedulingDecision> oldDecision)
{
    auto decision = std::make_shared<SchedulingDecision>(oldDecision->appId,
                                                         oldDecision->groupId);

    auto hostFreqCount = getHostFreqCount(newDecision);

    auto nextHostWithSlots = [&hostFreqCount]() -> std::string {
        for (auto [ip, slots] : hostFreqCount) {
            if (slots > 0) {
                return ip;
            }
        }
        throw std::runtime_error("No next host with slots found!");
    };

    assert(newDecision->hosts.size() == oldDecision->hosts.size());

    for (int i = 0; i < oldDecision->hosts.size(); i++) {
        auto oldHost = oldDecision->hosts.at(i);

        if (hostFreqCount.contains(oldHost) && hostFreqCount.at(oldHost) > 0) {
            decision->addMessageInPosition(i,
                                           oldHost,
                                           oldDecision->messageIds.at(i),
                                           oldDecision->appIdxs.at(i),
                                           oldDecision->groupIdxs.at(i),
                                           oldDecision->mpiPorts.at(i));

            hostFreqCount.at(oldHost) -= 1;
        }
    }

    for (int i = 0; i < oldDecision->hosts.size(); i++) {
        if (decision->nFunctions <= i || decision->hosts.at(i).empty()) {
            auto nextHost = nextHostWithSlots();
            decision->addMessageInPosition(i,
                                           nextHost,
                                           oldDecision->messageIds.at(i),
                                           oldDecision->appIdxs.at(i),
                                           oldDecision->groupIdxs.at(i),
                                           -1);

            hostFreqCount.at(nextHost) -= 1;
        }
    }

#ifndef NDEBUG
    for (auto [host, freq] : hostFreqCount) {
        assert(freq == 0);
    }
#endif

    return decision;
}

PerfAdaptiveScheduler::PerfAdaptiveScheduler()
  : latP50HiUs(envDouble("PERF_LAT_P50_US", 800.0))
  , minRps(envDouble("PERF_MIN_RPS", 0.0))
  , cpuThreshPct(envDouble("PERF_CPU_PCT", 50.0))
  , sustainWindows(static_cast<int>(envInt64("PERF_SUSTAIN_WINDOWS", 2)))
  , maxAttempts(static_cast<int>(envInt64("PERF_MAX_ATTEMPTS", 2)))
  , cooldownMs(envInt64("PERF_COOLDOWN_MS", 3000))
  , improveFrac(envDouble("PERF_IMPROVE_FRAC", 0.15))
{
    if (sustainWindows < 1) {
        sustainWindows = 1;
    }
    if (maxAttempts < 1) {
        maxAttempts = 1;
    }

    SPDLOG_INFO(
      "[PERF POLICY] perf-adaptive scheduler active: latency unhealthy when "
      "p50>{:.0f}us (sustained {} windows) or rps<{:.0f}; scale-in when "
      "cpu<{:.0f}%, scale-out when cpu>={:.0f}%; give up after {} "
      "non-improving migrations; cooldown={}ms",
      latP50HiUs,
      sustainWindows,
      minRps,
      cpuThreshPct,
      cpuThreshPct,
      maxAttempts,
      cooldownMs);
}

bool PerfAdaptiveScheduler::isFirstDecisionBetter(
  std::shared_ptr<SchedulingDecision> decisionA,
  std::shared_ptr<SchedulingDecision> decisionB)
{
    throw std::runtime_error(
      "Method not supported for PERF_ADAPTIVE scheduler");
}

std::vector<Host> PerfAdaptiveScheduler::getSortedHosts(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  const DecisionType& decisionType)
{
    std::vector<Host> sortedHosts;
    for (auto [ip, host] : hostMap) {
        sortedHosts.push_back(host);
    }

    std::map<std::string, int> hostFreqCount;
    if (decisionType != DecisionType::NEW &&
        inFlightReqs.contains(req->appid())) {
        hostFreqCount = getHostFreqCount(inFlightReqs.at(req->appid()).second);
    }

    auto isFirstHostLarger = [&](const Host& hostA, const Host& hostB) -> bool {
        int nAvailableA = numSlotsAvailable(hostA);
        int nAvailableB = numSlotsAvailable(hostB);
        if (nAvailableA != nAvailableB) {
            return nAvailableA > nAvailableB;
        }

        int nSlotsA = numSlots(hostA);
        int nSlotsB = numSlots(hostB);
        if (nSlotsA != nSlotsB) {
            return nSlotsA > nSlotsB;
        }

        return getIp(hostA) > getIp(hostB);
    };

    auto isFirstHostLargerWithFreq = [&](const Host& hostA,
                                         const Host& hostB) -> bool {
        int numInHostA = hostFreqCount.contains(getIp(hostA))
                           ? hostFreqCount.at(getIp(hostA))
                           : 0;
        int numInHostB = hostFreqCount.contains(getIp(hostB))
                           ? hostFreqCount.at(getIp(hostB))
                           : 0;
        if (numInHostA != numInHostB) {
            return numInHostA > numInHostB;
        }

        return isFirstHostLarger(hostA, hostB);
    };

    switch (decisionType) {
        case DecisionType::SCALE_CHANGE: {
            std::sort(sortedHosts.begin(),
                      sortedHosts.end(),
                      isFirstHostLargerWithFreq);
            break;
        }
        case DecisionType::NEW:
        default: {
            std::sort(
              sortedHosts.begin(), sortedHosts.end(), isFirstHostLarger);
            break;
        }
    }

    return sortedHosts;
}

std::shared_ptr<SchedulingDecision>
PerfAdaptiveScheduler::makeSchedulingDecision(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<BatchExecuteRequest> req)
{
    auto decisionType = getDecisionType(inFlightReqs, req);
-
    // behave like bin packing
    if (decisionType != DecisionType::DIST_CHANGE) {
        auto decision = std::make_shared<SchedulingDecision>(req->appid(), 0);
        auto sortedHosts =
          getSortedHosts(hostMap, inFlightReqs, req, decisionType);

        auto itr = sortedHosts.begin();
        int numLeftToSchedule = req->messages_size();
        int msgIdx = 0;
        while (itr < sortedHosts.end()) {
            int numOnThisHost =
              std::min<int>(numLeftToSchedule, numSlotsAvailable(*itr));
            for (int i = 0; i < numOnThisHost; i++) {
                decision->addMessage(getIp(*itr), req->messages(msgIdx));
                msgIdx++;
            }

            numLeftToSchedule -= numOnThisHost;
            if (numLeftToSchedule == 0) {
                break;
            }
            itr++;
        }

        if (numLeftToSchedule > 0) {
            return std::make_shared<SchedulingDecision>(
              NOT_ENOUGH_SLOTS_DECISION);
        }

        return decision;
    }

    //  metric-driven scale-in / scale-out.
    const int32_t appId = req->appid();
    auto oldDecision = inFlightReqs.at(appId).second;
    const int nFunctions = static_cast<int>(oldDecision->hosts.size());

    if (!req->has_perfmetrics() || !req->perfmetrics().valid()) {
        SPDLOG_DEBUG("[PERF POLICY] app {} migration point without metrics; "
                     "MIGRATE_SKIP",
                     appId);
        return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
    }

    const auto& pm = req->perfmetrics();
    const double p99 = pm.p99rttus();
    const double p50 = pm.p50rttus();
    const double rps = pm.rps();
    const double cpu = pm.cpupct();

    auto hostFreqCount = getHostFreqCount(oldDecision);
    const int distinctHosts = static_cast<int>(hostFreqCount.size());

    std::lock_guard<std::mutex> lock(stateMx);
    auto& st = appState[appId];

    if (st.gaveUp) {
        SPDLOG_INFO("[PERF POLICY] app {} already gave up (slowdown is not a "
                    "placement problem); MIGRATE_SKIP",
                    appId);
        return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
    }

    auto dirName = [](Direction d) {
        return d == Direction::ScaleOut ? "scale-out" : "scale-in";
    };

    // Latency-unhealthy signal for this window
    const bool latHighNow =
      (p50 > latP50HiUs) || (minRps > 0.0 && rps < minRps);
    st.latHighHist.push_back(latHighNow);
    while (static_cast<int>(st.latHighHist.size()) > sustainWindows) {
        st.latHighHist.pop_front();
    }
    const bool sustained =
      static_cast<int>(st.latHighHist.size()) >= sustainWindows &&
      std::all_of(st.latHighHist.begin(),
                  st.latHighHist.end(),
                  [](bool b) { return b; });

    // Cooldown: after a migration, suppress both evaluation and action for a period
    const auto now = std::chrono::steady_clock::now();
    if (st.hasMigrated) {
        const auto elapsedMs =
          std::chrono::duration_cast<std::chrono::milliseconds>(
            now - st.lastMigration)
            .count();
        if (elapsedMs < cooldownMs) {
            SPDLOG_INFO("[PERF POLICY] app {} settling after migration "
                        "({}ms<{}ms); MIGRATE_SKIP",
                        appId,
                        elapsedMs,
                        cooldownMs);
            return std::make_shared<SchedulingDecision>(
              DO_NOT_MIGRATE_DECISION);
        }
    }

    if (st.pendingEval) {
        int& failCount = (st.lastDirection == Direction::ScaleOut)
                           ? st.failScaleOut
                           : st.failScaleIn;
        if (!sustained) {
            SPDLOG_INFO("[PERF POLICY] app {} RECOVERED after migration "
                        "(p50={:.0f}us cpu={:.1f}%); metric back below "
                        "threshold",
                        appId,
                        p50,
                        cpu);
            st.failScaleIn = 0;
            st.failScaleOut = 0;
        } else {
            const bool improved =
              p50 < st.p50AtLastMigration * (1.0 - improveFrac);
            if (improved) {
                SPDLOG_INFO("[PERF POLICY] app {} migration improved p50 "
                            "({:.0f}->{:.0f}us) but still unhealthy; adapting",
                            appId,
                            st.p50AtLastMigration,
                            p50);
                st.failScaleIn = 0;
                st.failScaleOut = 0;
            } else {
                failCount += 1;
                SPDLOG_INFO("[PERF POLICY] app {} {} did NOT improve p50 "
                            "({:.0f}->{:.0f}us); fail {}/{} for that direction",
                            appId,
                            dirName(st.lastDirection),
                            st.p50AtLastMigration,
                            p50,
                            failCount,
                            maxAttempts);
            }
        }
        st.pendingEval = false;
        if (st.failScaleIn >= maxAttempts && st.failScaleOut >= maxAttempts) {
            st.gaveUp = true;
            SPDLOG_WARN("[PERF POLICY] app {} GIVE_UP: both directions "
                        "exhausted ({} scale-in, {} scale-out non-improving); "
                        "slowdown is not placement-related",
                        appId,
                        st.failScaleIn,
                        st.failScaleOut);
            return std::make_shared<SchedulingDecision>(
              DO_NOT_MIGRATE_DECISION);
        }
    }

    if (!sustained) {
        SPDLOG_INFO("[PERF POLICY] app {} observing (p50={:.0f}us p99={:.0f}us "
                    "rps={:.0f} cpu={:.1f}% hosts={} highWindows={}/{}); "
                    "MIGRATE_SKIP",
                    appId,
                    p50,
                    p99,
                    rps,
                    cpu,
                    distinctHosts,
                    static_cast<int>(std::count(st.latHighHist.begin(),
                                                st.latHighHist.end(),
                                                true)),
                    sustainWindows);
        return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
    }

    const Direction direction =
      (cpu < cpuThreshPct) ? Direction::ScaleIn : Direction::ScaleOut;

    const int dirFails = (direction == Direction::ScaleOut) ? st.failScaleOut
                                                            : st.failScaleIn;
    if (dirFails >= maxAttempts) {
        SPDLOG_INFO("[PERF POLICY] app {} wants {} (cpu={:.1f}%) but that "
                    "direction already failed {}/{} times; MIGRATE_SKIP",
                    appId,
                    dirName(direction),
                    cpu,
                    dirFails,
                    maxAttempts);
        return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
    }

    auto effectiveCap = [&](const Host& host) -> int {
        int own = hostFreqCount.contains(getIp(host))
                    ? hostFreqCount.at(getIp(host))
                    : 0;
        return numSlotsAvailable(host) + own;
    };

    std::shared_ptr<SchedulingDecision> target = nullptr;
    std::string action;

    if (direction == Direction::ScaleIn) {
        // SCALE-IN: co-locate all services on a single host
        if (distinctHosts <= 1) {
            SPDLOG_INFO("[PERF POLICY] app {} scale-in wanted (p99={:.0f}us) "
                        "but already co-located on {} host; MIGRATE_SKIP",
                        appId,
                        p99,
                        distinctHosts);
            return std::make_shared<SchedulingDecision>(
              DO_NOT_MIGRATE_DECISION);
        }

        std::string bestHost;
        int bestFreq = -1;
        int bestCap = -1;
        for (const auto& [ip, host] : hostMap) {
            int cap = effectiveCap(host);
            if (cap < nFunctions) {
                continue;
            }
            int freq = hostFreqCount.contains(ip) ? hostFreqCount.at(ip) : 0;
            if (freq > bestFreq || (freq == bestFreq && cap > bestCap)) {
                bestFreq = freq;
                bestCap = cap;
                bestHost = ip;
            }
        }

        if (bestHost.empty()) {
            SPDLOG_INFO("[PERF POLICY] app {} scale-in wanted (p99={:.0f}us) "
                        "but no single host can hold {} services; MIGRATE_SKIP",
                        appId,
                        p99,
                        nFunctions);
            return std::make_shared<SchedulingDecision>(
              DO_NOT_MIGRATE_DECISION);
        }

        target =
          std::make_shared<SchedulingDecision>(appId, oldDecision->groupId);
        for (int i = 0; i < nFunctions; i++) {
            target->addMessage(bestHost, 0, 0, 0);
        }
        action = fmt::format("SCALE_IN -> co-locate {} services on {}",
                             nFunctions,
                             bestHost);
    } else {
        // SCALE-OUT: spread services across as many hosts as possible 
        if (distinctHosts >= nFunctions) {
            SPDLOG_INFO("[PERF POLICY] app {} scale-out wanted (cpu={:.1f}%) "
                        "but already maximally spread over {} hosts; "
                        "MIGRATE_SKIP",
                        appId,
                        cpu,
                        distinctHosts);
            return std::make_shared<SchedulingDecision>(
              DO_NOT_MIGRATE_DECISION);
        }

        std::vector<std::pair<std::string, int>> caps;
        for (const auto& [ip, host] : hostMap) {
            caps.emplace_back(ip, effectiveCap(host));
        }
        std::sort(caps.begin(), caps.end(), [](const auto& a, const auto& b) {
            if (a.second != b.second) {
                return a.second > b.second;
            }
            return a.first > b.first;
        });

        target =
          std::make_shared<SchedulingDecision>(appId, oldDecision->groupId);
        std::vector<std::string> orderedIps;
        std::map<std::string, int> capLeft;
        for (const auto& [ip, cap] : caps) {
            orderedIps.push_back(ip);
            capLeft[ip] = cap;
        }

        int placed = 0;
        while (placed < nFunctions) {
            bool anyPlaced = false;
            for (const auto& ip : orderedIps) {
                if (placed >= nFunctions) {
                    break;
                }
                if (capLeft.at(ip) > 0) {
                    target->addMessage(ip, 0, 0, 0);
                    capLeft.at(ip) -= 1;
                    placed++;
                    anyPlaced = true;
                }
            }
            if (!anyPlaced) {
                break;
            }
        }

        if (placed < nFunctions) {
            SPDLOG_INFO("[PERF POLICY] app {} scale-out not possible (only {} "
                        "slots for {} services); MIGRATE_SKIP",
                        appId,
                        placed,
                        nFunctions);
            return std::make_shared<SchedulingDecision>(
              DO_NOT_MIGRATE_DECISION);
        }

        int newDistinct = static_cast<int>(getHostFreqCount(target).size());
        if (newDistinct <= distinctHosts) {
            SPDLOG_INFO("[PERF POLICY] app {} scale-out would not increase "
                        "spread ({} hosts); MIGRATE_SKIP",
                        appId,
                        distinctHosts);
            return std::make_shared<SchedulingDecision>(
              DO_NOT_MIGRATE_DECISION);
        }
        action = fmt::format("SCALE_OUT -> spread {} services over {} hosts",
                             nFunctions,
                             newDistinct);
    }

    auto finalDecision = minimiseNumOfMigrations(target, oldDecision);

    // Only migrate if the placement actually changes.
    if (finalDecision->hosts == oldDecision->hosts) {
        SPDLOG_INFO("[PERF POLICY] app {} target matches current placement; "
                    "MIGRATE_SKIP",
                    appId);
        return std::make_shared<SchedulingDecision>(DO_NOT_MIGRATE_DECISION);
    }

    st.lastMigration = now;
    st.hasMigrated = true;
    st.pendingEval = true;
    st.p50AtLastMigration = p50;
    st.lastDirection = direction;

    SPDLOG_INFO("[PERF POLICY] app {} {} (p50={:.0f}us p99={:.0f}us rps={:.0f} "
                "cpu={:.1f}%)",
                appId,
                action,
                p50,
                p99,
                rps,
                cpu);

    return finalDecision;
}
}
