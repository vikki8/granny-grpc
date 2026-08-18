#include <faabric/grpc/GrpcWorld.h>

#include <faabric/grpc/GrpcService.h>
#include <faabric/grpc/GrpcWorldRegistry.h>

#include <faabric/planner/PlannerClient.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/perf_monitor.h>
#include <grpcpp/grpcpp.h>
#include <faasm_grpc.grpc.pb.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#if defined(__linux__)
#include <sys/sysinfo.h>
#include <unistd.h>
#endif

namespace faabric::grpc {

// Host utilisation sampler
namespace {

inline int64_t steadyNowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

inline double effectiveCpuCores()
{
    const char* env = std::getenv("PERF_CPU_CORES");
    if (env != nullptr) {
        char* end = nullptr;
        const double v = std::strtod(env, &end);
        if (end != env && v > 0.0) {
            return v;
        }
    }
    return static_cast<double>(std::max(1L, sysconf(_SC_NPROCESSORS_ONLN)));
}

#if defined(__linux__)
// Total CPU time consumed by this process (all threads), in nanoseconds.
inline int64_t processCpuNs()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts) != 0) {
        return 0;
    }
    return static_cast<int64_t>(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
}

inline long processRssBytes()
{
    FILE* fp = std::fopen("/proc/self/statm", "r");
    if (fp == nullptr) {
        return -1;
    }
    long sizePages = 0;
    long rssPages = 0;
    const int scanned = std::fscanf(fp, "%ld %ld", &sizePages, &rssPages);
    std::fclose(fp);
    if (scanned != 2) {
        return -1;
    }
    return rssPages * sysconf(_SC_PAGESIZE);
}

class HostUtilSampler
{
  public:
    HostUtilSampler()
      : worker([this] { run(); })
    {}

    ~HostUtilSampler()
    {
        stop.store(true, std::memory_order_release);
        if (worker.joinable()) {
            worker.join();
        }
    }

  private:
    void run()
    {
        const double cores = effectiveCpuCores();

        struct sysinfo si;
        const long long memTotalBytes =
          (sysinfo(&si) == 0)
            ? static_cast<long long>(si.totalram) * si.mem_unit
            : 0;

        int64_t lastCpuNs = processCpuNs();
        int64_t lastTsNs = steadyNowNs();

        while (!stop.load(std::memory_order_acquire)) {
            // Sleep in short slices so process teardown is not held up by a
            // full sampling interval.
            int sleptMs = 0;
            while (sleptMs < GRPC_HOST_UTIL_SAMPLE_MS &&
                   !stop.load(std::memory_order_acquire)) {
                const int sliceMs =
                  std::min(50, GRPC_HOST_UTIL_SAMPLE_MS - sleptMs);
                std::this_thread::sleep_for(
                  std::chrono::milliseconds(sliceMs));
                sleptMs += sliceMs;
            }
            if (stop.load(std::memory_order_acquire)) {
                break;
            }

            const int64_t nowCpuNs = processCpuNs();
            const int64_t nowTsNs = steadyNowNs();
            const int64_t wallDeltaNs = nowTsNs - lastTsNs;
            if (wallDeltaNs <= 0) {
                continue;
            }

            const double cpuPct =
              100.0 * static_cast<double>(nowCpuNs - lastCpuNs) /
              (static_cast<double>(wallDeltaNs) * cores);

            const long rssBytes = processRssBytes();
            const double memPct =
              (rssBytes > 0 && memTotalBytes > 0)
                ? 100.0 * static_cast<double>(rssBytes) /
                    static_cast<double>(memTotalBytes)
                : 0.0;

            lastCpuNs = nowCpuNs;
            lastTsNs = nowTsNs;

            // Publish the latest CPU% to the process-wide monitor so the
            // perf-adaptive scheduler can consult it at migration points.
            faabric::util::PerfMonitor::instance().setCpuPct(cpuPct);

            SPDLOG_INFO(
              "[HOST UTIL] ts_ns={} cpu_pct={:.2f} mem_pct={:.3f} "
              "rss_bytes={} cores={:g}",
              nowTsNs,
              cpuPct,
              memPct,
              rssBytes,
              cores);
        }
    }

    std::atomic<bool> stop{ false };
    std::thread worker; 
};
#endif 

} 

void startHostUtilSampler()
{
#if defined(__linux__)
    if (!GRPC_HOST_UTIL_ENABLED) {
        return;
    }
    static HostUtilSampler sampler;
    (void)sampler;
#endif
}

// Lifecycle & utilities
GrpcWorld::GrpcWorld(int32_t appIdIn, int32_t serviceIdIn)
  : appId(appIdIn)
  , serviceId(serviceIdIn)
  , thisHost(faabric::util::getSystemConfig().endpointHost)
{}

GrpcWorld::~GrpcWorld()
{
    try {
        destroy();
    } catch (const std::exception& ex) {
        SPDLOG_WARN("[GRPC TEARDOWN] app {} serviceId {} destroy() threw: {}",
                    appId, serviceId, ex.what());
    } catch (...) {
        SPDLOG_WARN("[GRPC TEARDOWN] app {} serviceId {} destroy() threw unknown",
                    appId, serviceId);
    }
}

void GrpcWorld::create()
{
    std::scoped_lock lock(worldMx);

    if (server != nullptr) {
        return;
    }

    // begin per-process CPU/memory utilisation sampling as soon as gRPC activity exists.
    startHostUtilSampler();

    // Preserve a pre-restored UUID (commit Phase) so the world remains the
    // same logical GrpcWorld across migration.
    if (dedupeUUID.empty()) {
        dedupeUUID =
          boost::uuids::to_string(boost::uuids::random_generator()());
    }

    service = std::make_unique<GrpcService>(*this);

    ::grpc::ServerBuilder builder;
    int selectedPort = 0;
    builder.AddListeningPort("0.0.0.0:0",
                             ::grpc::InsecureServerCredentials(),
                             &selectedPort);
    builder.RegisterService(service.get());

    server = builder.BuildAndStart();
    if (server == nullptr || selectedPort <= 0) {
        throw std::runtime_error("Failed to start GrpcWorld server");
    }

    listenPort = selectedPort;
    faabric::planner::getPlannerClient().setGrpcEndpoint(
      appId, serviceId, thisHost, listenPort);

    // Optimisation 1: the handler is now live, so a co-located service may
    // deliver to us in-process.
    serviceReady.store(true);

    SPDLOG_INFO("Started GrpcWorld app {} serviceId {} on {}:{}",
                appId,
                serviceId,
                thisHost,
                listenPort);
}

bool GrpcWorld::destroy()
{
    std::scoped_lock lock(worldMx);

    if (server == nullptr) {
        return false;
    }

    // Optimisation 1: stop accepting in-process deliveries before tearing the
    // handler down.
    serviceReady.store(false);

    const int32_t destroyPort = listenPort;
    SPDLOG_INFO("[GRPC TEARDOWN] app {} serviceId {} stopping server on {}:{}",
                appId,
                serviceId,
                thisHost,
                destroyPort);

    // Cap Shutdown at 2s so process-exit teardown never hangs on a stuck
    // in-flight RPC. Any outstanding calls are cancelled at the deadline.
    const auto shutdownDeadline =
      std::chrono::system_clock::now() + std::chrono::seconds(2);
    server->Shutdown(shutdownDeadline);
    SPDLOG_INFO("[GRPC TEARDOWN] app {} serviceId {} server->Shutdown() returned",
                appId,
                serviceId);
    server->Wait();
    SPDLOG_INFO("[GRPC TEARDOWN] app {} serviceId {} server->Wait() returned",
                appId,
                serviceId);
    server.reset();
    service.reset();

    std::size_t channelsDropped = 0;
    {
        std::scoped_lock cacheLock(channelCacheMx);
        channelsDropped = channelCache.size();
        channelCache.clear();
    }

    std::size_t pendingDropped = 0;
    {
        std::scoped_lock pendingLock(pendingResponsesMx);
        pendingDropped = pendingResponses.size();
        pendingResponses.clear();
    }

    std::size_t outboxDropped = 0;
    {
        std::scoped_lock obLock(outboxMx);
        outboxDropped = outbox.size();
        outbox.clear();
    }
    outboxNotFullCv.notify_all();

    pendingRequests.reset();
    pendingRequestsSize.store(0, std::memory_order_relaxed);
    listenPort = 0;

    std::size_t cursorsDropped = 0;
    std::size_t queuesDropped = 0;
    std::size_t preOpenDropped = 0;
    {
        std::scoped_lock streamLock(streamsMx);
        cursorsDropped = streamCursors.size();
        queuesDropped = streamInboundQueues.size();
        preOpenDropped = pendingPreOpenItems.size();
        streamCursors.clear();
        streamInboundQueues.clear();
        pendingPreOpenItems.clear();
    }
    pendingStreamOpens.reset();

    SPDLOG_INFO("[GRPC TEARDOWN] app {} serviceId {} stopped "
                "channels_dropped={} pending_unary_dropped={} "
                "outbox_dropped={} stream_cursors_dropped={} "
                "stream_queues_dropped={} preopen_dropped={} "
                "outbox_backpressure_waits={} outbox_backpressure_timeouts={}",
                appId,
                serviceId,
                channelsDropped,
                pendingDropped,
                outboxDropped,
                cursorsDropped,
                queuesDropped,
                preOpenDropped,
                outboxBackpressureWaits.load(std::memory_order_relaxed),
                outboxBackpressureTimeouts.load(std::memory_order_relaxed));

    {
        std::vector<std::thread> toJoin;
        {
            std::scoped_lock dlk(deferredMx_);
            deferredStop_.store(true);
            toJoin.swap(deferredThreads_);
        }
        deferredCv_.notify_all();
        for (auto& t : toJoin) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    return true;
}
std::shared_ptr<::grpc::Channel> GrpcWorld::getOrCreateChannel(int32_t destServiceId)
{
    {
        std::scoped_lock lock(channelCacheMx);
        auto cached = channelCache.find(destServiceId);
        if (cached != channelCache.end()) {
            return cached->second;
        }
    }

    auto& plannerCli = faabric::planner::getPlannerClient();
    std::string endpoint;
    constexpr int maxAttempts = 150;
    constexpr int connectTimeoutMs = 3000;
    int missedEndpointAttempts = 0;
    int failedConnectAttempts = 0;

    const auto channelDeadline =
      std::chrono::steady_clock::now() +
      std::chrono::milliseconds(CHANNEL_TOTAL_DEADLINE_MS);

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
        if (std::chrono::steady_clock::now() >= channelDeadline) {
            SPDLOG_WARN(
              "[GRPC CHANNEL] app {} serviceId {} -> serviceId {} DEADLINE (total={}ms) "
              "reached after {} attempts (endpoint_misses={} "
              "connect_failures={})",
              appId, serviceId, destServiceId,
              CHANNEL_TOTAL_DEADLINE_MS, attempt,
              missedEndpointAttempts, failedConnectAttempts);
            break;
        }
        endpoint = plannerCli.getGrpcEndpoint(appId, destServiceId);
        if (endpoint.empty()) {
            missedEndpointAttempts++;
            if (missedEndpointAttempts == 1 || missedEndpointAttempts % 20 == 0) {
                SPDLOG_INFO(
                  "[GRPC CHANNEL] app {} serviceId {} still waiting for planner "
                  "endpoint for serviceId {} (attempt {}/{}, misses={})",
                  appId, serviceId, destServiceId,
                  attempt + 1, maxAttempts,
                  missedEndpointAttempts);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        auto channel = ::grpc::CreateChannel(endpoint,
                                             ::grpc::InsecureChannelCredentials());
        bool connected = channel->WaitForConnected(
          std::chrono::system_clock::now() +
          std::chrono::milliseconds(connectTimeoutMs));
        if (!connected) {
            failedConnectAttempts++;
            if (failedConnectAttempts == 1) {
                SPDLOG_INFO(
                  "[GRPC CHANNEL] app {} serviceId {} -> serviceId {} connect attempt "
                  "{}/{} to {} timed out after {}ms; will retry",
                  appId, serviceId, destServiceId,
                  attempt + 1, maxAttempts, endpoint, connectTimeoutMs);
            } else {
                SPDLOG_DEBUG(
                  "[GRPC CHANNEL] app {} serviceId {} -> serviceId {} connect attempt "
                  "{}/{} to {} timed out (cumulative_failures={})",
                  appId, serviceId, destServiceId,
                  attempt + 1, maxAttempts, endpoint, failedConnectAttempts);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        std::scoped_lock lock(channelCacheMx);
        channelCache[destServiceId] = channel;
        SPDLOG_INFO("[GRPC CHANNEL] app {} serviceId {} connected to serviceId {} at {} "
                    "(attempt {}/{} endpoint_misses={} connect_failures={})",
                    appId,
                    serviceId,
                    destServiceId,
                    endpoint,
                    attempt + 1,
                    maxAttempts,
                    missedEndpointAttempts,
                    failedConnectAttempts);
        return channel;
    }

    SPDLOG_WARN("[GRPC CHANNEL] app {} serviceId {} FAILED to resolve serviceId {} "
                "(max_attempts={} total_deadline_ms={} "
                "endpoint_misses={} connect_failures={})",
                appId,
                serviceId,
                destServiceId,
                maxAttempts,
                CHANNEL_TOTAL_DEADLINE_MS,
                missedEndpointAttempts,
                failedConnectAttempts);
    throw std::runtime_error(
      fmt::format("Unable to resolve gRPC endpoint for app {} serviceId {} "
                  "(deadline={}ms misses={} failures={})",
                  appId,
                  destServiceId,
                  CHANNEL_TOTAL_DEADLINE_MS,
                  missedEndpointAttempts,
                  failedConnectAttempts));
}

int32_t GrpcWorld::allocateCallId()
{
    return callIdCounter.fetch_add(1) + 1;
}

int32_t GrpcWorld::allocateStreamId()
{
    int32_t local = streamIdCounter.fetch_add(1) + 1;
    return ((serviceId & 0xFF) << 24) | (local & 0x00FFFFFF);
}

// Migration orchestration
void GrpcWorld::preparePhase(int32_t newEpoch)
{
    const auto metricPrepareStart = std::chrono::steady_clock::now();
    size_t inFlight = 0;
    {
        std::scoped_lock lock(outboxMx);
        inFlight = outbox.size();
    }
    SPDLOG_INFO("[GRPC MIGRATE] PREPARE: app {} serviceId {} draining outbox "
                "({} in-flight) epoch {} -> {}",
                appId,
                serviceId,
                inFlight,
                migrationEpoch,
                newEpoch);

    migratingOut.store(true);
    migrationEpoch = std::max(migrationEpoch, newEpoch);

    // Drain outbox: wait for at most 5s for any in-flight send started before
    // prepare to either complete and ack, or we give up (replayed on dest).
    constexpr int drainTimeoutMs = 5000;
    constexpr int pollMs = 25;
    int waited = 0;
    while (waited < drainTimeoutMs) {
        {
            std::scoped_lock lock(outboxMx);
            if (outbox.empty()) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(pollMs));
        waited += pollMs;
    }

    size_t abortedStreamItems = 0;
    {
        std::vector<int32_t> stalePending;
        {
            std::scoped_lock streamLock(streamsMx);
            for (auto& [sid, queuePtr] : streamInboundQueues) {
                long pendingCount = queuePtr->size();
                for (long i = 0; i < pendingCount; i++) {
                    StreamInboundItem item;
                    queuePtr->dequeueIfPresent(&item);
                    if (item.callId != kNoAckCallId) {
                        stalePending.push_back(item.callId);
                    }
                }
            }
        }
        for (int32_t callId : stalePending) {
            std::shared_ptr<std::promise<UnaryResponse>> p;
            {
                std::scoped_lock plock(pendingResponsesMx);
                auto it = pendingResponses.find(callId);
                if (it != pendingResponses.end()) {
                    p = it->second;
                    pendingResponses.erase(it);
                }
            }
            if (p) {
                try {
                    p->set_exception(std::make_exception_ptr(std::runtime_error(
                      "grpc serviceId migrating; aborted pending stream item")));
                    abortedStreamItems++;
                } catch (...) {
                }
            }
        }
    }

    size_t abortedUnaryCalls = 0;
    size_t drainedQueuedRequests = 0;
    {
        std::map<int32_t, std::shared_ptr<std::promise<UnaryResponse>>> snapshot;
        {
            std::scoped_lock plock(pendingResponsesMx);
            snapshot.swap(pendingResponses);
        }
        for (auto& [cid, p] : snapshot) {
            if (!p) {
                continue;
            }
            try {
                p->set_exception(std::make_exception_ptr(std::runtime_error(
                  "grpc serviceId migrating; aborted pending unary call")));
                abortedUnaryCalls++;
            } catch (...) {
                // Promise already satisfied — ignore.
            }
        }
        long queued = pendingRequests.size();
        for (long i = 0; i < queued; i++) {
            UnaryRequest discard;
            if (pendingRequests.tryDequeueIfPresent(&discard)) {
                drainedQueuedRequests++;
                pendingRequestsSize.fetch_sub(1, std::memory_order_relaxed);
            }
        }
    }

    SPDLOG_INFO("[GRPC MIGRATE] PREPARE swept: app {} serviceId {} "
                "aborted_stream_items={} aborted_unary_calls={} "
                "drained_queued_requests={}",
                appId,
                serviceId,
                abortedStreamItems,
                abortedUnaryCalls,
                drainedQueuedRequests);

    const auto metricPrepareEnd = std::chrono::steady_clock::now();
    SPDLOG_INFO(
      "[GRPC METRIC] PREPARE_MS={} app {} serviceId {} in_flight_start={}",
      std::chrono::duration_cast<std::chrono::milliseconds>(metricPrepareEnd -
                                                             metricPrepareStart)
        .count(),
      appId,
      serviceId,
      inFlight);

    // We intentionally DO NOT shut down the local server here. Keeping it
    // alive lets us hand off in-flight inbound RPCs by forwarding to the
    // destination during the FORWARDING_GRACE_WINDOW_MS window. The caller
    // is responsible for scheduling destroy() after the grace window.
}

faabric::GrpcMigrationMetadata GrpcWorld::transferPhase()
{
    const auto metricTransferStart = std::chrono::steady_clock::now();
    faabric::GrpcMigrationMetadata meta;
    meta.set_appid(appId);
    meta.set_serviceid(serviceId);
    meta.set_newepoch(migrationEpoch);
    meta.set_dedupeuuid(dedupeUUID);
    meta.set_lastallocatedcallid(callIdCounter.load());

    {
        std::scoped_lock lock(outboxMx);
        for (const auto& [callId, entry] : outbox) {
            auto* protoEntry = meta.add_outbox();
            protoEntry->set_callid(callId);
            protoEntry->set_destserviceid(entry.destServiceId);
            protoEntry->set_method(entry.method);
            if (!entry.requestBytes.empty()) {
                protoEntry->set_requestbytes(entry.requestBytes.data(),
                                             entry.requestBytes.size());
            }
            protoEntry->set_migrationepoch(entry.envelope.migrationEpoch);
            protoEntry->set_streamid(entry.streamId);
            protoEntry->set_seqnum(entry.seqNum);
        }
    }

    {
        std::scoped_lock streamLock(streamsMx);
        for (const auto& [sid, cursor] : streamCursors) {
            auto* sc = meta.add_streamcursors();
            sc->set_streamid(cursor.streamId);
            sc->set_peerserviceid(cursor.peerServiceId);
            sc->set_isclient(cursor.isClient);
            sc->set_sendseqnum(cursor.sendSeqNum);
            sc->set_lastreceivedseq(cursor.lastReceivedSeq);
            sc->set_halfclosedlocal(cursor.halfClosedLocal);
            sc->set_halfclosedremote(cursor.halfClosedRemote);
        }
    }

    if (service) {
        service->snapshotMigrationState(meta);
    }

    // Measured snapshot footprint
    {
        std::size_t outboxBytes = 0;
        for (const auto& e : meta.outbox()) {
            outboxBytes += e.ByteSizeLong();
        }
        std::size_t cursorBytes = 0;
        for (const auto& e : meta.streamcursors()) {
            cursorBytes += e.ByteSizeLong();
        }
        std::size_t dedupeBytes = 0;
        std::size_t dedupePayloadBytes = 0;
        for (const auto& e : meta.dedupecache()) {
            dedupeBytes += e.ByteSizeLong();
            dedupePayloadBytes += e.payload().size();
        }
        std::size_t epochBytes = 0;
        for (const auto& e : meta.senderepochs()) {
            epochBytes += e.ByteSizeLong();
        }
        SPDLOG_INFO(
          "[GRPC SNAPSHOT] app {} serviceId {} total_bytes={} outbox_bytes={} "
          "cursor_bytes={} dedupe_bytes={} epoch_bytes={} "
          "dedupe_payload_bytes={} outbox_n={} cursor_n={} dedupe_n={} "
          "epoch_n={}",
          appId,
          serviceId,
          meta.ByteSizeLong(),
          outboxBytes,
          cursorBytes,
          dedupeBytes,
          epochBytes,
          dedupePayloadBytes,
          meta.outbox_size(),
          meta.streamcursors_size(),
          meta.dedupecache_size(),
          meta.senderepochs_size());
    }

    SPDLOG_INFO("[GRPC MIGRATE] TRANSFER: app {} serviceId {} dedupeUUID={} "
                "lastallocatedcallid={} outbox={} streams={} dedupe={} "
                "senderEpochs={} epoch={}",
                appId,
                serviceId,
                dedupeUUID,
                meta.lastallocatedcallid(),
                meta.outbox_size(),
                meta.streamcursors_size(),
                meta.dedupecache_size(),
                meta.senderepochs_size(),
                migrationEpoch);
    const auto metricTransferEnd = std::chrono::steady_clock::now();
    SPDLOG_INFO(
      "[GRPC METRIC] TRANSFER_MS={} app {} serviceId {} outbox={} streams={}",
      std::chrono::duration_cast<std::chrono::milliseconds>(metricTransferEnd -
                                                           metricTransferStart)
        .count(),
      appId,
      serviceId,
      meta.outbox_size(),
      meta.streamcursors_size());
    return meta;
}

void GrpcWorld::commitPhase(const faabric::GrpcMigrationMetadata& meta)
{
    const auto metricCommitStart = std::chrono::steady_clock::now();
    SPDLOG_INFO("[GRPC MIGRATE] COMMIT: app {} serviceId {} restoring on {} "
                "epoch={} dedupeUUID={} lastallocatedcallid={} outbox={} "
                "streams={} dedupe={} senderEpochs={}",
                meta.appid(),
                meta.serviceid(),
                thisHost,
                meta.newepoch(),
                meta.dedupeuuid(),
                meta.lastallocatedcallid(),
                meta.outbox_size(),
                meta.streamcursors_size(),
                meta.dedupecache_size(),
                meta.senderepochs_size());

    // Restore identity first so create() registers a world that is a
    // continuation of the pre-migration one (same dedupe UUID).
    dedupeUUID = meta.dedupeuuid();
    migrationEpoch = meta.newepoch();
    callIdCounter.store(std::max(callIdCounter.load(),
                                 static_cast<int32_t>(meta.lastallocatedcallid())));

    {
        std::scoped_lock lock(outboxMx);
        outbox.clear();
        for (const auto& entry : meta.outbox()) {
            OutboxEntry restored;
            restored.destServiceId = entry.destserviceid();
            restored.method = entry.method();
            const std::string& bytes = entry.requestbytes();
            restored.requestBytes.assign(bytes.begin(), bytes.end());
            restored.envelope.dedupeUUID = dedupeUUID;
            restored.envelope.migrationEpoch = entry.migrationepoch();
            restored.envelope.callId = entry.callid();
            restored.streamId = entry.streamid();
            restored.seqNum = entry.seqnum();
            outbox[entry.callid()] = std::move(restored);
        }
    }

    {
        std::scoped_lock streamLock(streamsMx);
        streamCursors.clear();
        streamInboundQueues.clear();
        pendingPreOpenItems.clear();
        for (const auto& sc : meta.streamcursors()) {
            StreamCursor restored;
            restored.streamId = sc.streamid();
            restored.peerServiceId = sc.peerserviceid();
            restored.isClient = sc.isclient();
            restored.sendSeqNum = sc.sendseqnum();
            restored.lastReceivedSeq = sc.lastreceivedseq();
            restored.halfClosedLocal = sc.halfclosedlocal();
            restored.halfClosedRemote = sc.halfclosedremote();
            streamCursors[restored.streamId] = restored;
            streamInboundQueues[restored.streamId] = std::make_shared<
              faabric::util::Queue<StreamInboundItem>>();
        }
    }

    // Start a fresh server on this host and register the new endpoint with the
    // planner, overwriting the old one so callers re-resolve to us.
    create();
    migratingOut.store(false);

    if (service) {
        service->restoreMigrationState(meta);
    }

    SPDLOG_INFO("[GRPC MIGRATE] COMMIT complete: app {} serviceId {} now serving "
                "on {}:{} (planner updated)",
                appId,
                serviceId,
                thisHost,
                listenPort);
    const auto metricCommitServeEnd = std::chrono::steady_clock::now();

    {
        std::scoped_lock cacheLock(channelCacheMx);
        channelCache.clear();
    }

    {
        std::map<int32_t, StreamCursor> snapshot;
        {
            std::scoped_lock streamLock(streamsMx);
            snapshot = streamCursors;
        }
        if (snapshot.empty()) {
            SPDLOG_INFO(
              "[GRPC RETRANSMIT] app {} serviceId {} no active streams; "
              "nothing to request",
              appId, serviceId);
        }
  
        std::vector<int32_t> deferredStreamIds;

        for (const auto& [sid, cursor] : snapshot) {
            if (cursor.peerServiceId < 0) {
                SPDLOG_WARN(
                  "[GRPC RETRANSMIT] app {} serviceId {} stream {} skipped: "
                  "peer unknown (peerServiceId=-1, lastReceivedSeq={}); "
                  "scheduling deferred retry",
                  appId, serviceId, cursor.streamId, cursor.lastReceivedSeq);
                deferredStreamIds.push_back(cursor.streamId);
                continue;
            }
            if (cursor.isClient) {
                // Only the receiving side requests retransmits; the client
                // side survives via the outbox replay + unary retry path.
                SPDLOG_INFO(
                  "[GRPC RETRANSMIT] app {} serviceId {} stream {} to peer serviceId "
                  "{} skipped: migrating side is CLIENT "
                  "(sendSeqNum={}); outbox replay + client "
                  "retry path handles gaps",
                  appId, serviceId, cursor.streamId, cursor.peerServiceId,
                  cursor.sendSeqNum);
                continue;
            }
            const int64_t fromSeq = cursor.lastReceivedSeq + 1;
            SPDLOG_INFO(
              "[GRPC RETRANSMIT] app {} serviceId {} stream {} peer serviceId {} "
              "scheduling DEFERRED replay from seq {} "
              "(avoids commitPhase deadlock)",
              appId, serviceId, cursor.streamId, cursor.peerServiceId, fromSeq);
            deferredStreamIds.push_back(cursor.streamId);
        }

        if (!deferredStreamIds.empty()) {
            constexpr int RETRANSMIT_RETRY_DELAY_MS = 500;
            SPDLOG_INFO(
              "[GRPC RETRANSMIT] app {} serviceId {} scheduling deferred retry "
              "for {} stream(s) in {}ms",
              appId, serviceId, deferredStreamIds.size(), RETRANSMIT_RETRY_DELAY_MS);

            {
                std::scoped_lock dlk(deferredMx_);
                if (deferredStop_.load()) {
                    SPDLOG_INFO(
                      "[GRPC RETRANSMIT] app {} serviceId {} destroy() already started; "
                      "skipping deferred retry spawn",
                      appId, serviceId);
                } else {
                    deferredThreads_.emplace_back(
                      [this,
                       streamIds = std::move(deferredStreamIds),
                       delayMs = RETRANSMIT_RETRY_DELAY_MS]() {
                          {
                              std::unique_lock<std::mutex> lk(deferredMx_);
                              deferredCv_.wait_for(
                                lk,
                                std::chrono::milliseconds(delayMs),
                                [this] { return deferredStop_.load(); });
                          }
                          if (deferredStop_.load()) {
                              return;
                          }

                          std::map<int32_t, StreamCursor> snap;
                          {
                              std::scoped_lock lk(streamsMx);
                              snap = streamCursors;
                          }
                          for (int32_t sid : streamIds) {
                              auto it = snap.find(sid);
                              if (it == snap.end()) {
                                  SPDLOG_WARN(
                                    "[GRPC RETRANSMIT] deferred retry: app {} "
                                    "serviceId {} stream {} no longer exists; skipping",
                                    appId, serviceId, sid);
                                  continue;
                              }
                              const StreamCursor& c = it->second;
                              if (c.peerServiceId < 0) {
                                  SPDLOG_WARN(
                                    "[GRPC RETRANSMIT] deferred retry: app {} "
                                    "serviceId {} stream {} still has unknown peer "
                                    "after {}ms; giving up - manual recovery needed",
                                    appId, serviceId, sid, delayMs);
                                  continue;
                              }
                              const int64_t fromSeq = c.lastReceivedSeq + 1;
                              SPDLOG_INFO(
                                "[GRPC RETRANSMIT] deferred retry: app {} serviceId {} "
                                "stream {} peer serviceId {} requesting seq {}+",
                                appId, serviceId, sid, c.peerServiceId, fromSeq);
                              doRetransmitRequest(sid, c.peerServiceId, fromSeq);
                          }
                      });
                }
            }
        }
    }
    const auto metricCommitEnd = std::chrono::steady_clock::now();
    SPDLOG_INFO(
      "[GRPC METRIC] COMMIT_SERVE_MS={} COMMIT_TAIL_MS={} COMMIT_TOTAL_MS={} "
      "app {} serviceId {}",
      std::chrono::duration_cast<std::chrono::milliseconds>(
        metricCommitServeEnd - metricCommitStart)
        .count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(metricCommitEnd -
                                                           metricCommitServeEnd)
        .count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(metricCommitEnd -
                                                           metricCommitStart)
        .count(),
      appId,
      serviceId);
}

// Unary communication
void GrpcWorld::outboxAdd(int32_t callId, OutboxEntry entry)
{
    std::unique_lock<std::mutex> lock(outboxMx);
    // sender outbox backpressure
    if (outbox.size() >= OUTBOX_MAX_ENTRIES) {
        outboxBackpressureWaits.fetch_add(1, std::memory_order_relaxed);
        SPDLOG_WARN(
          "[GRPC OUTBOX] app {} serviceId {} outbox FULL (size={} cap={}); "
          "sender blocking up to {}ms for capacity (callId {})",
          appId,
          serviceId,
          outbox.size(),
          OUTBOX_MAX_ENTRIES,
          OUTBOX_ADD_BLOCK_TIMEOUT_MS,
          callId);
        const bool gotSpace = outboxNotFullCv.wait_for(
          lock,
          std::chrono::milliseconds(OUTBOX_ADD_BLOCK_TIMEOUT_MS),
          [this]() { return outbox.size() < OUTBOX_MAX_ENTRIES; });
        if (!gotSpace) {
            outboxBackpressureTimeouts.fetch_add(1, std::memory_order_relaxed);
            throw std::runtime_error(fmt::format(
              "gRPC outbox full for app {} serviceId {}: "
              "cap={} size={} (timed out after {}ms)",
              appId, serviceId, OUTBOX_MAX_ENTRIES, outbox.size(),
              OUTBOX_ADD_BLOCK_TIMEOUT_MS));
        }
    }
    outbox[callId] = std::move(entry);
}

void GrpcWorld::outboxAck(int32_t callId)
{
    std::scoped_lock lock(outboxMx);
    if (outbox.erase(callId) > 0) {
        outboxNotFullCv.notify_one();
    }
}

void GrpcWorld::buildRequest(faabric::faasmgrpc::GrpcRequest& request,
                             int32_t destServiceId,
                             int32_t callId,
                             const std::string& method,
                             int32_t streamId,
                             int64_t seqNum,
                             const std::vector<uint8_t>& payload) const
{
    request.set_appid(appId);
    request.set_callid(callId);
    request.set_sourceserviceid(serviceId);
    request.set_destserviceid(destServiceId);
    request.set_method(method);
    request.set_streamid(streamId);
    request.set_seqnum(seqNum);
    if (!payload.empty()) {
        request.set_payload(payload.data(), payload.size());
    } else {
        request.clear_payload();
    }
}

std::vector<uint8_t> GrpcWorld::callUnary(int32_t destServiceId,
                                          const std::string& method,
                                          const std::vector<uint8_t>& requestBytes,
                                          int32_t streamId,
                                          int64_t seqNum,
                                          int timeoutMs)
{
    // non-bidi methods must not carry stream routing fields. Catching
    // this at the sender avoids confusing dedupe/outbox mismatches on the peer.
    const bool isBidi = isBidiMethod(method);
    if (!isBidi && (streamId != 0 || seqNum != 0)) {
        throw std::runtime_error(fmt::format(
          "callUnary: non-bidi method '{}' called with streamId={} seqNum={} "
          "(both must be 0 for unary calls)",
          method, streamId, seqNum));
    }

    const auto rtt_t0 = std::chrono::steady_clock::now();
    const auto rtt_t0_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
        rtt_t0.time_since_epoch())
      .count();

    const int32_t callId = allocateCallId();
    faasgrpc::GrpcEnvelope envelope;
    envelope.dedupeUUID = dedupeUUID;
    envelope.migrationEpoch = migrationEpoch;
    envelope.callId = callId;
    envelope.forwardHops = 0;

    // Track the in-flight call in the outbox so that if this serviceId were to
    // migrate, the destination can replay it.
    outboxAdd(callId,
              OutboxEntry{ destServiceId,
                           method,
                           requestBytes,
                           envelope,
                           streamId,
                           seqNum });

    const auto buildT0 = std::chrono::steady_clock::now();
    faabric::faasmgrpc::GrpcRequest request;
    buildRequest(
      request, destServiceId, callId, method, streamId, seqNum, requestBytes);
    const int64_t buildNs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - buildT0)
        .count();


    constexpr int maxGenuineAttempts = 5; 
    constexpr int backoffBaseMs = 50;
    constexpr int maxBackoffMs = 500;
    constexpr int redirectDelayBaseMs = 5;   
    constexpr int redirectDelayMaxMs = 100;  
    constexpr int maxRedirectWaitMs = 10000; 
    constexpr int maxRedirectAttempts = 400; 

    auto& registry = getGrpcWorldRegistry();
    const char* kind = bidiKindTag(method);

    ::grpc::Status status;
    faabric::faasmgrpc::GrpcResponse response;
    std::shared_ptr<::grpc::Channel> channel; 
    int genuineAttempts = 0;
    int redirectAttempts = 0;
    int redirectWaitedMs = 0;
    int retriesUsed = 0;
    bool succeededViaFastPath = false;
    int64_t rpcNs = 0;

    while (true) {
        // Optimisation 1: per-attempt fast-path eligibility. A co-located,
        // ready, non-migrating peer can be served in-process.
        std::shared_ptr<GrpcWorld> localDest;
        if (GRPC_FAST_PATH_ENABLED && destServiceId != serviceId) {
            localDest = registry.tryGetWorld(appId, destServiceId);
            if (localDest && !localDest->localDeliveryReady()) {
                localDest.reset();
            }
        }
        const bool attemptViaFastPath = localDest != nullptr;

        response.Clear();
        const auto rpcT0 = std::chrono::steady_clock::now();
        if (attemptViaFastPath) {
            status = localDest->handleInboundLocally(request, envelope, &response);
        } else {
            if (!channel) {
                channel = getOrCreateChannel(destServiceId);
            }
            auto stub = faabric::faasmgrpc::FaasmGrpc::NewStub(channel);
            ::grpc::ClientContext context;
            faasgrpc::addEnvelopeToClientContext(context, envelope);
            // Cap each attempt at the configured timeout so a server blocked
            // on WASM (or mid-migration) cannot stall the client indefinitely.
            if (timeoutMs > 0) {
                context.set_deadline(
                  std::chrono::system_clock::now() +
                  std::chrono::milliseconds(timeoutMs));
            }
            status = stub->CallUnary(&context, request, &response);
        }

        if (status.ok()) {
            rpcNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      std::chrono::steady_clock::now() - rpcT0)
                      .count();
            succeededViaFastPath = attemptViaFastPath;
            break;
        }

        const auto code = status.error_code();
        const bool retryable =
          code == ::grpc::StatusCode::UNAVAILABLE ||
          code == ::grpc::StatusCode::FAILED_PRECONDITION ||
          code == ::grpc::StatusCode::ABORTED ||
          code == ::grpc::StatusCode::DEADLINE_EXCEEDED;
        // Optimisation 2: distinguish "peer moved" from "peer died". When the
        // toggle is off, migrationRedirect is always false, so migration
        // UNAVAILABLE falls through to the genuine-failure back-off path 
        const bool migrationRedirect =
          GRPC_REDIRECT_AWARE_RETRY_ENABLED &&
          code == ::grpc::StatusCode::UNAVAILABLE &&
          status.error_message().find(GRPC_MIGRATION_REDIRECT_MARKER) !=
            std::string::npos;
        const bool budgetExhausted =
          migrationRedirect ? (redirectWaitedMs >= maxRedirectWaitMs ||
                               redirectAttempts + 1 >= maxRedirectAttempts)
                            : (genuineAttempts + 1 >= maxGenuineAttempts);

        if (!retryable || budgetExhausted) {
            SPDLOG_WARN(
              "[GRPC CLIENT] [{}] app {} serviceId {} callId {} method='{}' "
              "streamId={} seqNum={} -> serviceId {} FAILED code={} msg={} "
              "redirect={} fastpath={} genuine_attempts={} "
              "redirect_attempts={} redirect_waited_ms={}/{}",
              kind, appId, serviceId, callId, method, streamId, seqNum, destServiceId,
              static_cast<int>(code), status.error_message(),
              migrationRedirect, attemptViaFastPath, genuineAttempts,
              redirectAttempts, redirectWaitedMs, maxRedirectWaitMs);
            outboxAck(callId);
            throw std::runtime_error(
              fmt::format("Unary call failed {}: {}",
                          static_cast<int>(code),
                          status.error_message()));
        }

        retriesUsed++;
        if (migrationRedirect) {
            redirectAttempts++;
            const int redirectDelayMs =
              std::min(redirectDelayMaxMs,
                       redirectDelayBaseMs
                         << std::min(redirectAttempts - 1, 5));
            redirectWaitedMs += redirectDelayMs;
            SPDLOG_INFO(
              "[GRPC CLIENT] [{}] callId {} method='{}' streamId={} seqNum={} "
              "to serviceId {} MIGRATION REDIRECT reason={} (code={}) "
              "redirect_attempt={}/{} re-resolving in {}ms "
              "(waited {}ms/{}ms)",
              kind, callId, method, streamId, seqNum, destServiceId,
              GRPC_MIGRATION_REDIRECT_MARKER, static_cast<int>(code),
              redirectAttempts, maxRedirectAttempts, redirectDelayMs,
              redirectWaitedMs, maxRedirectWaitMs);
            SPDLOG_INFO("[GRPC METRIC] RETRY_REDIRECT_MS={} reason={} kind={} "
                        "callId={} streamId={} seqNum={} destServiceId={} "
                        "redirect_attempt={}/{} code={} app={} serviceId={} "
                        "redirect_waited_ms={}",
                        redirectDelayMs, GRPC_MIGRATION_REDIRECT_MARKER, kind,
                        callId, streamId, seqNum, destServiceId, redirectAttempts,
                        maxRedirectAttempts, static_cast<int>(code), appId,
                        serviceId, redirectWaitedMs);
            std::this_thread::sleep_for(
              std::chrono::milliseconds(redirectDelayMs));
        } else {
            genuineAttempts++;
            // Exponential backoff (capped): 50, 100, 200, 400 (capped 500) ms.
            const int shift = std::min(genuineAttempts - 1, 4);
            const int backoffMs =
              std::min(maxBackoffMs, backoffBaseMs << shift);
            SPDLOG_INFO(
              "[GRPC CLIENT] [{}] Retrying callId {} method='{}' streamId={} "
              "seqNum={} to serviceId {} after code={} ({}) genuine_attempt={}/{}",
              kind, callId, method, streamId, seqNum, destServiceId,
              static_cast<int>(code), status.error_message(), genuineAttempts,
              maxGenuineAttempts);
            SPDLOG_INFO("[GRPC METRIC] RETRY_BACKOFF_MS={} kind={} callId={} "
                        "streamId={} seqNum={} destServiceId={} "
                        "genuine_attempt={}/{} code={} app={} serviceId={}",
                        backoffMs, kind, callId, streamId, seqNum, destServiceId,
                        genuineAttempts, maxGenuineAttempts,
                        static_cast<int>(code), appId, serviceId);
            std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
        }
        // Drop any cached channel so the next gRPC attempt re-resolves the endpoint via the planner
        {
            std::scoped_lock lock(channelCacheMx);
            channelCache.erase(destServiceId);
        }
        channel.reset();
    }

    if (retriesUsed > 0) {
        SPDLOG_INFO("[GRPC CLIENT] [{}] callId {} method='{}' to serviceId {} "
                    "succeeded after {} retry/retries (fastpath={})",
                    kind, callId, method, destServiceId, retriesUsed,
                    succeededViaFastPath);
    }

    if (response.status() != 0) {
        outboxAck(callId);
        throw std::runtime_error(
          fmt::format("Unary call returned status {} ({})",
                      response.status(),
                      response.error()));
    }

    {
        const auto rtt_t1 = std::chrono::steady_clock::now();
        const auto rtt_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
            rtt_t1 - rtt_t0)
          .count();

        faabric::util::PerfMonitor::instance().recordRtt(rtt_ns, rtt_t0_ns);

        const size_t reqBytes = requestBytes.size();
        const size_t respBytes = response.payload().size();
        SPDLOG_INFO(
          "[GRPC RTT] app={} sourceServiceId={} destServiceId={} callId={} "
          "kind={} method={} rtt_ns={} send_ts_ns={} retries={} seqNum={} "
          "path={} rpc_ns={} build_ns={} req_bytes={} resp_bytes={}",
          appId,
          serviceId,
          destServiceId,
          callId,
          isBidi ? "stream" : "unary",
          method,
          rtt_ns,
          rtt_t0_ns,
          retriesUsed,
          seqNum,
          succeededViaFastPath ? "fastpath" : "grpc",
          rpcNs,
          buildNs,
          reqBytes,
          respBytes);
    }

    outboxAck(callId);
    return std::vector<uint8_t>(response.payload().begin(),
                                response.payload().end());
}

::grpc::Status GrpcWorld::handleInboundLocally(
  const faabric::faasmgrpc::GrpcRequest& request,
  const faasgrpc::GrpcEnvelope& env,
  faabric::faasmgrpc::GrpcResponse* response)
{
    // Optimisation 1: in-process delivery into THIS world's service handler.
    GrpcService* impl = service.get();
    if (impl == nullptr || migratingOut.load()) {
        return { ::grpc::StatusCode::UNAVAILABLE,
                 fmt::format("{}: local world not ready for delivery",
                             GRPC_MIGRATION_REDIRECT_MARKER) };
    }
    return impl->handleUnaryRequest(&request, env, response,
                                    /*viaFastPath=*/true);
}

UnaryRequest GrpcWorld::recvRequest(int timeoutMs)
{
    UnaryRequest req = pendingRequests.dequeue(timeoutMs);
    // Keep pendingRequestsSize in sync for back-pressure accounting.
    pendingRequestsSize.fetch_sub(1, std::memory_order_relaxed);
    return req;
}

void GrpcWorld::sendResponse(int32_t callId,
                             const std::vector<uint8_t>& responseBytes)
{
    std::shared_ptr<std::promise<UnaryResponse>> responsePromise = nullptr;
    {
        std::scoped_lock lock(pendingResponsesMx);
        auto it = pendingResponses.find(callId);
        if (it == pendingResponses.end()) {
            throw std::runtime_error(
              fmt::format("Unknown gRPC call id {} in sendResponse", callId));
        }

        responsePromise = it->second;
        pendingResponses.erase(it);
    }

    UnaryResponse response = {
        .callId = callId,
        .status = 0,
        .error = "",
        .payload = responseBytes,
    };
    responsePromise->set_value(response);
}

std::shared_ptr<std::promise<UnaryResponse>> GrpcWorld::enqueueRequest(
  const UnaryRequest& request)
{
    // RECEIVER BACK-PRESSURE (immediate reject): unlike outboxAdd which blocks
    // the sender, here we throw immediately. 
    if (pendingRequestsSize.load(std::memory_order_relaxed) >=
        PENDING_REQUESTS_MAX) {
        SPDLOG_WARN(
          "[GRPC BACKPRESSURE] app {} serviceId {} pendingRequests FULL "
          "(size~={}/{}): rejecting inbound unary",
          appId, serviceId,
          pendingRequestsSize.load(std::memory_order_relaxed),
          PENDING_REQUESTS_MAX);
        throw std::runtime_error("grpc pendingRequests queue full");
    }

    int32_t callId = request.callId;
    if (callId <= 0) {
        callId = allocateCallId();
    }

    auto responsePromise = std::make_shared<std::promise<UnaryResponse>>();
    {
        std::scoped_lock lock(pendingResponsesMx);
        if (pendingResponses.size() >= PENDING_RESPONSES_MAX) {
            SPDLOG_WARN(
              "[GRPC BACKPRESSURE] app {} serviceId {} pendingResponses FULL "
              "(size={}/{}): rejecting inbound unary callId {}",
              appId, serviceId, pendingResponses.size(),
              PENDING_RESPONSES_MAX, callId);
            throw std::runtime_error("grpc pendingResponses map full");
        }
        pendingResponses[callId] = responsePromise;
    }

    UnaryRequest unaryRequest = request;
    unaryRequest.callId = callId;

    pendingRequestsSize.fetch_add(1, std::memory_order_relaxed);
    pendingRequests.enqueue(std::move(unaryRequest));

    return responsePromise;
}

// Tunnelled bidirectional streams
int32_t GrpcWorld::openBidiStream(int32_t destServiceId)
{
    int32_t streamId = allocateStreamId();

    {
        std::scoped_lock lock(streamsMx);
        StreamCursor cursor;
        cursor.streamId = streamId;
        cursor.peerServiceId = destServiceId;
        cursor.isClient = true;
        cursor.sendSeqNum = 1;
        streamCursors[streamId] = cursor;
        streamInboundQueues[streamId] =
          std::make_shared<faabric::util::Queue<StreamInboundItem>>();
    }

    // Notify the peer to register the inbound stream. We tunnel via unary
    // so it inherits dedupe + retry + forwarding.
    callUnary(destServiceId, BIDI_OPEN_METHOD, {}, streamId, 0);

    SPDLOG_DEBUG(
      "openBidiStream app {} serviceId {} -> serviceId {} streamId {}",
      appId, serviceId, destServiceId, streamId);
    return streamId;
}

int32_t GrpcWorld::recvBidiStreamOpen(int32_t* peerServiceIdOut, int timeoutMs)
{
    StreamOpenNotification n = pendingStreamOpens.dequeue(timeoutMs);
    if (peerServiceIdOut != nullptr) {
        *peerServiceIdOut = n.peerServiceId;
    }
    return n.streamId;
}

void GrpcWorld::registerInboundStream(int32_t streamId, int32_t peerServiceId)
{
    std::vector<StreamInboundItem> backlog;
    {
        std::scoped_lock lock(streamsMx);
        auto cursorIt = streamCursors.find(streamId);
        if (cursorIt == streamCursors.end()) {
            StreamCursor cursor;
            cursor.streamId = streamId;
            cursor.peerServiceId = peerServiceId;
            cursor.isClient = false;
            streamCursors[streamId] = cursor;
        } else {
            // Already known (restored via migration). Just update peer.
            cursorIt->second.peerServiceId = peerServiceId;
        }
        if (!streamInboundQueues.contains(streamId)) {
            streamInboundQueues[streamId] = std::make_shared<
              faabric::util::Queue<StreamInboundItem>>();
        }
        auto bIt = pendingPreOpenItems.find(streamId);
        if (bIt != pendingPreOpenItems.end()) {
            backlog = std::move(bIt->second);
            pendingPreOpenItems.erase(bIt);
        }
    }

    pendingStreamOpens.enqueue(StreamOpenNotification{ streamId, peerServiceId });

    // Re-deliver anything that arrived before the open notification.
    if (!backlog.empty()) {
        std::shared_ptr<faabric::util::Queue<StreamInboundItem>> q;
        {
            std::scoped_lock lock(streamsMx);
            q = streamInboundQueues.at(streamId);
        }
        for (auto& item : backlog) {
            q->enqueue(std::move(item));
        }
    }
}

std::shared_ptr<std::promise<UnaryResponse>> GrpcWorld::enqueueStreamItem(
  int32_t streamId,
  int64_t seqNum,
  std::vector<uint8_t> payload,
  int32_t callId,
  bool isClose)
{
    auto promise = std::make_shared<std::promise<UnaryResponse>>();

    {
        std::scoped_lock lock(streamsMx);
        auto qIt = streamInboundQueues.find(streamId);
        if (qIt != streamInboundQueues.end() &&
            qIt->second->size() >= static_cast<long>(STREAM_QUEUE_MAX)) {
            SPDLOG_WARN(
              "[GRPC BACKPRESSURE] app {} serviceId {} stream {} inbound queue "
              "FULL (size={}/{}): rejecting seq {}",
              appId, serviceId, streamId, qIt->second->size(),
              STREAM_QUEUE_MAX, seqNum);
            throw std::runtime_error("grpc stream queue full");
        }
    }

    if (callId != kNoAckCallId) {
        std::scoped_lock lock(pendingResponsesMx);
        if (pendingResponses.count(callId)) {
            SPDLOG_DEBUG(
              "[GRPC STREAM] in-flight duplicate callId={} stream={} seq={} "
              "serviceId={} — returning immediate OK, skipping enqueue",
              callId, streamId, seqNum, serviceId);
            auto dupPromise = std::make_shared<std::promise<UnaryResponse>>();
            dupPromise->set_value(UnaryResponse{});
            return dupPromise;
        }
        if (pendingResponses.size() >= PENDING_RESPONSES_MAX) {
            SPDLOG_WARN(
              "[GRPC BACKPRESSURE] app {} serviceId {} pendingResponses FULL "
              "(size={}/{}): rejecting stream item callId {} stream {} seq {}",
              appId, serviceId, pendingResponses.size(),
              PENDING_RESPONSES_MAX, callId, streamId, seqNum);
            throw std::runtime_error("grpc pendingResponses map full");
        }
        pendingResponses[callId] = promise;
    }

    StreamInboundItem item;
    item.streamId = streamId;
    item.seqNum = seqNum;
    item.payload = std::move(payload);
    item.callId = callId;
    item.isClose = isClose;

    std::shared_ptr<faabric::util::Queue<StreamInboundItem>> q;
    bool stash = false;
    {
        std::scoped_lock lock(streamsMx);
        auto qIt = streamInboundQueues.find(streamId);
        if (qIt != streamInboundQueues.end()) {
            q = qIt->second;
        } else {
            stash = true;
            pendingPreOpenItems[streamId].push_back(item);
        }
    }
    if (!stash) {
        q->enqueue(std::move(item));
    }

    return promise;
}

void GrpcWorld::bidiStreamSend(int32_t streamId,
                               const std::vector<uint8_t>& data)
{
    int32_t destServiceId = -1;
    int64_t seqNum = 0;
    {
        std::scoped_lock lock(streamsMx);
        auto it = streamCursors.find(streamId);
        if (it == streamCursors.end()) {
            throw std::runtime_error(
              fmt::format("bidiStreamSend on unknown stream {}", streamId));
        }
        if (it->second.halfClosedLocal) {
            throw std::runtime_error(
              fmt::format("bidiStreamSend on half-closed stream {}", streamId));
        }
        destServiceId = it->second.peerServiceId;
        seqNum = it->second.sendSeqNum;
        it->second.sendSeqNum = seqNum + 1;
    }

    callUnary(destServiceId, BIDI_SEND_METHOD, data, streamId, seqNum);
}

std::vector<uint8_t> GrpcWorld::bidiStreamRecv(int32_t streamId,
                                               int64_t* seqOut,
                                               bool* isCloseOut,
                                               int timeoutMs)
{
    std::shared_ptr<faabric::util::Queue<StreamInboundItem>> q;
    {
        std::scoped_lock lock(streamsMx);
        auto qIt = streamInboundQueues.find(streamId);
        if (qIt == streamInboundQueues.end()) {
            throw std::runtime_error(
              fmt::format("bidiStreamRecv on unknown stream {}", streamId));
        }
        q = qIt->second;
    }

    StreamInboundItem item = q->dequeue(timeoutMs);

    // Ack the underlying unary call so the sender side can advance.
    if (item.callId != kNoAckCallId) {
        UnaryResponse ack = {
            .callId = item.callId,
            .status = 0,
            .error = "",
            .payload = {},
        };
        std::shared_ptr<std::promise<UnaryResponse>> p;
        {
            std::scoped_lock plock(pendingResponsesMx);
            auto it = pendingResponses.find(item.callId);
            if (it != pendingResponses.end()) {
                p = it->second;
                pendingResponses.erase(it);
            }
        }
        if (p) {
            p->set_value(ack);
        }
    }

    {
        std::scoped_lock lock(streamsMx);
        auto cIt = streamCursors.find(streamId);
        if (cIt != streamCursors.end()) {
            cIt->second.lastReceivedSeq =
              std::max(cIt->second.lastReceivedSeq, item.seqNum);
            if (item.isClose) {
                cIt->second.halfClosedRemote = true;
            }
        }
    }

    if (seqOut != nullptr) {
        *seqOut = item.seqNum;
    }
    if (isCloseOut != nullptr) {
        *isCloseOut = item.isClose;
    }
    return item.payload;
}

void GrpcWorld::bidiStreamHalfClose(int32_t streamId)
{
    int32_t destServiceId = -1;
    {
        std::scoped_lock lock(streamsMx);
        auto it = streamCursors.find(streamId);
        if (it == streamCursors.end()) {
            return;
        }
        if (it->second.halfClosedLocal) {
            return;
        }
        destServiceId = it->second.peerServiceId;
        it->second.halfClosedLocal = true;
    }

    if (destServiceId >= 0) {
        try {
            callUnary(destServiceId, BIDI_CLOSE_METHOD, {}, streamId, 0);
        } catch (const std::exception& ex) {
            SPDLOG_WARN("bidiStreamHalfClose unary failed: {}", ex.what());
        }
    }
}

// Retransmission & recovery
std::vector<OutboxEntry> GrpcWorld::snapshotOutboxForStream(
  int32_t streamId,
  int64_t fromSeq,
  int64_t toSeq)
{
    // Guard against callers passing streamId==0. All unary outbox entries carry
    // streamId==0, so a zero query would match every unary in-flight call and
    // replay them as stream messages
    if (streamId == 0) {
        SPDLOG_WARN("[GRPC RETRANSMIT] app {} serviceId {} refusing snapshotOutboxForStream "
                    "with streamId=0 (would match unary entries)",
                    appId, serviceId);
        return {};
    }
    std::vector<OutboxEntry> matches;
    std::scoped_lock lock(outboxMx);
    for (const auto& [cid, entry] : outbox) {
        if (entry.streamId == 0) {
            continue;
        }
        if (entry.streamId != streamId) {
            continue;
        }
        if (entry.seqNum < fromSeq || entry.seqNum >= toSeq) {
            continue;
        }
        matches.push_back(entry);
    }
    return matches;
}

bool GrpcWorld::replayOutboxEntry(const OutboxEntry& entry)
{
    // Re-issue the stored outbox entry to its destination with the original
    // callId/dedupeUUID so the peer's dedupe cache can deduplicate if it
    // already processed the call.
    const char* kind = (entry.streamId == 0) ? "UNARY" : "STREAM";
    faabric::faasmgrpc::GrpcRequest request;
    buildRequest(request,
                 entry.destServiceId,
                 entry.envelope.callId,
                 entry.method,
                 entry.streamId,
                 entry.seqNum,
                 entry.requestBytes);

    std::shared_ptr<::grpc::Channel> channel;
    try {
        channel = getOrCreateChannel(entry.destServiceId);
    } catch (const std::exception& ex) {
        SPDLOG_WARN("[GRPC RETRANSMIT] [{}] replay channel failure callId {} "
                    "streamId={} seqNum={}: {}",
                    kind, entry.envelope.callId,
                    entry.streamId, entry.seqNum, ex.what());
        return false;
    }
    auto stub = faabric::faasmgrpc::FaasmGrpc::NewStub(channel);
    ::grpc::ClientContext ctx;
    faasgrpc::addEnvelopeToClientContext(ctx, entry.envelope);
    faabric::faasmgrpc::GrpcResponse response;
    auto status = stub->CallUnary(&ctx, request, &response);
    if (!status.ok()) {
        SPDLOG_WARN(
          "[GRPC RETRANSMIT] [{}] replay callId {} streamId={} seqNum={} "
          "to serviceId {} failed code={} msg={}",
          kind, entry.envelope.callId, entry.streamId, entry.seqNum,
          entry.destServiceId,
          static_cast<int>(status.error_code()),
          status.error_message());
        return false;
    }
    SPDLOG_INFO(
      "[GRPC RETRANSMIT] [{}] replayed callId {} streamId={} seqNum={} "
      "-> serviceId {} status=OK",
      kind, entry.envelope.callId, entry.streamId, entry.seqNum, entry.destServiceId);
    return true;
}

bool GrpcWorld::doRetransmitRequest(int32_t streamId,
                                    int32_t peerServiceId,
                                    int64_t fromSeq)
{
    try {
        auto channel = getOrCreateChannel(peerServiceId);
        auto stub = faabric::faasmgrpc::FaasmGrpc::NewStub(channel);
        ::grpc::ClientContext ctx;
        faabric::faasmgrpc::RetransmitStreamRangeRequest rreq;
        rreq.set_appid(appId);
        rreq.set_streamid(streamId);
        rreq.set_requesterserviceid(serviceId);
        rreq.set_fromseq(fromSeq);
        rreq.set_toseq(std::numeric_limits<int64_t>::max());
        faabric::faasmgrpc::RetransmitStreamRangeResponse rresp;
        auto status = stub->RetransmitStreamRange(&ctx, rreq, &rresp);
        if (!status.ok()) {
            SPDLOG_WARN(
              "[GRPC RETRANSMIT] app {} serviceId {} stream {} peer serviceId {} "
              "returned {} ({})",
              appId, serviceId, streamId, peerServiceId,
              static_cast<int>(status.error_code()),
              status.error_message());
            return false;
        }
        SPDLOG_INFO(
          "[GRPC RETRANSMIT] app {} serviceId {} stream {} peer serviceId {} "
          "ack status=OK (resp.status={})",
          appId, serviceId, streamId, peerServiceId, rresp.status());
        return true;
    } catch (const std::exception& ex) {
        SPDLOG_WARN("[GRPC RETRANSMIT] doRetransmitRequest exception: {}",
                    ex.what());
        return false;
    }
}
}
