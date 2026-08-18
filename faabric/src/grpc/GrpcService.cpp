#include <faabric/grpc/GrpcService.h>

#include <faabric/planner/PlannerClient.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faasgrpc/GrpcEnvelope.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <fmt/format.h>
#include <vector>

namespace faabric::grpc {

using faabric::faasmgrpc::GrpcRequest;
using faabric::faasmgrpc::GrpcResponse;

namespace {
inline std::string migrationRejectMsg(const std::string& detail)
{
    if (GRPC_REDIRECT_AWARE_RETRY_ENABLED) {
        return fmt::format("{}: {}", GRPC_MIGRATION_REDIRECT_MARKER, detail);
    }
    return detail;
}

inline const char* migrationReasonTag()
{
    return GRPC_REDIRECT_AWARE_RETRY_ENABLED ? GRPC_MIGRATION_REDIRECT_MARKER
                                             : "redirect-disabled";
}
} 

// Lifecycle & utilities
static int32_t knownEpochForSender(
  const std::map<SenderEpochKey, int32_t>& knownEpochPerSender,
  const SenderEpochKey& key)
{
    auto it = knownEpochPerSender.find(key);
    if (it == knownEpochPerSender.end()) {
        return -1;
    }
    return it->second;
}

static const char* statusCodeName(::grpc::StatusCode code)
{
    switch (code) {
        case ::grpc::StatusCode::OK: return "OK";
        case ::grpc::StatusCode::CANCELLED: return "CANCELLED";
        case ::grpc::StatusCode::UNKNOWN: return "UNKNOWN";
        case ::grpc::StatusCode::INVALID_ARGUMENT: return "INVALID_ARGUMENT";
        case ::grpc::StatusCode::DEADLINE_EXCEEDED: return "DEADLINE_EXCEEDED";
        case ::grpc::StatusCode::NOT_FOUND: return "NOT_FOUND";
        case ::grpc::StatusCode::ALREADY_EXISTS: return "ALREADY_EXISTS";
        case ::grpc::StatusCode::PERMISSION_DENIED: return "PERMISSION_DENIED";
        case ::grpc::StatusCode::UNAUTHENTICATED: return "UNAUTHENTICATED";
        case ::grpc::StatusCode::RESOURCE_EXHAUSTED: return "RESOURCE_EXHAUSTED";
        case ::grpc::StatusCode::FAILED_PRECONDITION: return "FAILED_PRECONDITION";
        case ::grpc::StatusCode::ABORTED: return "ABORTED";
        case ::grpc::StatusCode::OUT_OF_RANGE: return "OUT_OF_RANGE";
        case ::grpc::StatusCode::UNIMPLEMENTED: return "UNIMPLEMENTED";
        case ::grpc::StatusCode::INTERNAL: return "INTERNAL";
        case ::grpc::StatusCode::UNAVAILABLE: return "UNAVAILABLE";
        case ::grpc::StatusCode::DATA_LOSS: return "DATA_LOSS";
        default: return "OTHER";
    }
}

GrpcServiceImpl::GrpcServiceImpl(GrpcWorld& worldIn)
  : world(worldIn)
{
    forwardExpiryThread =
      std::thread(&GrpcServiceImpl::forwardExpiryLoop, this);
}

GrpcServiceImpl::~GrpcServiceImpl()
{
    forwardExpiryStop.store(true, std::memory_order_release);
    forwardExpiryCv.notify_all();
    if (forwardExpiryThread.joinable()) {
        forwardExpiryThread.join();
    }
    {
        std::scoped_lock lock(forwardMx);
        if (forwardEntry.has_value()) {
            emitForwardExpiredLocked(*forwardEntry);
            forwardEntry.reset();
        }
    }
    emitStatusSummary();
}
std::size_t GrpcServiceImpl::dedupeCacheSize() const
{
    return dedupeCache.size();
}

void GrpcServiceImpl::fillResponse(GrpcResponse* response,
                                   const GrpcRequest* request,
                                   int32_t callId,
                                   int32_t status,
                                   const std::string& error,
                                   const std::vector<uint8_t>& payload,
                                   bool servedFromCache) const
{
    response->set_appid(world.getAppId());
    response->set_callid(callId);
    response->set_sourceserviceid(world.getServiceId());
    response->set_destserviceid(request->sourceserviceid());
    response->set_status(status);
    response->set_error(error);
    if (!payload.empty()) {
        response->set_payload(payload.data(), payload.size());
    } else {
        response->clear_payload();
    }
    response->set_served_from_dedupe_cache(servedFromCache);
}

void GrpcServiceImpl::recordInboundStatus(::grpc::StatusCode code)
{
    auto idx = static_cast<std::size_t>(code);
    if (idx >= NUM_STATUS_BUCKETS) {
        idx = static_cast<std::size_t>(::grpc::StatusCode::UNKNOWN);
    }
    inboundStatusCounts[idx].fetch_add(1, std::memory_order_relaxed);
}

void GrpcServiceImpl::recordForwardStatus(::grpc::StatusCode code)
{
    auto idx = static_cast<std::size_t>(code);
    if (idx >= NUM_STATUS_BUCKETS) {
        idx = static_cast<std::size_t>(::grpc::StatusCode::UNKNOWN);
    }
    forwardStatusCounts[idx].fetch_add(1, std::memory_order_relaxed);
}

void GrpcServiceImpl::emitStatusSummary()
{
    auto render = [](const std::array<std::atomic<uint64_t>,
                                      NUM_STATUS_BUCKETS>& counts) {
        std::string out;
        for (std::size_t i = 0; i < NUM_STATUS_BUCKETS; ++i) {
            uint64_t v = counts[i].load(std::memory_order_relaxed);
            if (v == 0) {
                continue;
            }
            if (!out.empty()) {
                out += ",";
            }
            out += fmt::format(
              "{}={}", statusCodeName(static_cast<::grpc::StatusCode>(i)), v);
        }
        if (out.empty()) {
            out = "none";
        }
        return out;
    };
    std::uint64_t probesPerformed = 0;
    std::uint64_t probesSuppressed = 0;
    std::size_t senderEpochSize = 0;
    std::size_t senderEpochEvictedLocal = 0;
    {
        std::scoped_lock lock(forwardMx);
        probesPerformed = plannerProbesPerformed;
        probesSuppressed = plannerProbesSuppressed;
    }
    {
        std::scoped_lock lock(rpcMx);
        senderEpochSize = knownEpochPerSender.size();
        senderEpochEvictedLocal = senderEpochEvictions;
    }
    SPDLOG_INFO("[GRPC STATUS] Summary app {} serviceId {} inbound=[{}] "
                "forwarded=[{}] dedupe_cache_size={} dedupe_evictions={} "
                "sender_epochs={} sender_epoch_evictions={} "
                "planner_probes={} planner_probes_suppressed={}",
                world.getAppId(),
                world.getServiceId(),
                render(inboundStatusCounts),
                render(forwardStatusCounts),
                dedupeCacheSize(),
                dedupeEvictions,
                senderEpochSize,
                senderEpochEvictedLocal,
                probesPerformed,
                probesSuppressed);
}

// Migration orchestration
void GrpcServiceImpl::snapshotMigrationState(
  faabric::GrpcMigrationMetadata& meta)
{
    std::scoped_lock lock(rpcMx);
    for (const DedupeKey& key : dedupeInsertOrder) {
        auto it = dedupeCache.find(key);
        if (it == dedupeCache.end()) {
            continue;
        }
        const CachedUnaryResponse& cached = it->second;
        auto* out = meta.add_dedupecache();
        out->set_sourceserviceid(std::get<0>(key));
        out->set_dedupeuuid(std::get<1>(key));
        out->set_callid(std::get<2>(key));
        out->set_status(cached.status);
        out->set_error(cached.error);
        if (!cached.payload.empty()) {
            out->set_payload(cached.payload.data(), cached.payload.size());
        }
    }
    for (const SenderEpochKey& key : senderEpochInsertOrder) {
        auto it = knownEpochPerSender.find(key);
        if (it == knownEpochPerSender.end()) {
            continue;
        }
        auto* out = meta.add_senderepochs();
        out->set_sourceserviceid(key.first);
        out->set_dedupeuuid(key.second);
        out->set_knownepoch(it->second);
    }
    SPDLOG_INFO(
      "[GRPC MIGRATE] Snapshotting service state: app {} serviceId {} "
      "dedupe_entries={} sender_epochs={} cumulative_evictions={}",
      world.getAppId(),
      world.getServiceId(),
      meta.dedupecache_size(),
      meta.senderepochs_size(),
      dedupeEvictions);
}

void GrpcServiceImpl::restoreMigrationState(
  const faabric::GrpcMigrationMetadata& meta)
{
    std::scoped_lock lock(rpcMx);
    dedupeCache.clear();
    dedupeInsertOrder.clear();
    knownEpochPerSender.clear();
    senderEpochInsertOrder.clear();
    for (const auto& e : meta.dedupecache()) {
        DedupeKey key =
          std::make_tuple(e.sourceserviceid(), e.dedupeuuid(), e.callid());
        CachedUnaryResponse cached;
        cached.status = e.status();
        cached.error = e.error();
        const std::string& payload = e.payload();
        cached.payload.assign(payload.begin(), payload.end());
        dedupeCache[key] = std::move(cached);
        dedupeInsertOrder.push_back(key);
    }
    for (const auto& e : meta.senderepochs()) {
        SenderEpochKey key = std::make_pair(e.sourceserviceid(), e.dedupeuuid());
        knownEpochPerSender[key] = e.knownepoch();
        senderEpochInsertOrder.push_back(key);
    }
    SPDLOG_INFO(
      "[GRPC MIGRATE] Restored service state: app {} serviceId {} "
      "dedupe_entries={} sender_epochs={}",
      world.getAppId(),
      world.getServiceId(),
      dedupeCache.size(),
      knownEpochPerSender.size());
}

// Unary communication
std::size_t GrpcServiceImpl::evictDedupeIfOverfullLocked()
{
    std::size_t evicted = 0;
    while (dedupeCache.size() > DEDUPE_CACHE_MAX_ENTRIES &&
           !dedupeInsertOrder.empty()) {
        const DedupeKey& oldest = dedupeInsertOrder.front();
        dedupeCache.erase(oldest);
        dedupeInsertOrder.pop_front();
        ++evicted;
    }
    if (evicted > 0) {
        dedupeEvictions += evicted;
        SPDLOG_INFO("[GRPC DEDUPE] Evicted {} oldest entries (cap={} size={} "
                    "total_evicted={})",
                    evicted,
                    DEDUPE_CACHE_MAX_ENTRIES,
                    dedupeCache.size(),
                    dedupeEvictions);
    }
    return evicted;
}

std::size_t GrpcServiceImpl::evictSenderEpochIfOverfullLocked()
{
    std::size_t evicted = 0;
    while (knownEpochPerSender.size() > SENDER_EPOCH_MAX_ENTRIES &&
           !senderEpochInsertOrder.empty()) {
        const SenderEpochKey& oldest = senderEpochInsertOrder.front();
        knownEpochPerSender.erase(oldest);
        senderEpochInsertOrder.pop_front();
        ++evicted;
    }
    if (evicted > 0) {
        senderEpochEvictions += evicted;
        SPDLOG_INFO(
          "[GRPC EPOCH] Evicted {} oldest sender-epoch entries (cap={} "
          "size={} total_evicted={})",
          evicted,
          SENDER_EPOCH_MAX_ENTRIES,
          knownEpochPerSender.size(),
          senderEpochEvictions);
    }
    return evicted;
}

::grpc::Status GrpcServiceImpl::CallUnary(::grpc::ServerContext* context,
                                          const GrpcRequest* request,
                                          GrpcResponse* response)
{
    auto envOpt = faasgrpc::extractEnvelopeFromServerContext(context);
    if (!envOpt) {
        recordInboundStatus(::grpc::StatusCode::INVALID_ARGUMENT);
        SPDLOG_WARN("[GRPC STATUS] app {} serviceId {} CallUnary rejected "
                    "INVALID_ARGUMENT (missing envelope)",
                    world.getAppId(),
                    world.getServiceId());
        return { ::grpc::StatusCode::INVALID_ARGUMENT,
                 "missing granny-envelope metadata" };
    }
    return handleUnaryRequest(request, *envOpt, response, /*viaFastPath=*/false);
}

::grpc::Status GrpcServiceImpl::handleUnaryRequest(
  const GrpcRequest* request,
  const faasgrpc::GrpcEnvelope& env,
  GrpcResponse* response,
  bool viaFastPath)
{
    const auto recv_ts = std::chrono::steady_clock::now();

    if (request->callid() == kNoAckCallId || env.callId == kNoAckCallId) {
        recordInboundStatus(::grpc::StatusCode::INVALID_ARGUMENT);
        SPDLOG_WARN("[GRPC STATUS] app {} serviceId {} CallUnary INVALID_ARGUMENT "
                    "callId=0 is reserved (kNoAckCallId): "
                    "request.callId={} envelope.callId={}",
                    world.getAppId(),
                    world.getServiceId(),
                    request->callid(),
                    env.callId);
        return { ::grpc::StatusCode::INVALID_ARGUMENT,
                 "callId 0 is reserved" };
    }

    //  envelope/body integrity — callId must agree in both transports.
    if (env.callId != request->callid()) {
        recordInboundStatus(::grpc::StatusCode::INVALID_ARGUMENT);
        SPDLOG_WARN("[GRPC STATUS] app {} serviceId {} CallUnary INVALID_ARGUMENT "
                    "envelope.callId {} != request.callId {}",
                    world.getAppId(),
                    world.getServiceId(),
                    env.callId,
                    request->callid());
        return { ::grpc::StatusCode::INVALID_ARGUMENT,
                 "envelope callId does not match GrpcRequest.callId" };
    }

    // Calls routed to CallUnary that are not bidi control messages
    // must carry streamId==0 and seqNum==0
    {
        const std::string& method = request->method();
        if (!isBidiMethod(method) &&
            (request->streamid() != 0 || request->seqnum() != 0)) {
            recordInboundStatus(::grpc::StatusCode::INVALID_ARGUMENT);
            SPDLOG_WARN(
              "[GRPC STATUS] [UNARY] app {} serviceId {} CallUnary INVALID_ARGUMENT "
              "non-bidi method '{}' has non-zero streamId={} or seqNum={}",
              world.getAppId(),
              world.getServiceId(),
              method,
              request->streamid(),
              request->seqnum());
            return { ::grpc::StatusCode::INVALID_ARGUMENT,
                     "unary call has non-zero streamId or seqNum" };
        }
    }

    // install a forward entry if this world is in migratingOut
    if (env.forwardHops == 0 && world.isMigrating()) {
        std::string forwardProbe;
        bool needProbe = false;
        {
            std::scoped_lock lock(forwardMx);
            if (!forwardEntry.has_value() && !forwardRetired) {
                needProbe = shouldProbePlannerLocked();
            }
        }
        if (needProbe && !lookupForward(&forwardProbe)) {
            try {
                auto endpoint = faabric::planner::getPlannerClient()
                                  .getGrpcEndpoint(world.getAppId(),
                                                   world.getServiceId());
                const std::string selfEndpoint =
                  fmt::format("{}:{}",
                              world.getThisHost(),
                              world.getListenPort());
                if (!endpoint.empty() && endpoint != selfEndpoint) {
                    installForward(endpoint);
                }
            } catch (const std::exception& ex) {
                SPDLOG_WARN("Failed to probe planner for forward: {}",
                            ex.what());
            }
        }
    }

    std::string forwardEndpoint;
    if (env.forwardHops == 0 && world.isMigrating()) {
        bool hasForward = false;
        bool retired = false;
        std::string retiredEndpoint;
        {
            std::scoped_lock fwdLock(forwardMx);
            hasForward = forwardEntry.has_value() &&
                         std::chrono::steady_clock::now() <=
                           forwardEntry->expiresAt;
            retired = forwardRetired;
            retiredEndpoint = forwardRetiredEndpoint;
        }
        if (!hasForward) {
            recordInboundStatus(::grpc::StatusCode::UNAVAILABLE);
            if (retired) {
                SPDLOG_INFO(
                  "[GRPC STATUS] app {} serviceId {} CallUnary UNAVAILABLE "
                  "(forward retired after grace; must retry via planner) "
                  "callId {} sourceServiceId {} last_forward_endpoint={}",
                  world.getAppId(),
                  world.getServiceId(),
                  env.callId,
                  request->sourceserviceid(),
                  retiredEndpoint);
                return { ::grpc::StatusCode::UNAVAILABLE,
                         migrationRejectMsg("forward retired; retry via planner") };
            }

            SPDLOG_INFO("[GRPC STATUS] app {} serviceId {} CallUnary UNAVAILABLE "
                        "(migrating, no forward yet) reason={} callId {} "
                        "sourceServiceId {}",
                        world.getAppId(),
                        world.getServiceId(),
                        migrationReasonTag(),
                        env.callId,
                        request->sourceserviceid());
            return { ::grpc::StatusCode::UNAVAILABLE,
                     migrationRejectMsg("serviceId migrating; retry via planner") };
        }
    }

    if (env.forwardHops >= MAX_FORWARD_HOPS) {
        recordInboundStatus(::grpc::StatusCode::FAILED_PRECONDITION);
        SPDLOG_WARN("[GRPC STATUS] app {} serviceId {} CallUnary FAILED_PRECONDITION "
                    "(forward hop limit reached {} >= {}) callId {} sourceServiceId {}",
                    world.getAppId(),
                    world.getServiceId(),
                    env.forwardHops,
                    MAX_FORWARD_HOPS,
                    env.callId,
                    request->sourceserviceid());
        return { ::grpc::StatusCode::FAILED_PRECONDITION,
                 "forward hop limit exceeded" };
    }

    {
        const char* fwdKind = bidiKindTag(request->method());

        if (lookupForward(&forwardEndpoint, env.callId)) {
            auto channel = ::grpc::CreateChannel(
              forwardEndpoint, ::grpc::InsecureChannelCredentials());
            auto stub = faabric::faasmgrpc::FaasmGrpc::NewStub(channel);

            faasgrpc::GrpcEnvelope forwardedEnv = env;
            forwardedEnv.forwardHops = env.forwardHops + 1;
            ::grpc::ClientContext fwdCtx;
            faasgrpc::addEnvelopeToClientContext(fwdCtx, forwardedEnv);
            auto fwdStatus = stub->CallUnary(&fwdCtx, *request, response);
            recordForwardStatus(fwdStatus.error_code());
            recordInboundStatus(fwdStatus.error_code());
            if (fwdStatus.ok()) {
                SPDLOG_DEBUG(
                  "[GRPC FORWARD] [{}] callId {} streamId={} seqNum={} "
                  "src {} -> {} hop {} status=OK",
                  fwdKind, env.callId,
                  request->streamid(), request->seqnum(),
                  request->sourceserviceid(), forwardEndpoint,
                  forwardedEnv.forwardHops);
            } else {
                SPDLOG_WARN(
                  "[GRPC FORWARD] [{}] callId {} streamId={} seqNum={} "
                  "src {} -> {} hop {} status={} ({}) msg={}",
                  fwdKind, env.callId,
                  request->streamid(), request->seqnum(),
                  request->sourceserviceid(), forwardEndpoint,
                  forwardedEnv.forwardHops,
                  static_cast<int>(fwdStatus.error_code()),
                  statusCodeName(fwdStatus.error_code()),
                  fwdStatus.error_message());
            }
            return fwdStatus;
        }
    }

    const int32_t sourceServiceId = request->sourceserviceid();
    const DedupeKey dedupeKey =
      std::make_tuple(sourceServiceId, env.dedupeUUID, env.callId);
    const SenderEpochKey senderKey =
      std::make_pair(sourceServiceId, env.dedupeUUID);

    std::unique_lock<std::mutex> lock(rpcMx);

    auto dupIt = dedupeCache.find(dedupeKey);
    if (dupIt != dedupeCache.end()) {
        const CachedUnaryResponse& cached = dupIt->second;
        fillResponse(response,
                     request,
                     request->callid(),
                     cached.status,
                     cached.error,
                     cached.payload,
                     /*servedFromCache=*/true);
        SPDLOG_INFO("[GRPC DEDUPE] [UNARY] Cache hit: app {} sourceServiceId {} "
                    "callId {} epoch {} cache_size={} - returning cached response",
                    world.getAppId(),
                    sourceServiceId,
                    env.callId,
                    env.migrationEpoch,
                    dedupeCache.size());
        recordInboundStatus(::grpc::StatusCode::OK);
        return ::grpc::Status::OK;
    }

    const int32_t prevKnown = knownEpochForSender(knownEpochPerSender, senderKey);
    if (env.migrationEpoch < prevKnown) {
        {
            const std::string& em = request->method();
            SPDLOG_WARN("[GRPC DEDUPE] [{}] Rejected stale call: app {} sourceServiceId {} "
                        "callId {} method='{}' inbound epoch {} < known epoch {}",
                        bidiKindTag(em),
                        world.getAppId(),
                        sourceServiceId,
                        env.callId,
                        em,
                        env.migrationEpoch,
                        prevKnown);
        }
        recordInboundStatus(::grpc::StatusCode::FAILED_PRECONDITION);
        return { ::grpc::StatusCode::FAILED_PRECONDITION,
                 "stale grpc migration epoch" };
    }

    lock.unlock();

    // tunnlled bidi calls
    const std::string& method = request->method();
    std::shared_ptr<std::promise<UnaryResponse>> responsePromise;
    if (method == BIDI_OPEN_METHOD) {
        world.registerInboundStream(request->streamid(),
                                    request->sourceserviceid());
        // Inline OK response. no WASM-side ack required.
        UnaryResponse imm = {
            .callId = request->callid(),
            .status = 0,
            .error = "",
            .payload = {},
        };
        auto p = std::make_shared<std::promise<UnaryResponse>>();
        p->set_value(imm);
        responsePromise = p;
    } else if (method == BIDI_CLOSE_METHOD) {
        world.enqueueStreamItem(request->streamid(),
                                request->seqnum(),
                                {},
                                0 /* no ack */,
                                true /* isClose */);
        UnaryResponse imm = {
            .callId = request->callid(),
            .status = 0,
            .error = "",
            .payload = {},
        };
        auto p = std::make_shared<std::promise<UnaryResponse>>();
        p->set_value(imm);
        responsePromise = p;
    } else if (method == BIDI_SEND_METHOD) {
        std::vector<uint8_t> payload(request->payload().begin(),
                                     request->payload().end());
        try {
            responsePromise = world.enqueueStreamItem(request->streamid(),
                                                      request->seqnum(),
                                                      std::move(payload),
                                                      request->callid(),
                                                      false);
        } catch (const std::exception& ex) {
            recordInboundStatus(::grpc::StatusCode::RESOURCE_EXHAUSTED);
            SPDLOG_WARN(
              "[GRPC STATUS] app {} serviceId {} CallUnary RESOURCE_EXHAUSTED "
              "stream queue back-pressure callId {}: {}",
              world.getAppId(), world.getServiceId(), env.callId, ex.what());
            return { ::grpc::StatusCode::RESOURCE_EXHAUSTED, ex.what() };
        }
    } else {
        UnaryRequest unaryRequest = {
            .callId = request->callid(),
            .sourceServiceId = request->sourceserviceid(),
            .method = method,
            .payload = std::vector<uint8_t>(request->payload().begin(),
                                            request->payload().end()),
        };
        try {
            responsePromise = world.enqueueRequest(unaryRequest);
        } catch (const std::exception& ex) {
            recordInboundStatus(::grpc::StatusCode::RESOURCE_EXHAUSTED);
            SPDLOG_WARN(
              "[GRPC STATUS] app {} serviceId {} CallUnary RESOURCE_EXHAUSTED "
              "pendingRequests back-pressure callId {}: {}",
              world.getAppId(), world.getServiceId(), env.callId, ex.what());
            return { ::grpc::StatusCode::RESOURCE_EXHAUSTED, ex.what() };
        }
    }
    auto responseFuture = responsePromise->get_future();

    constexpr int kSliceMs = 250;
    constexpr int kMaxWaitMs = 30000;
    int waitedMs = 0;
    std::future_status waitStatus = std::future_status::timeout;
    while (waitedMs < kMaxWaitMs) {
        waitStatus =
          responseFuture.wait_for(std::chrono::milliseconds(kSliceMs));
        if (waitStatus == std::future_status::ready) {
            break;
        }
        if (world.isMigrating()) {
            recordInboundStatus(::grpc::StatusCode::UNAVAILABLE);
            SPDLOG_INFO(
              "[GRPC STATUS] app {} serviceId {} CallUnary UNAVAILABLE (migrating "
              "mid-wait after {}ms) reason={} callId {} sourceServiceId {} method={}",
              world.getAppId(), world.getServiceId(),
              waitedMs, migrationReasonTag(), env.callId, sourceServiceId,
              request->method());
            return { ::grpc::StatusCode::UNAVAILABLE,
                     migrationRejectMsg("serviceId started migrating during wait") };
        }
        waitedMs += kSliceMs;
    }
    if (waitStatus != std::future_status::ready) {
        recordInboundStatus(::grpc::StatusCode::DEADLINE_EXCEEDED);
        SPDLOG_WARN("[GRPC STATUS] app {} serviceId {} CallUnary DEADLINE_EXCEEDED "
                    "callId {} sourceServiceId {} method={} waited_ms={}",
                    world.getAppId(),
                    world.getServiceId(),
                    env.callId,
                    sourceServiceId,
                    request->method(),
                    waitedMs);
        return ::grpc::Status(::grpc::StatusCode::DEADLINE_EXCEEDED,
                              "Timed out waiting for unary response");
    }

    UnaryResponse unaryResponse;
    try {
        unaryResponse = responseFuture.get();
    } catch (const std::exception& ex) {
        recordInboundStatus(::grpc::StatusCode::UNAVAILABLE);
        SPDLOG_INFO("[GRPC STATUS] app {} serviceId {} CallUnary UNAVAILABLE "
                    "(promise rejected) callId {} sourceServiceId {} msg={}",
                    world.getAppId(),
                    world.getServiceId(),
                    env.callId,
                    sourceServiceId,
                    ex.what());
        return { ::grpc::StatusCode::UNAVAILABLE,
                 migrationRejectMsg(ex.what()) };
    }

    CachedUnaryResponse cached = {
        .status = unaryResponse.status,
        .error = unaryResponse.error,
        .payload = unaryResponse.payload,
    };

    lock.lock();
    const int32_t latestKnown =
      knownEpochForSender(knownEpochPerSender, senderKey);
    auto epochInsert = knownEpochPerSender.insert_or_assign(
      senderKey, std::max(latestKnown, env.migrationEpoch));
    if (epochInsert.second) {
        senderEpochInsertOrder.push_back(senderKey);
    }
    evictSenderEpochIfOverfullLocked();
    auto insertRes = dedupeCache.insert_or_assign(dedupeKey, std::move(cached));
    if (insertRes.second) {
        dedupeInsertOrder.push_back(dedupeKey);
    }
    evictDedupeIfOverfullLocked();

    const auto respBuildT0 = std::chrono::steady_clock::now();
    fillResponse(response,
                 request,
                 unaryResponse.callId,
                 unaryResponse.status,
                 unaryResponse.error,
                 unaryResponse.payload,
                 /*servedFromCache=*/false);
    const int64_t respBuildNs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - respBuildT0)
        .count();

    recordInboundStatus(::grpc::StatusCode::OK);

    {
        const auto reply_ts = std::chrono::steady_clock::now();
        const auto serverside_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
            reply_ts - recv_ts)
          .count();
        const auto recv_ts_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
            recv_ts.time_since_epoch())
          .count();
        const std::string& method = request->method();
        const bool isBidiTunnel = isBidiMethod(method);
        const size_t reqBytes = request->payload().size();
        const size_t respBytes = unaryResponse.payload.size();
        SPDLOG_INFO(
          "[GRPC SERVED] app={} destServiceId={} sourceServiceId={} callId={} "
          "kind={} method={} serverside_ns={} recv_ts_ns={} "
          "path={} resp_build_ns={} req_bytes={} resp_bytes={}",
          world.getAppId(),
          world.getServiceId(),
          request->sourceserviceid(),
          request->callid(),
          isBidiTunnel ? "stream" : "unary",
          method,
          serverside_ns,
          recv_ts_ns,
          viaFastPath ? "fastpath" : "grpc",
          respBuildNs,
          reqBytes,
          respBytes);
    }

    return ::grpc::Status::OK;
}

// Retransmission & recovery
::grpc::Status GrpcServiceImpl::RetransmitStreamRange(
  ::grpc::ServerContext* context,
  const faabric::faasmgrpc::RetransmitStreamRangeRequest* request,
  faabric::faasmgrpc::RetransmitStreamRangeResponse* response)
{
    auto entries = world.snapshotOutboxForStream(
      request->streamid(), request->fromseq(), request->toseq());
    int replayed = 0;
    int replayFailed = 0;
    for (const OutboxEntry& e : entries) {
        if (world.replayOutboxEntry(e)) {
            ++replayed;
        } else {
            ++replayFailed;
        }
    }
    SPDLOG_INFO(
      "[GRPC RETRANSMIT] app {} stream {} requester {} range [{},{}) "
      "candidates={} replayed={} failed={}",
      request->appid(),
      request->streamid(),
      request->requesterserviceid(),
      request->fromseq(),
      request->toseq(),
      entries.size(),
      replayed,
      replayFailed);
    response->set_status(replayFailed == 0 ? 0 : 1);
    return ::grpc::Status::OK;
}

// Forwarding proxy (grace window)
void GrpcServiceImpl::installForward(const std::string& newEndpoint)
{
    {
        std::scoped_lock lock(forwardMx);
        if (forwardEntry.has_value()) {
            emitForwardSummaryLocked(*forwardEntry);
        }
        forwardRetired = false;
        forwardRetiredEndpoint.clear();
        ForwardEntry entry;
        entry.newEndpoint = newEndpoint;
        entry.expiresAt =
          std::chrono::steady_clock::now() +
          std::chrono::milliseconds(FORWARDING_GRACE_WINDOW_MS);
        forwardEntry = std::move(entry);
        SPDLOG_INFO("[GRPC FORWARD] Installed forward for app {} serviceId {} -> {} "
                    "(grace_ms={})",
                    world.getAppId(),
                    world.getServiceId(),
                    newEndpoint,
                    FORWARDING_GRACE_WINDOW_MS);
    }
    forwardExpiryCv.notify_all();
}

bool GrpcServiceImpl::lookupForward(std::string* out, int32_t callId)
{
    std::scoped_lock lock(forwardMx);
    if (!forwardEntry.has_value()) {
        return false;
    }
    if (std::chrono::steady_clock::now() > forwardEntry->expiresAt) {
        emitForwardExpiredLocked(*forwardEntry);
        forwardEntry.reset();
        return false;
    }
    *out = forwardEntry->newEndpoint;
    if (callId >= 0) {
        if (forwardEntry->firstCallId < 0) {
            forwardEntry->firstCallId = callId;
        }
        forwardEntry->lastCallId = callId;
        forwardEntry->forwardedCalls++;
        forwardEntry->uniqueCallIds.insert(callId);
    }
    return true;
}

void GrpcServiceImpl::forwardExpiryLoop()
{
    std::unique_lock<std::mutex> lk(forwardMx);
    while (!forwardExpiryStop.load(std::memory_order_acquire)) {
        auto wakeAt = std::chrono::steady_clock::now() +
                      std::chrono::minutes(5);
        if (forwardEntry.has_value()) {
            wakeAt = forwardEntry->expiresAt;
        }
        forwardExpiryCv.wait_until(lk, wakeAt);
        if (forwardExpiryStop.load(std::memory_order_acquire)) {
            break;
        }
        if (forwardEntry.has_value() &&
            std::chrono::steady_clock::now() >= forwardEntry->expiresAt) {
            emitForwardExpiredLocked(*forwardEntry);
            forwardEntry.reset();
        }
    }
}

void GrpcServiceImpl::emitForwardExpiredLocked(const ForwardEntry& entry)
{
    forwardRetired = true;
    forwardRetiredEndpoint = entry.newEndpoint;

    SPDLOG_INFO(
      "[GRPC FORWARD] Forward expired app {} serviceId {} endpoint={} "
      "grace_ms={} forwarded_calls={} unique_call_ids={} — shutting down "
      "proxy",
      world.getAppId(),
      world.getServiceId(),
      entry.newEndpoint,
      FORWARDING_GRACE_WINDOW_MS,
      entry.forwardedCalls,
      entry.uniqueCallIds.size());
    emitForwardSummaryLocked(entry);
}

bool GrpcServiceImpl::shouldProbePlannerLocked()
{
    const auto now = std::chrono::steady_clock::now();
    if (lastPlannerProbeAt.time_since_epoch().count() == 0 ||
        (now - lastPlannerProbeAt) >=
          std::chrono::milliseconds(PLANNER_PROBE_MIN_INTERVAL_MS)) {
        lastPlannerProbeAt = now;
        ++plannerProbesPerformed;
        return true;
    }
    ++plannerProbesSuppressed;
    return false;
}

void GrpcServiceImpl::emitForwardSummaryLocked(const ForwardEntry& entry)
{
    if (entry.forwardedCalls == 0) {
        return;
    }
    SPDLOG_INFO(
      "[GRPC FORWARD] Summary: app {} serviceId {} endpoint={} forwarded_calls={} "
      "unique_call_ids={} first_call_id={} last_call_id={}",
      world.getAppId(),
      world.getServiceId(),
      entry.newEndpoint,
      entry.forwardedCalls,
      entry.uniqueCallIds.size(),
      entry.firstCallId,
      entry.lastCallId);
}

}
