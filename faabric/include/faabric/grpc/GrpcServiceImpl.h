#pragma once

#include <grpcpp/grpcpp.h>
#include <faasm_grpc.grpc.pb.h>
#include <faabric/grpc/GrpcWorld.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace faabric {
class GrpcMigrationMetadata;
}

namespace faabric::grpc {

struct CachedUnaryResponse
{
    int32_t status = 0;
    std::string error;
    std::vector<uint8_t> payload;
};

using DedupeKey = std::tuple<int32_t, std::string, int32_t>;

using SenderEpochKey = std::pair<int32_t, std::string>;

constexpr int FORWARDING_GRACE_WINDOW_MS = 500;
constexpr std::size_t DEDUPE_CACHE_MAX_ENTRIES = 50000;
constexpr std::size_t SENDER_EPOCH_MAX_ENTRIES = 10000;
constexpr int PLANNER_PROBE_MIN_INTERVAL_MS = 250;

struct ForwardEntry
{
    std::string newEndpoint;
    std::chrono::steady_clock::time_point expiresAt;
    int32_t forwardedCalls = 0;
    int32_t firstCallId = -1;
    int32_t lastCallId = -1;
    std::set<int32_t> uniqueCallIds;
};

class GrpcServiceImpl final : public faabric::faasmgrpc::FaasmGrpc::Service
{
  public:
    explicit GrpcServiceImpl(GrpcWorld& worldIn);

    ~GrpcServiceImpl();

    ::grpc::Status CallUnary(::grpc::ServerContext* context,
                             const faabric::faasmgrpc::GrpcRequest* request,
                             faabric::faasmgrpc::GrpcResponse* response) override;

    ::grpc::Status handleUnaryRequest(
      const faabric::faasmgrpc::GrpcRequest* request,
      const faasgrpc::GrpcEnvelope& env,
      faabric::faasmgrpc::GrpcResponse* response,
      bool viaFastPath = false);

    ::grpc::Status RetransmitStreamRange(
      ::grpc::ServerContext* context,
      const faabric::faasmgrpc::RetransmitStreamRangeRequest* request,
      faabric::faasmgrpc::RetransmitStreamRangeResponse* response) override;

    // Install a one-hop forward entry so inbound calls are bounced to new endpoint
    void installForward(const std::string& newEndpoint);

    bool hasActiveForward();

    void snapshotMigrationState(faabric::GrpcMigrationMetadata& meta);

    void restoreMigrationState(const faabric::GrpcMigrationMetadata& meta);

    std::size_t dedupeCacheSize() const;

  private:

    void fillResponse(faabric::faasmgrpc::GrpcResponse* response,
                      const faabric::faasmgrpc::GrpcRequest* request,
                      int32_t callId,
                      int32_t status,
                      const std::string& error,
                      const std::vector<uint8_t>& payload,
                      bool servedFromCache) const;


    bool lookupForward(std::string* out, int32_t callId = -1);

    void emitForwardSummaryLocked(const ForwardEntry& entry);

    void recordInboundStatus(::grpc::StatusCode code);
    void recordForwardStatus(::grpc::StatusCode code);

    void emitStatusSummary();

    std::size_t evictDedupeIfOverfullLocked();

    std::size_t evictSenderEpochIfOverfullLocked();

    bool shouldProbePlannerLocked();

    GrpcWorld& world;

    std::mutex rpcMx;

    std::map<SenderEpochKey, int32_t> knownEpochPerSender;
    std::deque<SenderEpochKey> senderEpochInsertOrder;
    std::size_t senderEpochEvictions = 0;

    std::map<DedupeKey, CachedUnaryResponse> dedupeCache;

    std::deque<DedupeKey> dedupeInsertOrder;

    std::size_t dedupeEvictions = 0;

    std::mutex forwardMx;

    std::optional<ForwardEntry> forwardEntry;

    bool forwardRetired = false;
    std::string forwardRetiredEndpoint;
    std::chrono::steady_clock::time_point lastPlannerProbeAt{};
    std::uint64_t plannerProbesPerformed = 0;
    std::uint64_t plannerProbesSuppressed = 0;

    std::thread forwardExpiryThread;
    std::condition_variable forwardExpiryCv;
    std::atomic<bool> forwardExpiryStop{ false };
    void forwardExpiryLoop();

    void emitForwardExpiredLocked(const ForwardEntry& entry);

    static constexpr std::size_t NUM_STATUS_BUCKETS = 17;
    std::array<std::atomic<uint64_t>, NUM_STATUS_BUCKETS> inboundStatusCounts{};
    std::array<std::atomic<uint64_t>, NUM_STATUS_BUCKETS> forwardStatusCounts{};
};
}

