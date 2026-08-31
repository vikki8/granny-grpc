#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/queue.h>
#include <faasgrpc/GrpcEnvelope.h>
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Forward-declare the generated protobuf message types so the fast-path
// handler can take them by reference without pulling the heavyweight
// faasm_grpc.grpc.pb.h into this header (kept lean on purpose).
namespace faabric::faasmgrpc {
class GrpcRequest;
class GrpcResponse;
}

namespace faabric::grpc {

// Bounded-size limits for back-pressure. 
constexpr std::size_t OUTBOX_MAX_ENTRIES = 20000;
constexpr int OUTBOX_ADD_BLOCK_TIMEOUT_MS = 5000;

constexpr std::size_t PENDING_REQUESTS_MAX = 20000;
constexpr std::size_t PENDING_RESPONSES_MAX = 20000;
constexpr std::size_t STREAM_QUEUE_MAX = 20000;

constexpr int CHANNEL_TOTAL_DEADLINE_MS = 30000;

constexpr int MAX_FORWARD_HOPS = 2;

// Optimisation 1: co-located fast path.
constexpr bool GRPC_FAST_PATH_ENABLED = true;

// Optimisation 2: failure-aware retry.
constexpr bool GRPC_FAILURE_AWARE_RETRY_ENABLED = true;
constexpr const char* GRPC_MIGRATION_REDIRECT_MARKER = "grpc-faasm-migrating";

constexpr bool GRPC_HOST_UTIL_ENABLED = true;
constexpr int GRPC_HOST_UTIL_SAMPLE_MS = 200;

void startHostUtilSampler();

class GrpcService;

struct UnaryRequest
{
    int32_t callId;
    int32_t sourceServiceId;
    std::string method;
    std::vector<uint8_t> payload;
};

struct UnaryResponse
{
    int32_t callId;
    int32_t status;
    std::string error;
    std::vector<uint8_t> payload;
};

struct OutboxEntry
{
    int32_t destServiceId = 0;
    std::string method;
    std::vector<uint8_t> requestBytes;
    faasgrpc::GrpcEnvelope envelope;
    int32_t streamId = 0;
    int64_t seqNum = 0;
};

// Bidi streams are tunnelled over the existing unary CallUnary RPC. Each
// logical stream message becomes a CallUnary call with these 3 special method name
constexpr const char* BIDI_OPEN_METHOD = "bidi_open";
constexpr const char* BIDI_SEND_METHOD = "bidi_send";
constexpr const char* BIDI_CLOSE_METHOD = "bidi_close";

inline bool isBidiMethod(const std::string& method)
{
    return method == BIDI_OPEN_METHOD || method == BIDI_SEND_METHOD ||
           method == BIDI_CLOSE_METHOD;
}

inline const char* bidiKindTag(const std::string& method)
{
    return isBidiMethod(method) ? "STREAM" : "UNARY";
}

constexpr int32_t kNoAckCallId = 0;

struct StreamCursor
{
    int32_t streamId = 0;
    int32_t peerServiceId = -1;
    bool isClient = false;
    int64_t sendSeqNum = 1;     
    int64_t lastReceivedSeq = 0; 
    bool halfClosedLocal = false;
    bool halfClosedRemote = false;
};


struct StreamInboundItem
{
    int32_t streamId = 0;
    int64_t seqNum = 0;
    std::vector<uint8_t> payload;
    bool isClose = false;  
    int32_t callId = 0;     
};

// Notification of a freshly opened inbound stream.
struct StreamOpenNotification
{
    int32_t streamId = 0;
    int32_t peerServiceId = -1;
};

class GrpcWorld
{
  public:
    GrpcWorld(int32_t appIdIn, int32_t serviceIdIn);

    ~GrpcWorld();

    void create();

    bool destroy();

    std::shared_ptr<::grpc::Channel> getOrCreateChannel(int32_t destServiceId);

    std::vector<uint8_t> callUnary(int32_t destServiceId,
                                   const std::string& method,
                                   const std::vector<uint8_t>& requestBytes,
                                   int32_t streamId = 0,
                                   int64_t seqNum = 0,
                                   int timeoutMs = 30000);

    UnaryRequest recvRequest(int timeoutMs = 30000);

    void sendResponse(int32_t callId, const std::vector<uint8_t>& responseBytes);

    std::shared_ptr<std::promise<UnaryResponse>> enqueueRequest(
      const UnaryRequest& request);

    int32_t getAppId() const { return appId; }

    int32_t getServiceId() const { return serviceId; }

    const std::string& getThisHost() const { return thisHost; }

    int32_t getListenPort() const { return listenPort; }

    const std::string& getDedupeUuid() const { return dedupeUUID; }

    int32_t getMigrationEpoch() const { return migrationEpoch; }

    // live migration phases
    void preparePhase(int32_t newEpoch);

    faabric::GrpcMigrationMetadata transferPhase();

    void commitPhase(const faabric::GrpcMigrationMetadata& meta);

    bool isMigrating() const { return migratingOut.load(); }

    int32_t openBidiStream(int32_t destServiceId);

    int32_t recvBidiStreamOpen(int32_t* peerServiceIdOut, int timeoutMs = 60000);

    void bidiStreamSend(int32_t streamId, const std::vector<uint8_t>& data);

    std::vector<uint8_t> bidiStreamRecv(int32_t streamId,
                                        int64_t* seqOut,
                                        bool* isCloseOut,
                                        int timeoutMs = 60000);

    void bidiStreamHalfClose(int32_t streamId);

    void registerInboundStream(int32_t streamId, int32_t peerServiceId);

    std::shared_ptr<std::promise<UnaryResponse>> enqueueStreamItem(
      int32_t streamId,
      int64_t seqNum,
      std::vector<uint8_t> payload,
      int32_t callId,
      bool isClose);
    
      // co-located fast path.
      bool localDeliveryReady() const
    {
        return serviceReady.load() && !migratingOut.load();
    }

    ::grpc::Status handleInboundLocally(
      const faabric::faasmgrpc::GrpcRequest& request,
      const faasgrpc::GrpcEnvelope& env,
      faabric::faasmgrpc::GrpcResponse* response);

  private:
    int32_t allocateCallId();

    void outboxAdd(int32_t callId, OutboxEntry entry);

    void outboxAck(int32_t callId);

    int32_t appId;
    int32_t serviceId;
    std::string thisHost;
    int32_t listenPort = 0;

    std::string dedupeUUID;
    int32_t migrationEpoch = 0;

    std::unique_ptr<::grpc::Server> server;
    std::unique_ptr<GrpcService> service;

    std::map<int, std::shared_ptr<::grpc::Channel>> channelCache;
    std::mutex channelCacheMx;
    std::mutex worldMx;

    std::atomic<int32_t> callIdCounter{ 0 };

    std::atomic<bool> migratingOut{ false };

    std::atomic<bool> serviceReady{ false };

    faabric::util::Queue<UnaryRequest> pendingRequests;
    std::atomic<std::size_t> pendingRequestsSize{ 0 };

    std::map<int32_t, std::shared_ptr<std::promise<UnaryResponse>>>
      pendingResponses;
    std::mutex pendingResponsesMx;

    std::map<int32_t, OutboxEntry> outbox;
    std::mutex outboxMx;
    std::condition_variable outboxNotFullCv;
    std::atomic<std::uint64_t> outboxBackpressureWaits{ 0 };
    std::atomic<std::uint64_t> outboxBackpressureTimeouts{ 0 };

  public:
    std::vector<OutboxEntry> snapshotOutboxForStream(int32_t streamId,
                                                     int64_t fromSeq,
                                                     int64_t toSeq);

    bool replayOutboxEntry(const OutboxEntry& entry);

  private:

    std::atomic<int32_t> streamIdCounter{ 0 };

    int32_t allocateStreamId();

    std::mutex streamsMx;
    std::map<int32_t, StreamCursor> streamCursors;

    std::map<int32_t,
             std::shared_ptr<faabric::util::Queue<StreamInboundItem>>>
      streamInboundQueues;

    std::map<int32_t, std::vector<StreamInboundItem>> pendingPreOpenItems;

    faabric::util::Queue<StreamOpenNotification> pendingStreamOpens;

    std::mutex deferredMx_;
    std::condition_variable deferredCv_;
    std::atomic<bool> deferredStop_{ false };
    std::vector<std::thread> deferredThreads_;

    bool doRetransmitRequest(int32_t streamId, int32_t peerServiceId, int64_t fromSeq);

    void buildRequest(faabric::faasmgrpc::GrpcRequest& request,
                      int32_t destServiceId,
                      int32_t callId,
                      const std::string& method,
                      int32_t streamId,
                      int64_t seqNum,
                      const std::vector<uint8_t>& payload) const;
};
}
