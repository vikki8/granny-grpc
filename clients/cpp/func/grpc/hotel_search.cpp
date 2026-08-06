// Service 1 of the three-service hotel pipeline. Implements the search service:
// receives a unary `nearby` request (lat, lon, radiusKm) from serviceId 0 and
// returns the IDs of all hotels within the given radius, computed via
// haversine distance over an embedded geo index.

#include "hotel_codec.h"
#include "hotel_geo_data.h"
#include "hotel_evt.h"
#include "hotel_payload.h"

#include <faasm/faasm.h>
#include <faasm/host_interface.h>
#include <faasgrpc/grpc.h>

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace {

constexpr int HOTEL_MIGRATE_EVERY  = 50;
constexpr int METHOD_BUF           = 64;
constexpr int REQ_BUF              = 64;
constexpr const char* SHUTDOWN_METHOD = "__shutdown__";
constexpr const char* NEARBY_METHOD   = "nearby";

hotel::codec::SearchResponse searchNearby(const hotel::codec::SearchRequest& req)
{
    using namespace hotel::data;
    using hotel::codec::haversineKm;
    hotel::codec::SearchResponse resp;
    resp.hotelIds.reserve(16);
    for (std::size_t i = 0; i < kGeoCount; ++i) {
        const auto& g = kGeoTable[i];
        if (haversineKm(req.lat, req.lon, g.lat, g.lon) <= req.radiusKm) {
            resp.hotelIds.push_back(g.id);
            if (resp.hotelIds.size() >= 32) break;
        }
    }
    return resp;
}

} 

static int32_t s_processed = 0;

static void runSearchEntry(int /*unused*/);
extern "C" int hotelSearchMain();
static void runSearchEntry(int /*unused*/) { hotelSearchMain(); }

extern "C" int hotelSearchMain()
{
    hotel::evt::lifecycleStart(1);
    const std::size_t payloadBytes = hotel::targetPayloadBytes();
    std::printf("[search] serviceId 1 starting, geo table has %zu entries "
                "payloadBytes=%zu\n",
                hotel::data::kGeoCount, payloadBytes);

    const std::size_t reqCap = std::max<std::size_t>(REQ_BUF, payloadBytes + 64);
    std::vector<uint8_t> reqBuf(reqCap);

    while (true) {
        char    methodBuf[METHOD_BUF] = {0};
        int32_t callId     = 0;
        int32_t sourceServiceId = 0;

        int32_t payloadLen = grpc_recv_request(
          methodBuf,     static_cast<int32_t>(sizeof(methodBuf) - 1),
          reqBuf.data(), static_cast<int32_t>(reqBuf.size()),
          &callId, &sourceServiceId);

        if (payloadLen < 0) {
            std::printf("[search] grpc_recv_request returned %d, exiting\n", payloadLen);
            break;
        }

        if (std::strcmp(methodBuf, SHUTDOWN_METHOD) == 0) {
            std::printf("[search] shutdown received after %d requests\n", s_processed);
            uint8_t ok[4] = {1, 0, 0, 0};
            grpc_send_response(callId, ok, static_cast<int32_t>(sizeof(ok)));
            break;
        }

        if (std::strcmp(methodBuf, NEARBY_METHOD) != 0) {
            std::printf("[search] unexpected method: %s\n", methodBuf);
            uint8_t empty[4] = {0};
            grpc_send_response(callId, empty, static_cast<int32_t>(sizeof(empty)));
            continue;
        }

        std::vector<uint8_t> payload(reqBuf.begin(), reqBuf.begin() + payloadLen);
        hotel::codec::SearchRequest sreq;
        try {
            sreq = hotel::codec::decodeSearchRequest(payload);
        } catch (const std::exception& e) {
            std::printf("[search] decode error: %s\n", e.what());
            uint8_t empty[4] = {0};
            grpc_send_response(callId, empty, static_cast<int32_t>(sizeof(empty)));
            continue;
        }

        auto sresp = searchNearby(sreq);
        auto bytes = hotel::codec::encode(sresp);
        hotel::codec::appendPadding(bytes, payloadBytes);
        grpc_send_response(callId, bytes.data(), static_cast<int32_t>(bytes.size()));

        ++s_processed;

        if (s_processed % HOTEL_MIGRATE_EVERY == 0) {
            hotel::evt::lifecycleMigrateEnter(1);
            grpc_migration_point(reinterpret_cast<int32_t>(runSearchEntry), 0);
            hotel::evt::lifecycleMigrateExit(1);
        }

        if (s_processed % 200 == 0) {
            std::printf("[search] processed %d requests\n", s_processed);
        }
    }

    hotel::evt::lifecycleStop(1);

    char outBuf[64];
    int outLen = std::snprintf(outBuf, sizeof(outBuf),
                               "ok:%d|processed=%d", s_processed, s_processed);
    faasmSetOutput(outBuf, outLen);
    s_processed = 0;
    return 0;
}
