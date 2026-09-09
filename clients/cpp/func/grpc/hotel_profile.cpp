// Service 2 of the three-service hotel pipeline. Implements the "profile" service:
// serviceId 0 opens a single long-lived bidi stream to this serviceId and, for each
// hotel ID in a search result, sends a ProfileRequest and receives a ProfileResponse back
// COMP70073


#include "hotel_codec.h"
#include "hotel_profile_data.h"
#include "hotel_evt.h"
#include "hotel_payload.h"

#include <faasm/faasm.h>
#include <faasm/host_interface.h>
#include <faasgrpc/grpc.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace {

constexpr int      PREQ_BUF         = 32;
constexpr int      PRESP_BUF        = 256;
constexpr uint32_t SHUTDOWN_HOTEL_ID = 0xFFFFFFFFu;
constexpr int PROFILE_MIGRATE_EVERY = 1600;

const hotel::data::ProfileEntry* lookup(uint32_t id)
{
    if (id == 0 || id > hotel::data::kProfileCount) return nullptr;
    return &hotel::data::kProfileTable[id - 1];
}

hotel::codec::ProfileResponse buildResponse(uint32_t id)
{
    const auto* e = lookup(id);
    if (!e) return hotel::codec::ProfileResponse{0, "", "", 0.0, 0.0, 0.0};
    return hotel::codec::ProfileResponse{
        e->id, std::string(e->name), std::string(e->address),
        e->lat, e->lon, e->rate
    };
}

} 

static int32_t s_streamId  = 0;
static int32_t s_peerServiceId  = -1;
static int32_t s_processed = 0;

static void runProfileEntry(int /*unused*/);
extern "C" int hotelProfileMain();
static void runProfileEntry(int /*unused*/) { hotelProfileMain(); }

extern "C" int hotelProfileMain()
{
    hotel::evt::lifecycleStart(2);
    const std::size_t payloadBytes = hotel::targetPayloadBytes();
    std::printf("[profile] serviceId 2 starting, profile table has %zu entries "
                "payloadBytes=%zu\n",
                hotel::data::kProfileCount, payloadBytes);

    const std::size_t reqCap = std::max<std::size_t>(PREQ_BUF, payloadBytes + 64);
    std::vector<uint8_t> reqBuf(reqCap);

    if (s_streamId == 0) {
        s_streamId = grpc_bidi_stream_recv_open(&s_peerServiceId);
        if (s_streamId <= 0) {
            std::printf("[profile] failed to recv stream open (id=%d)\n", s_streamId);
            return 1;
        }
        std::printf("[profile] stream opened: streamId=%d peerServiceId=%d\n",
                    s_streamId, s_peerServiceId);
    } else {
        std::printf("[profile] resumed after migration: streamId=%d processed=%d\n",
                    s_streamId, s_processed);
    }

    while (true) {
        int64_t seq      = 0;
        int32_t isClose  = 0;
        int32_t n = grpc_bidi_stream_recv(
          s_streamId, reqBuf.data(), static_cast<int32_t>(reqBuf.size()),
          &seq, &isClose);

        if (isClose || n < 0) {
            std::printf("[profile] stream closed by peer after %d items\n", s_processed);
            break;
        }

        hotel::codec::ProfileRequest preq;
        try {
            std::vector<uint8_t> rv(reqBuf.begin(), reqBuf.begin() + n);
            preq = hotel::codec::decodeProfileRequest(rv);
        } catch (const std::exception& e) {
            std::printf("[profile] decode error: %s\n", e.what());
            uint8_t empty[1] = {0};
            grpc_bidi_stream_send(s_streamId, empty, 0);
            continue;
        }

        if (preq.hotelId == SHUTDOWN_HOTEL_ID) {
            std::printf("[profile] shutdown received after %d items\n", s_processed);
            hotel::codec::ProfileResponse presp{SHUTDOWN_HOTEL_ID, "", "", 0.0, 0.0, 0.0};
            auto bytes = hotel::codec::encode(presp);
            grpc_bidi_stream_send(s_streamId, bytes.data(),
                                  static_cast<int32_t>(bytes.size()));
            break;
        }

        auto presp = buildResponse(preq.hotelId);
        auto bytes = hotel::codec::encode(presp);
        hotel::codec::appendPadding(bytes, payloadBytes);
        grpc_bidi_stream_send(s_streamId, bytes.data(),
                              static_cast<int32_t>(bytes.size()));

        ++s_processed;

        if (s_processed % PROFILE_MIGRATE_EVERY == 0) {
            hotel::evt::lifecycleMigrateEnter(2);
            grpc_migration_point(reinterpret_cast<int32_t>(runProfileEntry), 0);
            hotel::evt::lifecycleMigrateExit(2);
        }

        if (s_processed % 200 == 0) {
            std::printf("[profile] processed %d items\n", s_processed);
        }
    }

    hotel::evt::lifecycleStop(2);

    char outBuf[64];
    int outLen = std::snprintf(outBuf, sizeof(outBuf),
                               "ok:%d|processed=%d", s_processed, s_processed);
    faasmSetOutput(outBuf, outLen);
    s_streamId = 0; s_peerServiceId = -1; s_processed = 0;
    return 0;
}
