// Service 0 of the three-service hotel pipeline. Drives the workload by issuing
// a sequence of SearchRequests to serviceId 1 (search), then for each returned
// hotel ID issuing a ProfileRequest over a long-lived bidi stream to serviceId 2

#include "hotel_codec.h"
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

constexpr int IN_FLIGHT_TARGET    = 16;
constexpr int TOTAL_SEARCHES      = 500;
constexpr int HOTEL_MIGRATE_EVERY = 50;
constexpr const char* SHUTDOWN_METHOD   = "__shutdown__";
constexpr uint32_t    SHUTDOWN_HOTEL_ID = 0xFFFFFFFFu;

constexpr int SEARCH_RESP_BUF  = 256;
constexpr int PROFILE_RESP_BUF = 512;

constexpr double QUERY_LAT_BASE  = 37.7849;
constexpr double QUERY_LON_BASE  = -122.4094;
constexpr double QUERY_RADIUS_KM = 5.0;

struct Rng {
    uint64_t s;
    explicit Rng(uint64_t seed) : s(seed ? seed : 0xdeadbeefULL) {}
    uint64_t next() {
        s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s;
    }
    double jitter(double r) {
        uint64_t x = next();
        double u = static_cast<double>(x & 0xfffffffffffffULL)
                 / static_cast<double>(1ULL << 52);
        return (u - 0.5) * 2.0 * r;
    }
};

}  // namespace

static void runFrontendEntry(int /*unused*/);
extern "C" int hotelFrontendMain();
static void runFrontendEntry(int /*unused*/) { hotelFrontendMain(); }

extern "C" int hotelFrontendMain()
{
    hotel::evt::lifecycleStart(0);
    const std::size_t payloadBytes = hotel::targetPayloadBytes();
    std::printf("[frontend] serviceId 0 starting, target=%d searches in_flight=%d "
                "payloadBytes=%zu\n",
                TOTAL_SEARCHES, IN_FLIGHT_TARGET, payloadBytes);

    if (grpc_connect(1) != 0) {
        std::printf("[frontend] failed to connect to serviceId 1 (search)\n");
        return 1;
    }
    if (grpc_connect(2) != 0) {
        std::printf("[frontend] failed to connect to serviceId 2 (profile)\n");
        return 1;
    }

    int32_t profileStreamId = grpc_bidi_stream_open(2);
    if (profileStreamId <= 0) {
        std::printf("[frontend] failed to open profile stream (id=%d)\n",
                    profileStreamId);
        return 1;
    }
    std::printf("[frontend] opened profile stream: streamId=%d\n", profileStreamId);

    Rng rng(0xfa01);

    int searchIssued    = 0;
    int searchCompleted = 0;
    int searchFailed    = 0;
    int profileSent     = 0;
    int profileReceived = 0;

    const std::size_t respCap =
      std::max<std::size_t>(SEARCH_RESP_BUF, payloadBytes + 64);
    const std::size_t presCap =
      std::max<std::size_t>(PROFILE_RESP_BUF, payloadBytes + 64);
    std::vector<uint8_t> respBuf(respCap);
    std::vector<uint8_t> presBuf(presCap);

    for (int i = 0; i < TOTAL_SEARCHES; ++i) {
        hotel::codec::SearchRequest sreq{
            QUERY_LAT_BASE + rng.jitter(0.01),
            QUERY_LON_BASE + rng.jitter(0.01),
            QUERY_RADIUS_KM
        };
        auto sreqBytes = hotel::codec::encode(sreq);
        hotel::codec::appendPadding(sreqBytes, payloadBytes);

        const char* method = "nearby";
        const int32_t mLen = static_cast<int32_t>(std::strlen(method));

        int32_t respLen = grpc_call_unary(
          1, method, mLen,
          sreqBytes.data(), static_cast<int32_t>(sreqBytes.size()),
          respBuf.data(),   static_cast<int32_t>(respBuf.size()));

        ++searchIssued;
        if (respLen < 0) {
            ++searchFailed;
            std::printf("[frontend] search %d failed (respLen=%d)\n", i, respLen);
            continue;
        }
        ++searchCompleted;

        hotel::codec::SearchResponse sresp;
        try {
            std::vector<uint8_t> sv(respBuf.begin(), respBuf.begin() + respLen);
            sresp = hotel::codec::decodeSearchResponse(sv);
        } catch (const std::exception& e) {
            std::printf("[frontend] search-resp decode error: %s\n", e.what());
            continue;
        }

        for (uint32_t id : sresp.hotelIds) {
            hotel::codec::ProfileRequest preq{id};
            auto preqBytes = hotel::codec::encode(preq);
            hotel::codec::appendPadding(preqBytes, payloadBytes);

            int32_t sendRc = grpc_bidi_stream_send(
              profileStreamId,
              preqBytes.data(),
              static_cast<int32_t>(preqBytes.size()));
            if (sendRc != 0) {
                std::printf("[frontend] stream send failed for hotel %u (rc=%d)\n",
                            id, sendRc);
                goto run_done;
            }
            ++profileSent;

            {
                int64_t seq     = 0;
                int32_t isClose = 0;
                int32_t n = grpc_bidi_stream_recv(
                  profileStreamId, presBuf.data(),
                  static_cast<int32_t>(presBuf.size()),
                  &seq, &isClose);
                if (isClose) {
                    std::printf("[frontend] stream closed by peer mid-run!\n");
                    goto run_done;
                }
                ++profileReceived;

                if (n > 0) {
                    try {
                        std::vector<uint8_t> pv(presBuf.begin(),
                                                presBuf.begin() + n);
                        auto presp = hotel::codec::decodeProfileResponse(pv);
                        if (presp.hotelId != 0 && presp.hotelId != id) {
                            std::printf("[frontend] !! profile id mismatch: "
                                        "expected %u got %u\n", id, presp.hotelId);
                        }
                    } catch (const std::exception& e) {
                        std::printf("[frontend] profile-resp decode error: %s\n",
                                    e.what());
                    }
                }
            }
        }

        if ((i + 1) % HOTEL_MIGRATE_EVERY == 0) {
            hotel::evt::lifecycleMigrateEnter(0);
            grpc_migration_point(reinterpret_cast<int32_t>(runFrontendEntry), 0);
            hotel::evt::lifecycleMigrateExit(0);
        }

        if ((i + 1) % 100 == 0) {
            std::printf("[frontend] completed %d/%d searches\n",
                        i + 1, TOTAL_SEARCHES);
        }
    }

run_done:
    {
        uint8_t resp[8] = {0};
        (void)grpc_call_unary(1, SHUTDOWN_METHOD,
                              static_cast<int32_t>(std::strlen(SHUTDOWN_METHOD)),
                              nullptr, 0, resp, static_cast<int32_t>(sizeof(resp)));
        std::printf("[frontend] sent shutdown to serviceId 1\n");
    }

    {
        hotel::codec::ProfileRequest shutdownReq{SHUTDOWN_HOTEL_ID};
        auto shutdownBytes = hotel::codec::encode(shutdownReq);
        (void)grpc_bidi_stream_send(profileStreamId, shutdownBytes.data(),
                                    static_cast<int32_t>(shutdownBytes.size()));
        uint8_t ackBuf[PROFILE_RESP_BUF] = {0};
        int64_t ackSeq  = 0;
        int32_t isClose = 0;
        (void)grpc_bidi_stream_recv(profileStreamId, ackBuf,
                                    static_cast<int32_t>(sizeof(ackBuf)),
                                    &ackSeq, &isClose);
        std::printf("[frontend] serviceId 2 shutdown ack received\n");
    }
    grpc_bidi_stream_half_close(profileStreamId);

    hotel::evt::lifecycleStop(0);

    std::printf("[ledger] searchIssued=%d searchCompleted=%d searchFailed=%d "
                "profileSent=%d profileReceived=%d\n",
                searchIssued, searchCompleted, searchFailed,
                profileSent, profileReceived);

    char outBuf[256];
    int outLen = std::snprintf(outBuf, sizeof(outBuf),
      "ok:%d|searched=%d|failed=%d|profileSent=%d|profileRecv=%d",
      searchCompleted, TOTAL_SEARCHES, searchFailed,
      profileSent, profileReceived);
    faasmSetOutput(outBuf, outLen);
    return 0;
}
