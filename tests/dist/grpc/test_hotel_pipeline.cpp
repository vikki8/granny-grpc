#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/planner/PlannerClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/batch.h>
#include <fmt/format.h>

#include <algorithm>
#include <cstdlib>
#include <string>

namespace tests {

constexpr int HOTEL_WORLD_SIZE   = 3;
constexpr int HOTEL_TOTAL_SEARCH = 500; // must match TOTAL_SEARCHES in hotel_frontend.cpp

// 0,32768,65536 payload
static std::shared_ptr<faabric::BatchExecuteRequest> makeHotelReq(
  int payloadBytes = 65536)
{
    auto req =
      faabric::util::batchExecFactory("grpc", "hotel_main", HOTEL_WORLD_SIZE);

    for (int i = 0; i < HOTEL_WORLD_SIZE; i++) {
        auto* msg = req->mutable_messages(i);
        msg->set_isgrpc(true);
        msg->set_grpcserviceid(i);
        msg->set_grpcworldsize(HOTEL_WORLD_SIZE);
        msg->set_groupidx(i);
        msg->set_inputdata(fmt::format("{}:{}", i, payloadBytes));
    }
    return req;
}

// The hotel pipeline drives 2000 searches with ~32 profile lookups each.
static std::shared_ptr<faabric::BatchExecuteRequestStatus>
waitForHotelResults(faabric::planner::PlannerClient& cli,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    constexpr int POLL_SLEEP_MS = 3000;
    constexpr int MAX_RETRIES   = 50; // 50 × 3 s = 150 s ceiling
    auto batchResults = cli.getBatchResults(req);
    for (int attempt = 0; attempt < MAX_RETRIES; ++attempt) {
        if (batchResults->messageresults_size() == HOTEL_WORLD_SIZE) {
            return batchResults;
        }
        SPDLOG_INFO("[HOTEL TEST] Waiting for results: {}/{} (attempt {}/{})",
                    batchResults->messageresults_size(),
                    HOTEL_WORLD_SIZE, attempt + 1, MAX_RETRIES);
        SLEEP_MS(POLL_SLEEP_MS);
        batchResults = cli.getBatchResults(req);
    }
    throw std::runtime_error("Timed-out waiting for hotel batch results");
}

static int parseFrontendOk(const std::string& out)
{
    if (out.rfind("ok:", 0) != 0) return -1;
    size_t bar = out.find('|');
    return std::stoi(out.substr(3, bar == std::string::npos ? std::string::npos : bar - 3));
}

static long long parseField(const std::string& s, const std::string& key)
{
    std::string needle = "|" + key + "=";
    size_t pos = s.find(needle);
    if (pos == std::string::npos) return -1;
    pos += needle.size();
    size_t end = s.find('|', pos);
    return std::stoll(s.substr(pos, end == std::string::npos ? end : end - pos));
}

// TEST 1: baseline, no migration 
TEST_CASE_METHOD(DistTestsFixture,
                 "Hotel pipeline completes without migration",
                 "[hotel][hotel-baseline]")
{
    updatePlannerPolicy("bin-pack");
    faasmConf.wasmVm = "wamr";

    setLocalRemoteSlots(
      HOTEL_WORLD_SIZE, HOTEL_WORLD_SIZE, 0, 0, HOTEL_WORLD_SIZE, 0);

    auto req = makeHotelReq();

    plannerCli.callFunctions(req);

    // The frontend does 2000 searches, each with up to ~8 profile round-trips.
    auto batchResults = waitForHotelResults(plannerCli, req);
    REQUIRE(batchResults->messageresults_size() == HOTEL_WORLD_SIZE);

    std::string service0Out, service1Out, service2Out;
    for (const auto& msg : batchResults->messageresults()) {
        REQUIRE(msg.returnvalue() == 0);
        if (msg.grpcserviceid() == 0) service0Out = msg.outputdata();
        if (msg.grpcserviceid() == 1) service1Out = msg.outputdata();
        if (msg.grpcserviceid() == 2) service2Out = msg.outputdata();
    }

    int searchCompleted = parseFrontendOk(service0Out);
    REQUIRE(searchCompleted == HOTEL_TOTAL_SEARCH);
    REQUIRE(parseField(service0Out, "failed")      == 0);
    long long sent = parseField(service0Out, "profileSent");
    long long recv = parseField(service0Out, "profileRecv");
    REQUIRE(sent >= 0);
    REQUIRE(sent == recv);

    REQUIRE(parseField(service1Out, "processed") > 0);
    REQUIRE(parseField(service2Out, "processed") > 0);

    SPDLOG_INFO("[HOTEL TEST] Baseline complete. "
                "searchCompleted={} profileSent={} profileRecv={} "
                "searchProcessed={} profileProcessed={}",
                searchCompleted, sent, recv,
                parseField(service1Out, "processed"),
                parseField(service2Out, "processed"));
}

// TEST 2: service 1 (search) migrates mid-run 
TEST_CASE_METHOD(DistTestsFixture,
                 "Hotel pipeline: service 1 (search) survives live migration",
                 "[hotel][hotel-migrate-search]")
{
    updatePlannerPolicy("bin-pack");
    faasmConf.wasmVm = "wamr";

    setLocalRemoteSlots(
      HOTEL_WORLD_SIZE, HOTEL_WORLD_SIZE, 0, 0, HOTEL_WORLD_SIZE, 0);

    auto req = makeHotelReq();

    const std::string masterIp = getDistTestMasterIp();
    const std::string workerIp = getDistTestWorkerIp();

    // Initial placement: service 1 (search) starts on master.
    auto preloadDec = std::make_shared<batch_scheduler::SchedulingDecision>(
      req->appid(), req->groupid());
    preloadDec->addMessage(workerIp, 0, 0, 0); // frontend on worker
    preloadDec->addMessage(masterIp, 0, 0, 1); // search on master — will migrate
    preloadDec->addMessage(workerIp, 0, 0, 2); // profile on worker
    plannerCli.preloadSchedulingDecision(preloadDec);

    plannerCli.callFunctions(req);

    auto batchResults = waitForHotelResults(plannerCli, req);
    REQUIRE(batchResults->messageresults_size() == HOTEL_WORLD_SIZE);

    std::string service0Out, service1Out, service2Out;
    std::string service0Host, service1InitHost, service1FinalHost;
    service1InitHost = masterIp;

    for (const auto& msg : batchResults->messageresults()) {
        REQUIRE(msg.returnvalue() == 0);
        if (msg.grpcserviceid() == 0) {
            service0Out  = msg.outputdata();
            service0Host = msg.executedhost();
        }
        if (msg.grpcserviceid() == 1) {
            service1Out       = msg.outputdata();
            service1FinalHost = msg.executedhost();
        }
        if (msg.grpcserviceid() == 2) service2Out = msg.outputdata();
    }

    int searchCompleted = parseFrontendOk(service0Out);
    REQUIRE(searchCompleted == HOTEL_TOTAL_SEARCH);
    REQUIRE(parseField(service0Out, "failed") == 0);
    long long sent = parseField(service0Out, "profileSent");
    long long recv = parseField(service0Out, "profileRecv");
    REQUIRE(sent == recv);
    REQUIRE(parseField(service1Out, "processed") > 0);
    REQUIRE(parseField(service2Out, "processed") > 0);

    bool service1Migrated = service1FinalHost != service1InitHost;
    SPDLOG_INFO("[HOTEL TEST] Search-service migration complete. "
                "searchCompleted={} profileSent={} profileRecv={} "
                "searchProcessed={} profileProcessed={} "
                "service1: {} -> {} (migrated={})",
                searchCompleted, sent, recv,
                parseField(service1Out, "processed"),
                parseField(service2Out, "processed"),
                service1InitHost, service1FinalHost, service1Migrated);

    std::string finalEndpoint = plannerCli.getGrpcEndpoint(req->appid(), 1);
    REQUIRE(!finalEndpoint.empty());
}

// TEST 3: service 2 (profile) migrates mid-run 
TEST_CASE_METHOD(DistTestsFixture,
                 "Hotel pipeline: service 2 (profile) survives live migration",
                 "[hotel][hotel-migrate-profile]")
{
    updatePlannerPolicy("bin-pack");
    faasmConf.wasmVm = "wamr";

    setLocalRemoteSlots(
      HOTEL_WORLD_SIZE, HOTEL_WORLD_SIZE, 0, 0, HOTEL_WORLD_SIZE, 0);

    auto req = makeHotelReq();

    const std::string masterIp = getDistTestMasterIp();
    const std::string workerIp = getDistTestWorkerIp();

    // Initial placement: service 2 (profile) starts on master, everything else
    auto preloadDec = std::make_shared<batch_scheduler::SchedulingDecision>(
      req->appid(), req->groupid());
    preloadDec->addMessage(workerIp, 0, 0, 0); // frontend on worker
    preloadDec->addMessage(workerIp, 0, 0, 1); // search on worker
    preloadDec->addMessage(masterIp, 0, 0, 2); // profile on master — will migrate
    plannerCli.preloadSchedulingDecision(preloadDec);

    plannerCli.callFunctions(req);

    auto batchResults = waitForHotelResults(plannerCli, req);
    REQUIRE(batchResults->messageresults_size() == HOTEL_WORLD_SIZE);

    std::string service0Out, service1Out, service2Out;
    std::string service2InitHost = masterIp;
    std::string service2FinalHost;

    for (const auto& msg : batchResults->messageresults()) {
        REQUIRE(msg.returnvalue() == 0);
        if (msg.grpcserviceid() == 0) service0Out = msg.outputdata();
        if (msg.grpcserviceid() == 1) service1Out = msg.outputdata();
        if (msg.grpcserviceid() == 2) {
            service2Out       = msg.outputdata();
            service2FinalHost = msg.executedhost();
        }
    }

    int searchCompleted = parseFrontendOk(service0Out);
    REQUIRE(searchCompleted == HOTEL_TOTAL_SEARCH);
    REQUIRE(parseField(service0Out, "failed") == 0);
    long long sent = parseField(service0Out, "profileSent");
    long long recv = parseField(service0Out, "profileRecv");
    REQUIRE(sent == recv);
    REQUIRE(parseField(service1Out, "processed") > 0);
    REQUIRE(parseField(service2Out, "processed") > 0);

    bool service2Migrated = service2FinalHost != service2InitHost;
    SPDLOG_INFO("[HOTEL TEST] Profile-service migration complete. "
                "searchCompleted={} profileSent={} profileRecv={} "
                "searchProcessed={} profileProcessed={} "
                "service2: {} -> {} (migrated={})",
                searchCompleted, sent, recv,
                parseField(service1Out, "processed"),
                parseField(service2Out, "processed"),
                service2InitHost, service2FinalHost, service2Migrated);

    std::string finalEndpoint = plannerCli.getGrpcEndpoint(req->appid(), 2);
    REQUIRE(!finalEndpoint.empty());
}

// ---- TEST 4: perf-adaptive policy 
TEST_CASE_METHOD(DistTestsFixture,
                 "Hotel pipeline: perf-adaptive policy auto-migrates to co-locate",
                 "[hotel-perf-adaptive]")
{
    updatePlannerPolicy("perf-adaptive");
    faasmConf.wasmVm = "wamr";

    setLocalRemoteSlots(
      HOTEL_WORLD_SIZE, HOTEL_WORLD_SIZE, 0, 0, HOTEL_WORLD_SIZE, 0);

    auto req = makeHotelReq(32768);

    const std::string masterIp = getDistTestMasterIp();
    const std::string workerIp = getDistTestWorkerIp();
    const std::string worker2Ip = getDistTestWorker2Ip();

    // Initial placement: search on master, frontend + profile on the worker.
    auto preloadDec = std::make_shared<batch_scheduler::SchedulingDecision>(
      req->appid(), req->groupid());
    preloadDec->addMessage(workerIp, 0, 0, 0); // frontend on worker
    preloadDec->addMessage(masterIp, 0, 0, 1); // search on master — should migrate
    preloadDec->addMessage(workerIp, 0, 0, 2); // profile on worker
    plannerCli.preloadSchedulingDecision(preloadDec);

    plannerCli.callFunctions(req);

    auto batchResults = waitForHotelResults(plannerCli, req);
    REQUIRE(batchResults->messageresults_size() == HOTEL_WORLD_SIZE);

    std::string service0Out, service1Out, service2Out;
    std::string service1FinalHost;
    const std::string service1InitHost = masterIp;

    for (const auto& msg : batchResults->messageresults()) {
        REQUIRE(msg.returnvalue() == 0);
        if (msg.grpcserviceid() == 0) service0Out = msg.outputdata();
        if (msg.grpcserviceid() == 1) {
            service1Out       = msg.outputdata();
            service1FinalHost = msg.executedhost();
        }
        if (msg.grpcserviceid() == 2) service2Out = msg.outputdata();
    }

    int searchCompleted = parseFrontendOk(service0Out);
    REQUIRE(searchCompleted == HOTEL_TOTAL_SEARCH);
    REQUIRE(parseField(service0Out, "failed") == 0);
    long long sent = parseField(service0Out, "profileSent");
    long long recv = parseField(service0Out, "profileRecv");
    REQUIRE(sent == recv);
    REQUIRE(parseField(service1Out, "processed") > 0);
    REQUIRE(parseField(service2Out, "processed") > 0);

    bool service1Migrated = service1FinalHost != service1InitHost;
    SPDLOG_INFO("[HOTEL TEST] perf-adaptive complete. "
                "searchCompleted={} profileSent={} profileRecv={} "
                "searchProcessed={} profileProcessed={} "
                "service1: {} -> {} (migrated={})",
                searchCompleted, sent, recv,
                parseField(service1Out, "processed"),
                parseField(service2Out, "processed"),
                service1InitHost, service1FinalHost, service1Migrated);

    REQUIRE((service1FinalHost == workerIp || service1FinalHost == masterIp ||
             service1FinalHost == worker2Ip));
}

}  
