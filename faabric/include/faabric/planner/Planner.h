#pragma once

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/PlannerState.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/snapshot/SnapshotRegistry.h>

#include <cstdint>
#include <shared_mutex>

namespace faabric::planner {
enum FlushType
{
    NoFlushType = 0,
    Hosts = 1,
    Executors = 2,
    SchedulingState = 3,
};

/* The planner is a standalone component that has a global view of the state
 * of a distributed faabric deployment.
 */
class Planner
{
  public:
    Planner();

    // ----------
    // Planner config
    // ----------

    PlannerConfig getConfig();

    void printConfig() const;

    std::string getPolicy();

    void setPolicy(const std::string& newPolicy);

    // ----------
    // Util public API
    // ----------

    bool reset();

    bool flush(faabric::planner::FlushType flushType);

    // ----------
    // Host membership public API
    // ----------

    std::vector<std::shared_ptr<Host>> getAvailableHosts();

    bool registerHost(const Host& hostIn, bool overwrite);

    // Best effort host removal. Don't fail if we can't
    void removeHost(const Host& hostIn);

    void setGrpcEndpoint(int32_t appId,
                              int32_t serviceId,
                              const std::string& host,
                              int32_t port);

    std::string getGrpcEndpoint(int32_t appId, int32_t serviceId);

    // source stores a serialised GrpcMigrationMetadata blob
    void setGrpcMigrationBlob(int32_t appId,
                              int32_t serviceId,
                              const std::vector<uint8_t>& blob);

    // Returns the blob and atomically removes it from the store
    std::vector<uint8_t> popGrpcMigrationBlob(int32_t appId, int32_t serviceId);

    // ----------
    // Request scheduling public API
    // ----------

    // Setters/getters for individual message results

    void setMessageResult(std::shared_ptr<faabric::Message> msg);

    std::shared_ptr<faabric::Message> getMessageResult(
      std::shared_ptr<faabric::Message> msg);

    // Setter/Getter to bypass the planner's scheduling for a specific app
    void preloadSchedulingDecision(
      int appId,
      std::shared_ptr<batch_scheduler::SchedulingDecision> decision);

    std::shared_ptr<batch_scheduler::SchedulingDecision>
    getPreloadedSchedulingDecision(int32_t appId,
                                   std::shared_ptr<BatchExecuteRequest> ber);

    // Get all the results recorded for one batch
    std::shared_ptr<faabric::BatchExecuteRequestStatus> getBatchResults(
      int32_t appId);

    std::shared_ptr<faabric::batch_scheduler::SchedulingDecision>
    getSchedulingDecision(std::shared_ptr<BatchExecuteRequest> req);

    faabric::batch_scheduler::InFlightReqs getInFlightReqs();

    // Helper method to get the number of migrations that have happened since
    // the planner was last reset
    int getNumMigrations();

    // Helper method to get the next host that will be evicted
    std::set<std::string> getNextEvictedHostIps();

    std::map<int32_t, std::shared_ptr<BatchExecuteRequest>> getEvictedReqs();

    // Main entrypoint to request the execution of batches
    std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> callBatch(
      std::shared_ptr<BatchExecuteRequest> req);

    // ----------
    // API exclusive to SPOT policy mode
    // ----------

    void setNextEvictedVm(const std::set<std::string>& vmIp);

  private:
    // There's a singleton instance of the planner running, but it must allow
    // concurrent requests
    std::shared_mutex plannerMx;

    PlannerState state;
    PlannerConfig config;

    // Snapshot registry to distribute snapshots in THREADS requests
    faabric::snapshot::SnapshotRegistry& snapshotRegistry;

    // ----------
    // Util private API
    // ----------

    void flushHosts();

    void flushExecutors();

    void flushSchedulingState();

    // ----------
    // Host membership private API
    // ----------

    // Check if a host's registration timestamp has expired
    bool isHostExpired(std::shared_ptr<Host> host, long epochTimeMs = 0);

    // ----------
    // Request scheduling private API
    // ----------

    void dispatchSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> decision);
};

Planner& getPlanner();
}
