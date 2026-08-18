#include <faabric/grpc/GrpcWorldRegistry.h>

#include <faabric/planner/PlannerClient.h>
#include <faabric/util/logging.h>

#include <google/protobuf/message.h>
#include <stdexcept>

namespace faabric::grpc {

GrpcWorldRegistry& getGrpcWorldRegistry()
{
    static GrpcWorldRegistry registry;
    return registry;
}

uint64_t GrpcWorldRegistry::makeWorldKey(int32_t appId, int32_t serviceId) const
{
    return (static_cast<uint64_t>(appId) << 32) |
           static_cast<uint32_t>(serviceId);
}

GrpcWorld& GrpcWorldRegistry::getOrInitialiseWorld(faabric::Message& msg)
{
    int32_t serviceId = msg.grpcserviceid();
    if (serviceId < 0) {
        serviceId = msg.groupidx();
        msg.set_grpcserviceid(serviceId);
    }

    int32_t worldSize = msg.grpcworldsize();
    if (worldSize <= 0) {
        worldSize = 1;
        msg.set_grpcworldsize(worldSize);
    }

    uint64_t worldKey = makeWorldKey(msg.appid(), serviceId);

    std::shared_ptr<GrpcWorld> retired;
    {
        std::scoped_lock lock(mx);
        auto existing = worldMap.find(worldKey);
        if (existing != worldMap.end() && existing->second != nullptr &&
            existing->second->isMigrating()) {
            retired = existing->second;
            worldMap.erase(existing);
        }
    }

    if (retired != nullptr) {
        SPDLOG_INFO("Retiring migrated-out GrpcWorld app {} serviceId {}: this host "
                    "is its migration destination again",
                    msg.appid(),
                    serviceId);
        retired->destroy();
        retired.reset();
    }

    std::scoped_lock lock(mx);
    if (!worldMap.contains(worldKey)) {
        auto world = std::make_shared<GrpcWorld>(msg.appid(), serviceId);

        bool restored = false;
        try {
            auto blob = faabric::planner::getPlannerClient()
                          .popGrpcMigrationBlob(msg.appid(), serviceId);
            if (!blob.empty()) {
                faabric::GrpcMigrationMetadata meta;
                if (meta.ParseFromArray(
                      blob.data(), static_cast<int>(blob.size()))) {
                    SPDLOG_INFO(
                      "Restoring GrpcWorld app {} serviceId {} from migration blob",
                      msg.appid(),
                      serviceId);
                    world->commitPhase(meta);
                    restored = true;
                } else {
                    SPDLOG_ERROR(
                      "Failed to parse gRPC migration metadata for {}/{}",
                      msg.appid(),
                      serviceId);
                }
            }
        } catch (const std::exception& ex) {
            SPDLOG_WARN("Could not probe migration blob: {}", ex.what());
        }

        if (!restored) {
            world->create();
        }
        worldMap[worldKey] = world;
    }

    return *worldMap.at(worldKey);
}

GrpcWorld& GrpcWorldRegistry::getWorld(int32_t appId, int32_t serviceId)
{
    uint64_t worldKey = makeWorldKey(appId, serviceId);
    std::scoped_lock lock(mx);
    auto it = worldMap.find(worldKey);
    if (it == worldMap.end()) {
        throw std::runtime_error("Requested GrpcWorld does not exist");
    }

    return *it->second;
}

std::shared_ptr<GrpcWorld> GrpcWorldRegistry::tryGetWorld(int32_t appId,
                                                          int32_t serviceId)
{
    uint64_t worldKey = makeWorldKey(appId, serviceId);
    std::scoped_lock lock(mx);
    auto it = worldMap.find(worldKey);
    if (it == worldMap.end()) {
        return nullptr;
    }
    return it->second;
}

bool GrpcWorldRegistry::worldExists(int32_t appId, int32_t serviceId)
{
    uint64_t worldKey = makeWorldKey(appId, serviceId);
    std::scoped_lock lock(mx);
    return worldMap.contains(worldKey);
}

void GrpcWorldRegistry::clear()
{
    std::scoped_lock lock(mx);
    for (auto& [worldKey, world] : worldMap) {
        if (world != nullptr) {
            world->destroy();
        }
    }
    worldMap.clear();
}
}
