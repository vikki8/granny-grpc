//COMP70073

#pragma once

#include <faabric/grpc/GrpcWorld.h>
#include <faabric/proto/faabric.pb.h>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

namespace faabric::grpc {

class GrpcWorldRegistry
{
  public:
    GrpcWorldRegistry() = default;

    GrpcWorld& getOrInitialiseWorld(faabric::Message& msg);

    GrpcWorld& getWorld(int32_t appId, int32_t serviceId);

    std::shared_ptr<GrpcWorld> tryGetWorld(int32_t appId, int32_t serviceId);

    bool worldExists(int32_t appId, int32_t serviceId);

    void clear();

  private:
    uint64_t makeWorldKey(int32_t appId, int32_t serviceId) const;

    std::map<uint64_t, std::shared_ptr<GrpcWorld>> worldMap;
    std::mutex mx;
};

GrpcWorldRegistry& getGrpcWorldRegistry();
}
