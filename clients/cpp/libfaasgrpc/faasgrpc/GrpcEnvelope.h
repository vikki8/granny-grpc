// gRPC metadata envelope 
// COMP70073

#pragma once

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <optional>
#include <sstream>
#include <string>

namespace faasgrpc
{

struct GrpcEnvelope
{
    std::string dedupeUUID;
    int32_t migrationEpoch = 0;
    int32_t callId = 0;
    int32_t forwardHops = 0;
};

constexpr const char* ENVELOPE_METADATA_KEY = "granny-envelope";

// Format: dedupeUUID|migrationEpoch|callId|forwardHops 
inline std::string serializeEnvelope(const GrpcEnvelope& env)
{
    return env.dedupeUUID + '|' + std::to_string(env.migrationEpoch) + '|' +
           std::to_string(env.callId) + '|' +
           std::to_string(env.forwardHops);
}

inline std::optional<GrpcEnvelope> parseEnvelope(const std::string& raw)
{
    std::istringstream in(raw);
    std::string uuidPart;
    std::string epochPart;
    std::string callPart;
    std::string hopsPart;
    if (!std::getline(in, uuidPart, '|') ||
        !std::getline(in, epochPart, '|') || !std::getline(in, callPart, '|')) {
        return std::nullopt;
    }
    std::getline(in, hopsPart);
    try {
        GrpcEnvelope env;
        env.dedupeUUID = std::move(uuidPart);
        env.migrationEpoch = static_cast<int32_t>(std::stol(epochPart));
        env.callId = static_cast<int32_t>(std::stol(callPart));
        env.forwardHops =
          hopsPart.empty() ? 0 : static_cast<int32_t>(std::stol(hopsPart));
        return env;
    } catch (...) {
        return std::nullopt;
    }
}

inline void addEnvelopeToClientContext(::grpc::ClientContext& ctx,
                                       const GrpcEnvelope& env)
{
    ctx.AddMetadata(ENVELOPE_METADATA_KEY, serializeEnvelope(env));
}

inline std::optional<GrpcEnvelope> extractEnvelopeFromServerContext(
  const ::grpc::ServerContext* ctx)
{
    if (ctx == nullptr) {
        return std::nullopt;
    }
    const auto& md = ctx->client_metadata();
    for (auto it = md.begin(); it != md.end(); ++it) {
        std::string key(it->first.data(), it->first.length());
        if (key != ENVELOPE_METADATA_KEY) {
            continue;
        }
        std::string val(it->second.data(), it->second.length());
        return parseEnvelope(val);
    }
    return std::nullopt;
}

}
