#include <faabric/executor/ExecutorContext.h>
#include <faabric/grpc/GrpcWorldRegistry.h>
#include <faabric/util/bytes.h>
#include <wamr/WAMRWasmModule.h>
#include <wamr/native.h>

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace wasm {

static faabric::grpc::GrpcWorld& getExecutingGrpcWorld(int32_t worldSizeHint = 0)
{
    auto execCtx = faabric::executor::ExecutorContext::get();
    auto& msg = execCtx->getMsg();

    msg.set_isgrpc(true);

    if (msg.grpcserviceid() == 0 && msg.groupidx() > 0) {
        msg.set_grpcserviceid(msg.groupidx());
    }

    if (msg.grpcworldsize() <= 0) {
        msg.set_grpcworldsize(worldSizeHint > 0 ? worldSizeHint : 1);
    }

    auto& world = faabric::grpc::getGrpcWorldRegistry().getOrInitialiseWorld(msg);
    msg.set_grpcport(world.getListenPort());

    return world;
}

static int32_t __faasm_grpc_init_wrapper(wasm_exec_env_t execEnv,
                                         int32_t worldSize)
{
    auto* module = getExecutingWAMRModule();
    try {
        auto& world = getExecutingGrpcWorld(worldSize);
        world.create();
        return 0;
    } catch (std::exception& ex) {
        module->doThrowException(ex);
        return -1;
    }
}

static int32_t __faasm_grpc_connect_wrapper(wasm_exec_env_t execEnv,
                                            int32_t destServiceId)
{
    auto* module = getExecutingWAMRModule();
    try {
        auto& world = getExecutingGrpcWorld();
        world.getOrCreateChannel(destServiceId);
        return 0;
    } catch (std::exception& ex) {
        module->doThrowException(ex);
        return -1;
    }
}

static int32_t __faasm_grpc_call_unary_wrapper(wasm_exec_env_t execEnv,
                                                int32_t destServiceId,
                                                char* method,
                                                int32_t methodLen,
                                                uint8_t* reqData,
                                                int32_t reqLen,
                                                uint8_t* respBuf,
                                                int32_t respBufLen)
{
    auto* module = getExecutingWAMRModule();
    try {
        if (reqLen < 0 || respBufLen < 0 || methodLen < 0) {
            throw std::runtime_error("Invalid gRPC buffer lengths");
        }

        auto& world = getExecutingGrpcWorld();

        std::string methodName =
          methodLen > 0 ? std::string(method, methodLen) : std::string(method);

        std::vector<uint8_t> requestBytes;
        if (reqLen > 0) {
            requestBytes.assign(reqData, reqData + reqLen);
        }
        std::vector<uint8_t> responseBytes =
          world.callUnary(destServiceId, methodName, requestBytes);

        if (responseBytes.size() > static_cast<size_t>(respBufLen)) {
            return -1;
        }

        return faabric::util::safeCopyToBuffer(
          responseBytes, respBuf, respBufLen);
    } catch (std::exception& ex) {
        module->doThrowException(ex);
        return -1;
    }
}

static int32_t __faasm_grpc_recv_request_wrapper(wasm_exec_env_t execEnv,
                                                  char* methodBuf,
                                                  int32_t methodBufLen,
                                                  uint8_t* reqBuf,
                                                  int32_t reqBufLen,
                                                  int32_t* callIdOut,
                                                  int32_t* sourceServiceIdOut)
{
    auto* module = getExecutingWAMRModule();
    try {
        if (methodBufLen <= 0 || reqBufLen < 0) {
            throw std::runtime_error("Invalid gRPC receive buffer lengths");
        }

        auto& world = getExecutingGrpcWorld();
        auto request = world.recvRequest();

        int32_t methodCopyLen = std::min<int32_t>(
          static_cast<int32_t>(request.method.size()), methodBufLen - 1);
        if (methodCopyLen > 0) {
            std::memcpy(methodBuf, request.method.data(), methodCopyLen);
        }
        methodBuf[methodCopyLen] = '\0';

        if (request.payload.size() > static_cast<size_t>(reqBufLen)) {
            return -1;
        }

        int copied = faabric::util::safeCopyToBuffer(request.payload, reqBuf, reqBufLen);
        *callIdOut = request.callId;
        *sourceServiceIdOut = request.sourceServiceId;

        return copied;
    } catch (std::exception& ex) {
        module->doThrowException(ex);
        return -1;
    }
}

static void __faasm_grpc_send_response_wrapper(wasm_exec_env_t execEnv,
                                               int32_t callId,
                                               uint8_t* respData,
                                               int32_t respLen)
{
    auto* module = getExecutingWAMRModule();
    try {
        if (respLen < 0) {
            throw std::runtime_error("Invalid gRPC response length");
        }

        auto& world = getExecutingGrpcWorld();
        std::vector<uint8_t> responseBytes;
        if (respLen > 0) {
            responseBytes.assign(respData, respData + respLen);
        }
        world.sendResponse(callId, responseBytes);
    } catch (std::exception& ex) {
        module->doThrowException(ex);
    }
}

static int32_t __faasm_grpc_bidi_stream_open_wrapper(wasm_exec_env_t execEnv,
                                                      int32_t destServiceId)
{
    auto* module = getExecutingWAMRModule();
    try {
        auto& world = getExecutingGrpcWorld();
        return world.openBidiStream(destServiceId);
    } catch (std::exception& ex) {
        module->doThrowException(ex);
        return -1;
    }
}

static int32_t __faasm_grpc_bidi_stream_recv_open_wrapper(
  wasm_exec_env_t execEnv,
  int32_t* peerServiceIdOut)
{
    auto* module = getExecutingWAMRModule();
    try {
        auto& world = getExecutingGrpcWorld();
        int32_t peer = -1;
        int32_t streamId = world.recvBidiStreamOpen(&peer);
        if (peerServiceIdOut != nullptr) {
            *peerServiceIdOut = peer;
        }
        return streamId;
    } catch (std::exception& ex) {
        module->doThrowException(ex);
        return -1;
    }
}

static int32_t __faasm_grpc_bidi_stream_send_wrapper(wasm_exec_env_t execEnv,
                                                      int32_t streamId,
                                                      uint8_t* data,
                                                      int32_t len)
{
    auto* module = getExecutingWAMRModule();
    try {
        if (len < 0) {
            throw std::runtime_error("Invalid bidi send length");
        }
        auto& world = getExecutingGrpcWorld();
        std::vector<uint8_t> payload;
        if (len > 0) {
            payload.assign(data, data + len);
        }
        world.bidiStreamSend(streamId, payload);
        return 0;
    } catch (std::exception& ex) {
        module->doThrowException(ex);
        return -1;
    }
}

static int32_t __faasm_grpc_bidi_stream_recv_wrapper(wasm_exec_env_t execEnv,
                                                      int32_t streamId,
                                                      uint8_t* buf,
                                                      int32_t bufLen,
                                                      int64_t* seqOut,
                                                      int32_t* isCloseOut)
{
    auto* module = getExecutingWAMRModule();
    try {
        if (bufLen < 0) {
            throw std::runtime_error("Invalid bidi recv buffer length");
        }
        auto& world = getExecutingGrpcWorld();
        int64_t seq = 0;
        bool isClose = false;
        std::vector<uint8_t> payload =
          world.bidiStreamRecv(streamId, &seq, &isClose);
        if (seqOut != nullptr) {
            *seqOut = seq;
        }
        if (isCloseOut != nullptr) {
            *isCloseOut = isClose ? 1 : 0;
        }
        if (payload.size() > static_cast<size_t>(bufLen)) {
            return -1;
        }
        return faabric::util::safeCopyToBuffer(payload, buf, bufLen);
    } catch (std::exception& ex) {
        module->doThrowException(ex);
        return -1;
    }
}

static int32_t __faasm_grpc_bidi_stream_half_close_wrapper(
  wasm_exec_env_t execEnv,
  int32_t streamId)
{
    auto* module = getExecutingWAMRModule();
    try {
        auto& world = getExecutingGrpcWorld();
        world.bidiStreamHalfClose(streamId);
        return 0;
    } catch (std::exception& ex) {
        module->doThrowException(ex);
        return -1;
    }
}

static NativeSymbol ns[] = {
    REG_NATIVE_FUNC(__faasm_grpc_init, "(i)i"),
    REG_NATIVE_FUNC(__faasm_grpc_connect, "(i)i"),
    REG_NATIVE_FUNC(__faasm_grpc_call_unary, "(i$i*~*~)i"),
    REG_NATIVE_FUNC(__faasm_grpc_recv_request, "(*~*~**)i"),
    REG_NATIVE_FUNC(__faasm_grpc_send_response, "(i*~)"),
    REG_NATIVE_FUNC(__faasm_grpc_bidi_stream_open, "(i)i"),
    REG_NATIVE_FUNC(__faasm_grpc_bidi_stream_recv_open, "(*)i"),
    REG_NATIVE_FUNC(__faasm_grpc_bidi_stream_send, "(i*~)i"),
    REG_NATIVE_FUNC(__faasm_grpc_bidi_stream_recv, "(i*~**)i"),
    REG_NATIVE_FUNC(__faasm_grpc_bidi_stream_half_close, "(i)i"),
};

uint32_t getFaasmGrpcApi(NativeSymbol** nativeSymbols)
{
    *nativeSymbols = ns;
    return sizeof(ns) / sizeof(NativeSymbol);
}
}
