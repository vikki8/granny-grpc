#include <faasgrpc/grpc.h>

extern "C"
{
#include "faasm/host_interface.h"
}

int32_t grpc_init(int32_t worldSize)
{
    return __faasm_grpc_init(worldSize);
}

int32_t grpc_connect(int32_t destServiceId)
{
    return __faasm_grpc_connect(destServiceId);
}

int32_t grpc_call_unary(int32_t destServiceId,
                        const char* method,
                        int32_t methodLen,
                        const uint8_t* reqData,
                        int32_t reqLen,
                        uint8_t* respBuf,
                        int32_t respBufLen)
{
    return __faasm_grpc_call_unary(
      destServiceId, method, methodLen, reqData, reqLen, respBuf, respBufLen);
}

int32_t grpc_recv_request(char* methodBuf,
                          int32_t methodBufLen,
                          uint8_t* reqBuf,
                          int32_t reqBufLen,
                          int32_t* callIdOut,
                          int32_t* sourceServiceIdOut)
{
    return __faasm_grpc_recv_request(methodBuf,
                                     methodBufLen,
                                     reqBuf,
                                     reqBufLen,
                                     callIdOut,
                                     sourceServiceIdOut);
}

void grpc_send_response(int32_t callId, const uint8_t* respData, int32_t respLen)
{
    __faasm_grpc_send_response(callId, respData, respLen);
}

void grpc_migration_point(int32_t entryFuncPtr, int32_t entryArg)
{
    __faasm_migrate_point(
      reinterpret_cast<FaasmMigrateEntryPoint>(entryFuncPtr), entryArg);
}

int32_t grpc_bidi_stream_open(int32_t destServiceId)
{
    return __faasm_grpc_bidi_stream_open(destServiceId);
}

int32_t grpc_bidi_stream_recv_open(int32_t* peerServiceIdOut)
{
    return __faasm_grpc_bidi_stream_recv_open(peerServiceIdOut);
}

int32_t grpc_bidi_stream_send(int32_t streamId,
                              const uint8_t* data,
                              int32_t len)
{
    return __faasm_grpc_bidi_stream_send(streamId, data, len);
}

int32_t grpc_bidi_stream_recv(int32_t streamId,
                              uint8_t* buf,
                              int32_t bufLen,
                              int64_t* seqOut,
                              int32_t* isCloseOut)
{
    return __faasm_grpc_bidi_stream_recv(
      streamId, buf, bufLen, seqOut, isCloseOut);
}

int32_t grpc_bidi_stream_half_close(int32_t streamId)
{
    return __faasm_grpc_bidi_stream_half_close(streamId);
}
