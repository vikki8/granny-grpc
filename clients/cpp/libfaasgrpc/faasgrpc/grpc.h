// Faasm WASM gRPC host API (C).
#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

int32_t grpc_init(int32_t worldSize);

int32_t grpc_connect(int32_t destServiceId);

void grpc_migration_point(int32_t entryFuncPtr, int32_t entryArg);

int32_t grpc_call_unary(int32_t destServiceId,
                        const char* method,
                        int32_t methodLen,
                        const uint8_t* reqData,
                        int32_t reqLen,
                        uint8_t* respBuf,
                        int32_t respBufLen);

int32_t grpc_recv_request(char* methodBuf,
                          int32_t methodBufLen,
                          uint8_t* reqBuf,
                          int32_t reqBufLen,
                          int32_t* callIdOut,
                          int32_t* sourceServiceIdOut);

void grpc_send_response(int32_t callId, const uint8_t* respData, int32_t respLen);

int32_t grpc_bidi_stream_open(int32_t destServiceId);

int32_t grpc_bidi_stream_recv_open(int32_t* peerServiceIdOut);

int32_t grpc_bidi_stream_send(int32_t streamId,
                              const uint8_t* data,
                              int32_t len);

int32_t grpc_bidi_stream_recv(int32_t streamId,
                              uint8_t* buf,
                              int32_t bufLen,
                              int64_t* seqOut,
                              int32_t* isCloseOut);

int32_t grpc_bidi_stream_half_close(int32_t streamId);

#ifdef __cplusplus
}
#endif
