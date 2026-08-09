faabric_lib(executor
    Executor.cpp
    ExecutorContext.cpp
    ExecutorFactory.cpp
    ExecutorTask.cpp
)

# Same as faabric/src/grpc: GrpcWorld.h includes <faasgrpc/GrpcEnvelope.h>
target_include_directories(executor_obj PUBLIC ${FAASM_LIBFAASGRPC_INCLUDE})

# FIXME: do we need all these deps here?
target_link_libraries(executor PRIVATE
    faabric::scheduling_util
    faabric::snapshot
    faabric::state
    faabric::redis
    faabric::grpc
)
