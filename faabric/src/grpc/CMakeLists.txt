faabric_lib(grpc
    GrpcServiceImpl.cpp
    GrpcWorld.cpp
    GrpcWorldRegistry.cpp
)

target_include_directories(grpc PUBLIC ${FAASM_LIBFAASGRPC_INCLUDE})
target_include_directories(grpc_obj PUBLIC ${FAASM_LIBFAASGRPC_INCLUDE})

target_link_libraries(grpc_obj PRIVATE faabric::proto)

target_link_libraries(grpc PRIVATE
    faabric::planner
    faabric::proto
    Boost::headers
)
target_link_libraries(grpc_obj PRIVATE Boost::headers)
