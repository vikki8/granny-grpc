# gRPC host API implementation (headers under faasgrpc/).
add_library(faasgrpc_obj OBJECT grpc.cpp)
target_include_directories(faasgrpc_obj PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
)
target_include_directories(faasgrpc_obj PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}/../libfaasm"
)
set_target_properties(faasgrpc_obj PROPERTIES POSITION_INDEPENDENT_CODE ON)
