# ----------------------------------------------
# Protobuf/gRPC generation
# ----------------------------------------------

set(PB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/proto/faabric.pb.h")

protobuf_generate_cpp(PB_SRC PB_HEADER faabric.proto)

# Generate gRPC stubs for the Faasm gRPC service definitions
set(FAASM_GRPC_PROTO "${CMAKE_CURRENT_LIST_DIR}/faasm_grpc.proto")
set(FAASM_GRPC_PB_SRC "${CMAKE_CURRENT_BINARY_DIR}/faasm_grpc.pb.cc")
set(FAASM_GRPC_PB_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faasm_grpc.pb.h")
set(FAASM_GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/faasm_grpc.grpc.pb.cc")
set(FAASM_GRPC_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faasm_grpc.grpc.pb.h")

add_custom_command(
    OUTPUT
    "${FAASM_GRPC_PB_SRC}"
    "${FAASM_GRPC_PB_HEADER}"
    "${FAASM_GRPC_SRC}"
    "${FAASM_GRPC_HEADER}"
    DEPENDS "${FAASM_GRPC_PROTO}"
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS
    --proto_path=${CMAKE_CURRENT_LIST_DIR}
    --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
    --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
    ${FAASM_GRPC_PROTO}
)

add_custom_target(
    faasm_grpc_proto_generated
    DEPENDS
    "${FAASM_GRPC_PB_SRC}"
    "${FAASM_GRPC_PB_HEADER}"
    "${FAASM_GRPC_SRC}"
    "${FAASM_GRPC_HEADER}"
)

# Copy the generated headers into place
add_custom_command(
    OUTPUT "${PB_HEADER_COPIED}"
    DEPENDS "${PB_HEADER}"
    COMMAND ${CMAKE_COMMAND}
    ARGS -E copy ${PB_HEADER} ${FAABRIC_INCLUDE_DIR}/faabric/proto/
)

add_custom_target(
    faabric_pbh_copied
    DEPENDS ${PB_HEADER_COPIED}
)

# ----------------------------------------------
# Faabric wrapper library
# ----------------------------------------------

faabric_lib(proto
    ${PB_SRC}
    ${FAASM_GRPC_PB_SRC}
    ${FAASM_GRPC_SRC}
)

add_dependencies(faabric_common_dependencies
    faabric_pbh_copied
    faasm_grpc_proto_generated
)

target_include_directories(proto_deps INTERFACE ${CMAKE_CURRENT_BINARY_DIR})
