set(FAASM_USER grpc)

# gRPC host API: libfaasgrpc/faasgrpc/grpc.h (parallel to libfaasm/faasm/*.h).
get_filename_component(
    LIBFAASGRPC_ROOT
    "${CMAKE_CURRENT_LIST_DIR}/../../libfaasgrpc"
    ABSOLUTE
)
get_filename_component(
    LIBFAASM_FUNC_INCLUDE_DIR
    "${CMAKE_CURRENT_LIST_DIR}/../../libfaasm"
    ABSOLUTE
)

function(grpc_func exec_name dir_path)
    faasm_func(${exec_name} ${dir_path})
    target_include_directories(${exec_name} PRIVATE
        "${LIBFAASM_FUNC_INCLUDE_DIR}"
        "${LIBFAASGRPC_ROOT}"
    )
    if (CMAKE_SYSTEM_NAME STREQUAL "WASI")
        target_sources(${exec_name} PRIVATE
            "${LIBFAASGRPC_ROOT}/grpc.cpp")
    endif ()
    set(ALL_GRPC_FUNCS ${ALL_GRPC_FUNCS} ${exec_name} PARENT_SCOPE)
endfunction()

grpc_func(hotel_main hotel_main.cpp)
target_sources(hotel_main PRIVATE
    hotel_frontend.cpp
    hotel_search.cpp
    hotel_profile.cpp
)
target_include_directories(hotel_main PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}"
)

add_custom_target(grpc_all_funcs DEPENDS ${ALL_GRPC_FUNCS})
