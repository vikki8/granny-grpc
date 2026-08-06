cmake_minimum_required(VERSION 3.8)
project(libfaasm)

# Public gRPC host API: ../libfaasgrpc/faasgrpc/grpc.h; implementation in ../libfaasgrpc/grpc.cpp.
get_filename_component(LIBFAASGRPC_ROOT "${CMAKE_CURRENT_LIST_DIR}/../libfaasgrpc" ABSOLUTE)
add_subdirectory("${LIBFAASGRPC_ROOT}" libfaasgrpc_build)

# ------ Faasm C/C++ library ------ #

set(CMAKE_CXX_STANDARD 17)

set(PUBLIC_HEADERS
    faasm/core.h
    faasm/host_interface.h
    faasm/host_interface_test.h
    faasm/array.h
    faasm/compare.h
    faasm/faasm.h
    faasm/files.h
    faasm/input.h
    faasm/memory.h
    faasm/migrate.h
    faasm/print.h
    faasm/random.h
    faasm/shared_mem.h
    faasm/state.h
    faasm/time.h
)

set(LIB_FILES
    compare.cpp
    core.cpp
    files.cpp
    input.cpp
    print.cpp
    random.cpp
    state.cpp
    time.cpp
    zygote.cpp
)

if (CMAKE_SYSTEM_NAME STREQUAL "WASI")
    message(STATUS "Libfaasm WebAssembly build")

    add_library(faasm STATIC ${LIB_FILES} $<TARGET_OBJECTS:faasgrpc_obj>)
    set_target_properties(faasm PROPERTIES PUBLIC_HEADER "${PUBLIC_HEADERS}")

    # Install in WASM sysroot
    install(
        TARGETS faasm
        ARCHIVE DESTINATION $ENV{FAASM_WASM_LIB_INSTALL_DIR}
        LIBRARY DESTINATION $ENV{FAASM_WASM_LIB_INSTALL_DIR}
        PUBLIC_HEADER DESTINATION $ENV{FAASM_WASM_HEADER_INSTALL_DIR}/faasm
    )

    install(
        FILES ${LIBFAASGRPC_ROOT}/faasgrpc/grpc.h
        DESTINATION $ENV{FAASM_WASM_HEADER_INSTALL_DIR}/faasgrpc
    )

    # Add allowed import files
    install(
        FILES libfaasm.imports
        DESTINATION $ENV{FAASM_WASM_LIB_INSTALL_DIR}
        RENAME libfaasm.imports
    )
else ()
    message(STATUS "Libfaasm native build")

    if(BUILD_SHARED_LIBS)
        add_library(faasm SHARED ${LIB_FILES} $<TARGET_OBJECTS:faasgrpc_obj>)
    else()
        add_library(faasm STATIC ${LIB_FILES} $<TARGET_OBJECTS:faasgrpc_obj>)
    endif()

    set_target_properties(faasm PROPERTIES PUBLIC_HEADER "${PUBLIC_HEADERS}")

    install(
        TARGETS faasm
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        PUBLIC_HEADER DESTINATION include/faasm
    )

    install(FILES ${LIBFAASGRPC_ROOT}/faasgrpc/grpc.h
                  ${LIBFAASGRPC_ROOT}/faasgrpc/GrpcEnvelope.h
            DESTINATION include/faasgrpc)
endif ()

target_include_directories(faasm PUBLIC
    ${CMAKE_CURRENT_LIST_DIR}
    ${LIBFAASGRPC_ROOT}
)
