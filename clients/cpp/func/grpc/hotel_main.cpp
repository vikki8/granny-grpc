// COMP70073
#include <faasm/faasm.h>
#include <faasm/input.h>
#include <faasgrpc/grpc.h>

#include <cstdio>
#include <cstdint>

extern "C" int hotelFrontendMain();
extern "C" int hotelSearchMain();
extern "C" int hotelProfileMain();

int main(int /*argc*/, char* /*argv*/[])
{
    // Dist tests set inputdata to "0", "1", or "2"
    // default "0" if empty (frontend)
    int role = faasm::getIntInput();

    // Initialise the 3-serviceId gRPC world. Every serviceId must call this before any
    // grpc operation. The host extracts the serviceId number from the execution
    // context (msg.grpcServiceId / groupidx), so we don't pass it explicitly.
    if (grpc_init(3) != 0) {
        std::printf("[main] grpc_init failed\n");
        return 1;
    }

    std::printf("[main] role=%d\n", role);
    switch (role) {
        case 0: return hotelFrontendMain();
        case 1: return hotelSearchMain();
        case 2: return hotelProfileMain();
        default:
            std::printf("[main] unknown role: %d\n", role);
            return 1;
    }
}
