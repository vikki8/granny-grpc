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
    int role = faasm::getIntInput();

    // Initialise the 3-serviceId gRPC world
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
