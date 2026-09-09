// Shared helper for the payload-size sweep. Every serviceId reads its target
// on-wire payload size from the function input so the SAME WASM binary can be
// swept across sizes without recompilation
// COMP70073

#pragma once

#include "hotel_codec.h"

#include <faasm/input.h>

#include <cstddef>
#include <cstdlib>
#include <cstring>

namespace hotel {

inline std::size_t targetPayloadBytes()
{
    static long s_cached = -1; 
    if (s_cached >= 0) {
        return static_cast<std::size_t>(s_cached);
    }

    std::size_t resolved = 0;
    const char* in = faasm::getStringInput("0");
    if (in != nullptr) {
        const char* colon = std::strchr(in, ':');
        if (colon != nullptr) {
            long v = std::strtol(colon + 1, nullptr, 10);
            if (v > 0) {
                resolved =
                  (static_cast<std::size_t>(v) > codec::kMaxPayloadBytes)
                    ? codec::kMaxPayloadBytes
                    : static_cast<std::size_t>(v);
            }
        }
    }

    s_cached = static_cast<long>(resolved);
    return resolved;
}

}  
