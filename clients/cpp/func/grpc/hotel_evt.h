// Shared structured stderr events for all three hotel services.
// COMP70073

#pragma once

#include <cstdint>
#include <cstdio>
#include <ctime>

namespace hotel::evt {

// Clock

inline uint64_t nowNs()
{
    struct timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL
         + static_cast<uint64_t>(ts.tv_nsec);
}

// Event emission 

inline void emit(int serviceId, int seq, const char* kind, const char* type, uint64_t ts_ns)
{
    std::fprintf(stderr,
                 "[evt] serviceId=%d seq=%d kind=%s type=%s ts_ns=%llu\n",
                 serviceId, seq, kind, type,
                 static_cast<unsigned long long>(ts_ns));
}

// Convenience wrappers 

inline void lifecycleStart(int serviceId)
{
    emit(serviceId, 0, "lifecycle", "start", nowNs());
}
inline void lifecycleStop(int serviceId)
{
    emit(serviceId, 0, "lifecycle", "stop", nowNs());
}
inline void lifecycleMigrateEnter(int serviceId)
{
    emit(serviceId, 0, "lifecycle", "migrate_enter", nowNs());
}
inline void lifecycleMigrateExit(int serviceId)
{
    emit(serviceId, 0, "lifecycle", "migrate_exit", nowNs());
}

}
