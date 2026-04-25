#ifndef __wasilibc___struct_rlimit_h
#define __wasilibc___struct_rlimit_h

#include <__struct_timeval.h>

typedef unsigned long long rlim_t;

struct rlimit {
    rlim_t rlim_cur;
    rlim_t rlim_max;
};

#endif
