#ifndef __wasilibc___header_sys_resource_h
#define __wasilibc___header_sys_resource_h

#include <__struct_rusage.h>
#include <__struct_rlimit.h>

#define RUSAGE_SELF 1
#define RUSAGE_CHILDREN 2

#define RLIMIT_CPU     0
#define RLIMIT_FSIZE   1
#define RLIMIT_DATA    2
#define RLIMIT_STACK   3
#define RLIMIT_CORE    4
#ifndef RLIMIT_RSS
#define RLIMIT_RSS     5
#define RLIMIT_NPROC   6
#define RLIMIT_NOFILE  7
#define RLIMIT_MEMLOCK 8
#define RLIMIT_AS      9
#endif

#ifdef __cplusplus
extern "C" {
#endif

int getrlimit (int resource, struct rlimit *);
int setrlimit (int resource, const struct rlimit *);

int getrusage(int who, struct rusage *usage);

int getpriority (int which, id_t who);
int setpriority (int which, id_t who, int prio);

#ifdef __cplusplus
}
#endif

#endif
