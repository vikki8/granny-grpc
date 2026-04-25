#ifndef _SYS_SYSCALL_H
#define _SYS_SYSCALL_H

#if defined(__wasilibc_unmodified_upstream) || defined(__faasm) 
#include <bits/syscall.h>
#else
/* The generic syscall funtion is not yet implemented. */
#endif

#endif
