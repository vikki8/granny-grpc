#include "pthread_impl.h"

int __pthread_cond_wait(pthread_cond_t *restrict c, pthread_mutex_t *restrict m)
{
	return pthread_cond_timedwait(c, m, 0);
}

#ifndef __faasm_use_own_threads
weak_alias(__pthread_cond_wait, pthread_cond_wait);
#endif
