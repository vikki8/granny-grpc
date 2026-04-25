#include "pthread_impl.h"

int __pthread_mutexattr_destroy(pthread_mutexattr_t *a)
{
	return 0;
}

#ifndef __faasm_use_own_threads
weak_alias(__pthread_mutexattr_destroy, pthread_mutexattr_destroy);
#endif
