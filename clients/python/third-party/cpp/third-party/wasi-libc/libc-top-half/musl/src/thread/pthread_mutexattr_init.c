#include "pthread_impl.h"

int __pthread_mutexattr_init(pthread_mutexattr_t *a)
{
	*a = (pthread_mutexattr_t){0};
	return 0;
}

#ifndef __faasm_use_own_threads
weak_alias(__pthread_mutexattr_init, pthread_mutexattr_init);
#endif
