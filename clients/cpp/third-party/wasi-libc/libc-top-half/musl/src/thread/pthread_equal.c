#include <pthread.h>
#include <threads.h>

static int __pthread_equal(pthread_t a, pthread_t b)
{
	return a==b;
}

#ifndef __faasm_use_own_threads
weak_alias(__pthread_equal, pthread_equal);
weak_alias(__pthread_equal, thrd_equal);
#endif
