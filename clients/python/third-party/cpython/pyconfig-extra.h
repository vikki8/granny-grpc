/*
 * NOTE: The following definitions are copied into place 
 * _in addition_ to the rest of the file which is auto-generated.
 *
 * They are here to explicitly switch off or override certain 
 * auto-detected features, or to avoid compilation bugs.
 */

#ifndef Py_PYCONFIG_EXTRAS_H
#define Py_PYCONFIG_EXTRAS_H 

// Switch off some features we know we don't need
#undef HAVE_MKNOD
#undef HAVE_DEVICE_MACROS 

#undef HAVE_EPOLL
#undef HAVE_EPOLL_CREATE1
#undef HAVE_LINUX_VM_SOCKETS_H
#undef HAVE_SOCKETPAIR
#undef HAVE_UTIMENSAT
#undef HAVE_SIGACTION

// 32-bit
#ifndef CONFIG_32
#define CONFIG_32
#endif

#endif

