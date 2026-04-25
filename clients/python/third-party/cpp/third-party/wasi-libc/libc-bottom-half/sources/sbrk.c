#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <__macro_PAGESIZE.h>

#ifdef __faasm
    extern void* __sbrk(intptr_t increment);
#endif

// Bare-bones implementation of sbrk.
void *sbrk(intptr_t increment) {
#ifdef __faasm
    // In Faasm we defer to our custom sbrk implementation
    return __sbrk(increment);
#else
    // sbrk(0) returns the current memory size.
    if (increment == 0) {
        // The wasm spec doesn't guarantee that memory.grow of 0 always succeeds.
        return (void *)(__builtin_wasm_memory_size(0) * PAGESIZE);
    }

    // We only support page-size increments.
    if (increment % PAGESIZE != 0) {
        abort();
    }

    // WebAssembly doesn't support shrinking linear memory.
    if (increment < 0) {
        abort();
    }

    uintptr_t old = __builtin_wasm_memory_grow(0, (uintptr_t)increment / PAGESIZE);

    if (old == SIZE_MAX) {
        errno = ENOMEM;
        return (void *)-1;
    }

    return (void *)(old * PAGESIZE);
#endif
}
