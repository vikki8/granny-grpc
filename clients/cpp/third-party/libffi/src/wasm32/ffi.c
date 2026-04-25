#include <ffi.h>
#include <ffi_common.h>
#include <stdint.h>
#include <stdlib.h>

ffi_status FFI_HIDDEN
ffi_prep_cif_machdep(ffi_cif *cif)
{
    return FFI_OK;
}

// Note:
// ffi_call will be implemented by the runtime, so we exclude it here
