#ifndef LIBFFI_TARGET_H
#define LIBFFI_TARGET_H

#ifndef LIBFFI_H
#error "Please do not include ffitarget.h directly into your source.  Use ffi.h instead."
#endif

typedef unsigned long ffi_arg;
typedef signed long ffi_sarg;

typedef enum ffi_abi {
  FFI_FIRST_ABI = 0,
  FFI_WASM32, 
  FFI_WASM32_EMSCRIPTEN, 
  FFI_LAST_ABI,
  FFI_DEFAULT_ABI = FFI_WASM32
} ffi_abi;

#define FFI_CLOSURES 1
#define FFI_GO_CLOSURES 0
#define FFI_TRAMPOLINE_SIZE 24
#define FFI_NATIVE_RAW_API 0

#endif
