CC=$(WASM_CC)
CXX=$(WASM_CXX)
CPP=$(WASM_CPP)
NM=$(WASM_NM)
LD=$(WASM_CC)

# Change for static/ dynamic libs
# Default to static
LIBEXT ?= .a

# Different link flags for different types of modules
ifeq ($(LIBEXT), .so)
    # Shared libs
    _FLAGS=$(WASM_CFLAGS_NO_QUOTES) \
        -D__wasi__ \
        -nostdlib -nostdlib++ \
        -fPIC \
        --target=wasm32-unknown-emscripten \
        -Xlinker --no-entry \
        -I$(TOPDIR)/INCLUDE
    LDFLAGS=$(WASM_SHARED_FLAGS_NO_QUOTES)
    CFLAGS=$(_FLAGS)
else
    _FLAGS=$(WASM_CFLAGS_NO_QUOTES)
    LDFLAGS=$(WASM_LDFLAGS_NO_QUOTES)
    CFLAGS=$(_FLAGS) -O3 -I$(TOPDIR)/INCLUDE
endif

SHELL = /bin/sh

PLAT = _WASM

LOADER    = $(CC)
LOADOPTS  =
NOOPT     = -O0 -I$(TOPDIR)/INCLUDE
DRVCFLAGS = $(CFLAGS)
F2CCFLAGS = $(CFLAGS)

TIMER    = INT_CPU_TIME

AR       = $(WASM_AR)
ARCH     = $(AR)
ARCHFLAGS= cr
RANLIB   = $(WASM_RANLIB)

CBLASLIB     = ../cblas$(PLAT)$(LIBEXT)
BLASLIB      = ../../blas$(PLAT)$(LIBEXT)

XBLASLIB     =

LAPACKLIB    = lapack$(PLAT)$(LIBEXT)
F2CLIB       = ../../F2CLIBS/libf2c$(LIBEXT)

# Not used in Faasm
TMGLIB       = junk_tmglib$(LIBEXT)
EIGSRCLIB    = junk_eigsrc$(LIBEXT)
LINSRCLIB    = junk_linsrc$(LIBEXT)
