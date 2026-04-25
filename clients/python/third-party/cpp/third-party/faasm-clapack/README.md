# CLAPACK and BLAS Support for Faasm

Without a Fortran-wasm compiler we can't use standard LAPACK and BLAS libraries,
so instead we use [CLAPACK](http://www.netlib.org/clapack/). 

Sources:

- [CLAPACK](http://www.netlib.org/clapack/)
- [LAPACK](https://github.com/Reference-LAPACK/lapack)

Certain Makefiles rely on [Faasm](https://github.com/faasm/faasm) being checked
out at `/usr/local/code/faasm`. 

Note that CBLAS sources aren't part of the default CLAPACK so they have been
downloaded from https://www.netlib.org/clapack/cblas.tgz and included in this
repo.

## Building

See the [Faasm toolchain](https://github.com/faasm/faasm-toolchain/) where this
repository is built.

