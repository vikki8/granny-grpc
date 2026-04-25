## Hacked CBLAS

These sources are downloaded from https://www.netlib.org/clapack/cblas.tgz and
are not included by default.

On a standard machine a "proper" BLAS implementation would provide this
interface. Because we're operating in the WebAssembly world, we need to hack
together something with the f2c'd files. 

Only some of the routines are wrapped properly in their `cblas_` form. Most of
the time this is just a case of passing a pointer to the f2c'd version.

