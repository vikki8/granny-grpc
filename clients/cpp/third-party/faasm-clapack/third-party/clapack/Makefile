#
#  Top Level Makefile for LAPACK
#  Version 3.2.1
#  June 2009
#

TOPDIR=$(abspath .)
TOOLCHAIN_ROOT=$(TOPDIR)/../../../..
include $(TOOLCHAIN_ROOT)/Makefile.envs

INSTALL_DIR=$(WASM_SYSROOT)/lib/wasm32-wasi
INCLUDE_DIR=$(WASM_SYSROOT)/include

include make.inc

$(info $$LIBEXT is ${LIBEXT})
$(info F2CLIB = ${F2CLIB})
$(info CBLASLIB = ${CBLASLIB})
$(info LAPACKLIB = ${LAPACKLIB})

all: f2clib lapack_install lib 
#all: f2clib lapack_install lib lapack_testing blas_testing variants_testing

#lib: lapacklib tmglib
#lib: f2clib lapacklib tmglib
lib: f2clib blaslib cblaslib lapacklib

clean: cleanlib cleantesting cleanblas_testing 

install: 
	cp $(LAPACKLIB) $(INSTALL_DIR)/liblapack$(LIBEXT)
	cp blas$(PLAT)$(LIBEXT) $(INSTALL_DIR)/libblas$(LIBEXT)
	cp cblas$(PLAT)$(LIBEXT) $(INSTALL_DIR)/libcblas$(LIBEXT)
	cp F2CLIBS/libf2c$(LIBEXT) $(INSTALL_DIR)/libf2c$(LIBEXT)
	mkdir -p $(INCLUDE_DIR)/clapack
	cp INCLUDE/* $(INCLUDE_DIR)/clapack/
	cp cblas/cblas.h $(INCLUDE_DIR)/clapack/

lapack_install:
	( cd INSTALL; $(MAKE) )

blaslib:
	( cd BLAS/SRC; $(MAKE) )

cblaslib:
	( cd cblas; $(MAKE) )

cblaswrap: 
	( cd BLAS/WRAP; $(MAKE) libcblaswr.a; cp libcblaswr.a ../.. )

fblaswrap: 
	( cd BLAS/WRAP; $(MAKE) libfblaswr.a; cp libfblaswr.a ../.. )

lapacklib: lapack_install
	( cd SRC; $(MAKE) )

variants:
	( cd SRC/VARIANTS ; $(MAKE))
	
tmglib:
	( cd TESTING/MATGEN; $(MAKE) )

f2clib:
	( cd F2CLIBS/libf2c; $(MAKE) )

lapack_testing:	lib
	( cd TESTING ; $(MAKE) )

variants_testing: lib
	( cd TESTING ; rm -f xlintst* ; $(MAKE)  VARLIB='SRC/VARIANTS/LIB/cholrl.a' ; \
	mv stest.out stest_cholrl.out ; mv dtest.out dtest_cholrl.out ; mv ctest.out ctest_cholrl.out ; mv ztest.out ztest_cholrl.out )
	( cd TESTING ; rm -f xlintst* ; $(MAKE)  VARLIB='SRC/VARIANTS/LIB/choltop.a' ; \
	mv stest.out stest_choltop.out ; mv dtest.out dtest_choltop.out ; mv ctest.out ctest_choltop.out ; mv ztest.out ztest_choltop.out )
	( cd TESTING ; rm -f xlintst* ; $(MAKE)  VARLIB='SRC/VARIANTS/LIB/lucr.a' ; \
	mv stest.out stest_lucr.out ; mv dtest.out dtest_lucr.out ; mv ctest.out ctest_lucr.out ; mv ztest.out ztest_lucr.out )
	( cd TESTING ;  rm -f xlintst* ; $(MAKE)  VARLIB='SRC/VARIANTS/LIB/lull.a' ; \
	mv stest.out stest_lull.out ; mv dtest.out dtest_lull.out ; mv ctest.out ctest_lull.out ; mv ztest.out ztest_lull.out )
	( cd TESTING ;  rm -f xlintst* ; $(MAKE)  VARLIB='SRC/VARIANTS/LIB/lurec.a' ; \
	mv stest.out stest_lurec.out ; mv dtest.out dtest_lurec.out ; mv ctest.out ctest_lurec.out ; mv ztest.out ztest_lurec.out )
	( cd TESTING ;  rm -f xlintst* ; $(MAKE)  VARLIB='SRC/VARIANTS/LIB/qrll.a' ; \
	mv stest.out stest_qrll.out ; mv dtest.out dtest_qrll.out ; mv ctest.out ctest_qrll.out ; mv ztest.out ztest_qrll.out )
		
blas_testing:
	( cd BLAS/TESTING; $(MAKE) -f Makeblat1 )
	( cd BLAS; ./xblat1s > sblat1.out    ; \
	           ./xblat1d > dblat1.out    ; \
	           ./xblat1c > cblat1.out    ; \
	           ./xblat1z > zblat1.out    ) 
	( cd BLAS/TESTING; $(MAKE) -f Makeblat2 )
	( cd BLAS; ./xblat2s < sblat2.in     ; \
	           ./xblat2d < dblat2.in     ; \
	           ./xblat2c < cblat2.in     ; \
	           ./xblat2z < zblat2.in     )
	( cd BLAS/TESTING; $(MAKE) -f Makeblat3 )
	( cd BLAS; ./xblat3s < sblat3.in     ; \
	           ./xblat3d < dblat3.in     ; \
	           ./xblat3c < cblat3.in     ; \
	           ./xblat3z < zblat3.in     ) 

cleanlib:
	( cd INSTALL; $(MAKE) clean )
	( cd BLAS/SRC; $(MAKE) clean )
	( cd cblas; $(MAKE) clean )
	( cd SRC; $(MAKE) clean )
	( cd SRC/VARIANTS; $(MAKE) clean )
	( cd TESTING/MATGEN; $(MAKE) clean )
	( cd F2CLIBS/libf2c; $(MAKE) clean )
	( cd F2CLIBS; rm -f *.a)
	( cd F2CLIBS; rm -f *.so)
	( rm -f *.a )
	( rm -f *.so )

cleanblas_testing:	
	( cd BLAS/TESTING; $(MAKE) -f Makeblat1 clean )
	( cd BLAS/TESTING; $(MAKE) -f Makeblat2 clean )
	( cd BLAS/TESTING; $(MAKE) -f Makeblat3 clean )
	( cd BLAS; rm -f xblat* )

cleantesting:
	( cd TESTING/LIN; $(MAKE) clean )
	( cd TESTING/EIG; $(MAKE) clean )
	( cd TESTING; rm -f xlin* xeig* )

cleanall: cleanlib cleanblas_testing cleantesting 
	rm -f *.a TESTING/*.out INSTALL/test*  BLAS/*.out

