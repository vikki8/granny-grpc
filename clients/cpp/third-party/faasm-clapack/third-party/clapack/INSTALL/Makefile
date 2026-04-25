TOPDIR=..
include $(TOPDIR)/make.inc
F2CLIB = $(TOPDIR)/F2CLIBS/libf2c$(LIBEXT)

.SUFFIXES : .o .c 
all:  testlsame testslamch testdlamch testsecond testdsecnd testieee testversion 

testlsame:  lsame.o lsametst.o
	$(CC) $(LDFLAGS) -o testlsame lsame.o lsametst.o $(F2CLIB) 

testslamch: slamch.o lsame.o slamchtst.o
	$(CC) $(LDFLAGS) -o testslamch slamch.o lsame.o slamchtst.o $(F2CLIB) 

testdlamch: dlamch.o lsame.o dlamchtst.o
	$(CC) $(LDFLAGS) -o testdlamch dlamch.o lsame.o dlamchtst.o $(F2CLIB) 

testsecond: second.o secondtst.o
	$(CC) $(LDFLAGS) -o testsecond second.o secondtst.o $(F2CLIB) 

testdsecnd: dsecnd.o dsecndtst.o
	$(CC) $(LDFLAGS) -o testdsecnd dsecnd.o dsecndtst.o $(F2CLIB) 

testieee: tstiee.o
	$(CC) $(LDFLAGS) -o testieee tstiee.o $(F2CLIB) 

testversion: ilaver.o LAPACK_version.o
	$(CC) $(LDFLAGS) -o testversion ilaver.o LAPACK_version.o $(F2CLIB) 

clean:
	rm -f *.o

#slamch.o: slamch.c 
#	$(CC) -fPIC $(NOOPT) -c $< -o $@
#dlamch.o: dlamch.c
#	$(CC) -fPIC $(NOOPT) -c $< -o $@

.c.o: 
	$(CC) $(CFLAGS) -c $< -o $@

