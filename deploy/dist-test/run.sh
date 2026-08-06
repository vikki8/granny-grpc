#!/bin/bash

THIS_DIR=$(dirname $(readlink -f $0))
export PROJ_ROOT=${THIS_DIR}/../..
pushd ${PROJ_ROOT} > /dev/null

RETURN_VAL=0

DIST_TEST_ARGS=${DIST_TEST_ARGS:-}

if [ -n "${DIST_TEST_ARGS}" ]; then
    faasmctl cli.faasm --cmd "/build/faasm/bin/dist_tests ${DIST_TEST_ARGS}"
else
    faasmctl cli.faasm --cmd "/build/faasm/bin/dist_tests"
fi

RETURN_VAL=$?

echo "-------------------------------------------"
echo "                SERVER LOGS                "
echo "-------------------------------------------"
docker compose logs dist-test-server dist-test-server-2
faasmctl logs -s dist-test-server
faasmctl logs -s dist-test-server-2

popd >> /dev/null

exit $RETURN_VAL
