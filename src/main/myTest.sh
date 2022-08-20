#!/bin/sh

rm -r mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*
# make sure software is freshly built.
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd .. && go build $RACE mrmaster.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

# generate the correct output
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*
echo '***' Starting wc test.

timeout -k 2s 180s ../mrmaster ../pg*txt &
# give the master time to create the sockets.
sleep 1
# start multiple workers.
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &

# wait for one of the processes to exit.
# under bash, this waits for all processes,
# including the master.
wait
echo '***' End.
# the master or a worker has exited. since workers are required
# to exit when a job is completely finished, and not before,
# that means the job has finished.

# sort mr-out* | grep . > mr-wc-all
# if cmp mr-wc-all mr-correct-wc.txt
# then
#   echo '---' wc test: PASS
# else
#   echo '---' wc output is not the same as mr-correct-wc.txt
#   echo '---' wc test: FAIL
#   failed_any=1
# fi
