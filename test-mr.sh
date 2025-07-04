#!/usr/bin/env bash

#
# map-reduce tests
#

# un-comment this to run the tests with the Go race detector.
# RACE=-race

if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

ISQUIET=$1
maybe_quiet() {
    if [ "$ISQUIET" == "quiet" ]; then
      "$@" > /dev/null 2>&1
    else
      "$@"
    fi
}


TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
echo "🛠  Rebuilding plugins and binaries..."

(cd ../mrapps && go clean)
(cd .. && go clean)

(cd ../mrapps/wc && go build $RACE -buildmode=plugin -o ../../mr-tmp/_build/wc.so wc.go)
(cd ../mrapps/indexer && go build $RACE -buildmode=plugin -o ../../mr-tmp/_build/indexer.so indexer.go)
(cd ../mrapps/mtiming     && go build $RACE -buildmode=plugin -o ../../mr-tmp/_build/mtiming.so mtiming.go)
(cd ../mrapps/rtiming     && go build $RACE -buildmode=plugin -o ../../mr-tmp/_build/rtiming.so rtiming.go)
(cd ../mrapps/jobcount    && go build $RACE -buildmode=plugin -o ../../mr-tmp/_build/jobcount.so jobcount.go)
(cd ../mrapps/early_exit  && go build $RACE -buildmode=plugin -o ../../mr-tmp/_build/early_exit.so early_exit.go)
(cd ../mrapps/crash       && go build $RACE -buildmode=plugin -o ../../mr-tmp/_build/crash.so crash.go)
(cd ../mrapps/nocrash     && go build $RACE -buildmode=plugin -o ../../mr-tmp/_build/nocrash.so nocrash.go)


(cd ../cmd/mrcoordinator && go build $RACE -o ../../mr-tmp/_build/mrcoordinator main.go)
(cd ../cmd/mrworker && go build $RACE -o ../../mr-tmp/_build/mrworker main.go)
(cd ../cmd/mrsequential && go build $RACE -o ../../mr-tmp/_build/mrsequential main.go)


failed_any=0

#########################################################
# first word-count

INPUT="../input/pg*txt"

# generate the correct output
./_build/mrsequential ./_build/wc.so $INPUT || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

maybe_quiet $TIMEOUT ./_build/mrcoordinator $INPUT &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ./_build/mrworker ./_build/wc.so) &
(maybe_quiet $TIMEOUT ./_build/mrworker ./_build/wc.so) &
(maybe_quiet $TIMEOUT ./_build/mrworker ./_build/wc.so) &

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort ./_output/mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

# #########################################################
# now indexer
rm -rf ./_output

# generate the correct output
./_build/mrsequential ./_build/indexer.so $INPUT || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

maybe_quiet $TIMEOUT ./_build/mrcoordinator $INPUT &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT ./_build/mrworker ./_build/indexer.so &
maybe_quiet $TIMEOUT ./_build/mrworker ./_build/indexer.so

sort ./_output/mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting map parallelism test.

rm -rf ./_output

maybe_quiet $TIMEOUT ./_build/mrcoordinator $INPUT &
sleep 1

maybe_quiet $TIMEOUT ./_build/mrworker ./_build/mtiming.so &
maybe_quiet $TIMEOUT ./_build/mrworker ./_build/mtiming.so

NT=`cat ./_output/mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat ./_output/mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting reduce parallelism test.

rm -rf ./_output

maybe_quiet $TIMEOUT ./_build/mrcoordinator $INPUT &
sleep 1

maybe_quiet $TIMEOUT ./_build/mrworker ./_build/rtiming.so  &
maybe_quiet $TIMEOUT ./_build/mrworker ./_build/rtiming.so

NT=`cat ./_output/mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################
echo '***' Starting job count test.

rm -rf ./_output

maybe_quiet $TIMEOUT ./_build/mrcoordinator $INPUT  &
sleep 1

maybe_quiet $TIMEOUT ./_build/mrworker ./_build/jobcount.so &
maybe_quiet $TIMEOUT ./_build/mrworker ./_build/jobcount.so
maybe_quiet $TIMEOUT ./_build/mrworker ./_build/jobcount.so &
maybe_quiet $TIMEOUT ./_build/mrworker ./_build/jobcount.so

NT=`cat ./_output/mr-out* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi

wait

#########################################################
# test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)
rm -rf ./_output

echo '***' Starting early exit test.

DF=anydone$$
rm -f $DF

(maybe_quiet $TIMEOUT ./_build/mrcoordinator $INPUT; touch $DF) &

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ./_build/mrworker ./_build/early_exit.so; touch $DF) &
(maybe_quiet $TIMEOUT ./_build/mrworker ./_build/early_exit.so; touch $DF) &
(maybe_quiet $TIMEOUT ./_build/mrworker ./_build/early_exit.so; touch $DF) &

# wait for any of the coord or workers to exit.
# `jobs` ensures that any completed old processes from other tests
# are not waited upon.
jobs &> /dev/null
if [[ "$OSTYPE" = "darwin"* ]]
then
  # bash on the Mac doesn't have wait -n
  while [ ! -e $DF ]
  do
    sleep 0.2
  done
else
  # the -n causes wait to wait for just one child process,
  # rather than waiting for all to finish.
  wait -n
fi

rm -f $DF

# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the coordinator exited early
sort ./_output/mr-out* | grep . > mr-wc-all-initial

# wait for remaining workers and coordinator to exit.
wait

# compare initial and final outputs
sort ./_output/mr-out* | grep . > mr-wc-all-final
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi

rm -rf ./_output

# #########################################################
echo '***' Starting crash test.

# generate the correct output
./_build/mrsequential ./_build/nocrash.so $INPUT || exit 1
sort ./mr-out-0 > mr-correct-crash.txt
rm -rf ./_output

rm -f mr-done
((maybe_quiet $TIMEOUT2 ./_build/mrcoordinator $INPUT); touch mr-done ) &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT2 ./_build/mrworker ./_build/crash.so &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/5840-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 ./_build/mrworker ./_build/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 ./_build/mrworker ./_build/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  maybe_quiet $TIMEOUT2 ./_build/mrworker ./_build/crash.so
  sleep 1
done

wait

rm $SOCKNAME
sort ./_output/mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
