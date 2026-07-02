#!/bin/sh

test_description='Test delegate plugin cancellation propagation (source <-> target)'

. $(dirname $0)/sharness.sh

test_under_flux 3

# Helper: extract the delegated jobid recorded in the source job's
# delegate::submit eventlog entry.
extract_delegated_id() {
	flux job eventlog "$1" |
		sed -nE "/delegate::submit/ s/.*jobid[\"=:[:space:]]+(\"?)([^\",}[:space:]]+).*/\2/p" |
		head -n 1
}

test_expect_success 'start one target sub-instance' '
	target_0=$(flux batch -n1 -t120s --wrap sleep inf) &&
	flux job wait-event ${target_0} start 
'

test_expect_success 'configure delegate plugin with one target URI' '
	uri_0=$(flux uri --local ${target_0}) &&
	printf "delegate = [ \"%s\" ]\n" "${uri_0}"  |
		flux config load && flux config get | jq -e .
'

test_expect_success 'plugin can be loaded' '
	flux jobtap load "${SHARNESS_TEST_SRCDIR}"/../src/job-manager/plugins/.libs/delegate.so &&
	flux jobtap list | grep delegate.so
'

#
# Test 1: cancelling the job in the SOURCE instance must propagate the
# cancellation down to the delegated job in the TARGET instance.
#
test_expect_success 'cancel from source propagates to target' '
	jobid=$(flux submit --dependency=delegate:random sleep inf) &&
	flux job wait-event --timeout=60 ${jobid} delegate::submit &&
	delegated_id=$(extract_delegated_id ${jobid}) &&
	test -n "${delegated_id}" &&
	# the delegated job is actually running in the target
	flux proxy ${uri_0} flux job wait-event --timeout=60 ${delegated_id} start &&
	# cancel from the source side
	flux cancel ${jobid} &&
	flux job wait-event --timeout=60 ${jobid} clean &&
	flux proxy ${uri_0} flux job wait-event --timeout=60 ${delegated_id} clean &&
	flux proxy ${uri_0} flux job eventlog ${delegated_id} |
		grep -E "exception" | grep -q "cancel"
'

test_expect_success 'no jobs left running in target after source cancel' '
	test $(flux proxy ${uri_0} flux jobs --no-header --filter=running |
		wc -l) -eq 0
'

#
# Test 2: cancelling the delegated job directly in the TARGET instance must
# propagate back up as a DelegationFailure exception on the SOURCE job.
#
test_expect_success 'cancel from target raises exception on source' '
	jobid=$(flux submit --dependency=delegate:random sleep inf) &&
	flux job wait-event --timeout=60 ${jobid} delegate::submit &&
	delegated_id=$(extract_delegated_id ${jobid}) &&
	test -n "${delegated_id}" &&
	flux proxy ${uri_0} flux job wait-event --timeout=60 ${delegated_id} start &&
	# cancel the delegated job *inside* the target instance
	flux proxy ${uri_0} flux cancel ${delegated_id} &&
	# wait_callback should raise DelegationFailure on the source job
	flux job wait-event --timeout=60 \
		--match-context=type=DelegationFailure ${jobid} exception &&
	flux job eventlog ${jobid} | grep -q "DelegationFailure"
'

test_expect_success 'source job is inactive after target-side cancel' '
	flux job wait-event --timeout=60 ${jobid} clean
'

test_expect_success 'unload delegate plugin' '
	flux jobtap remove delegate.so
'

test_expect_success 'cancel subinstances' '
	flux cancel --all
'

test_done

