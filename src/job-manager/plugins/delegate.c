/************************************************************\
 * Copyright 2024 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

/*
 * Jobtap plugin for delegating jobs to another Flux instance.
 */

#if HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <jansson.h>
#include <stdint.h>
#include <limits.h>
#include <unistd.h>

#include <flux/core.h>
#include <flux/jobtap.h>

#include "src/common/libutil/idf58.h"
#include "src/common/libutil/eventlog.h"
#include "src/common/select/select.h"

/*
 * Callback firing when job has completed.
 */
static void wait_callback (flux_future_t *f, void *arg)
{
    flux_plugin_t *p = arg;
    flux_jobid_t *id;
    bool success;
    const char *errstr;

    if (!(id = flux_future_aux_get (f, "flux::jobid"))) {
        return;
    }
    if (flux_job_wait_get_status (f, &success, &errstr) < 0) {
        flux_jobtap_raise_exception (p,
                                     *id,
                                     "DelegationFailure",
                                     0,
                                     "Could not fetch result of job");
        return;
    }
    if (success) {
        if (flux_jobtap_dependency_remove (p, *id, "delegated") < 0) {
            errstr = "failed to remove dependency";
        } else {
            flux_future_destroy (f);
            return;
        }
    }
    flux_jobtap_raise_exception (p, *id, "DelegationFailure", 0, "errstr %s", errstr);
    flux_future_destroy (f);
}

/*
 * Callback firing when events are ready.
 */
static void event_callback (flux_future_t *f, void *arg)
{
    flux_plugin_t *p = arg;
    flux_t *h;
    flux_jobid_t *id;
    json_t *o = NULL;
    json_t *context = NULL;
    const char *name = NULL, *event = NULL;
    double timestamp;

    if (!(h = flux_future_aux_get (f, "flux::handle"))) {
        return;
    }
    if (!(id = flux_future_aux_get (f, "flux::jobid"))) {
        flux_log_error (h, "could not fetch flux::jobid from future");
        return;
    }
    if (flux_job_event_watch_get (f, &event) != 0 || !(o = eventlog_entry_decode (event))
        || eventlog_entry_parse (o, &timestamp, &name, &context) < 0) {
        json_decref (o);
        flux_log_error (h, "Error decoding/parsing eventlog entry for %s", idf58 (*id));
        return;
    }
    if (!strcmp (name, "start")) {
        /*  'start' event with no cray_port_distribution event.
         *  assume cray-pals jobtap plugin is not loaded.
         */
        if (flux_jobtap_event_post_pack (p, *id, "delegate::start", "{s:f}", "timestamp", timestamp)
            < 0) {
            flux_log_error (h, "could not post delegate::start event for %s", idf58 (*id));
        }
    }

    if (!strcmp (name, "clean")) {
        // clean event, no more events needed
        flux_future_destroy (f);
    } else {
        flux_future_reset (f);
    }
    return;
}

/*
 * Callback firing when job has been submitted and ID is ready.
 */
static void submit_callback (flux_future_t *f, void *arg)
{
    flux_t *h, *delegated_h;
    flux_plugin_t *p = arg;
    flux_jobid_t delegated_id, *orig_id, *idcpy = NULL, *id_for_wait = NULL, *id_for_event = NULL;
    ;
    flux_future_t *wait_future = NULL, *event_future = NULL;

    const char *errstr;
    if (!(h = flux_jobtap_get_flux (p))) {
        flux_future_destroy (f);
        return;
    }
    if (!(id_for_wait = malloc (sizeof (*id_for_wait)))
        || !(id_for_event = malloc (sizeof (*id_for_event)))
        || !(idcpy = malloc (sizeof (*idcpy)))) {
        flux_log_error (h, "Unable to create flux_ids");
    }
    if (!(orig_id = flux_future_aux_get (f, "flux::jobid"))) {
        flux_log_error (h, "in submit callback: couldn't get jobid");
        flux_future_destroy (f);
        return;
    }
    *id_for_event = *orig_id;
    *id_for_wait = *orig_id;
    if (!(delegated_h = flux_future_get_flux (f)) || flux_job_submit_get_id (f, &delegated_id) < 0
        || !(wait_future = flux_job_wait (delegated_h, delegated_id))
        || flux_future_aux_set (wait_future, "flux::jobid", id_for_wait, free) < 0
        || flux_future_then (wait_future, -1, wait_callback, p) < 0
        || !(event_future = flux_job_event_watch (delegated_h, delegated_id, "eventlog", 0))
        || flux_future_aux_set (event_future, "flux::jobid", id_for_event, free) < 0
        || flux_future_aux_set (event_future, "flux::handle", h, NULL) < 0
        || flux_future_then (event_future, -1, event_callback, p) < 0
        || flux_jobtap_event_post_pack (p,
                                        *orig_id,
                                        "delegate::submit",
                                        "{s:I}",
                                        "jobid",
                                        delegated_id)
               < 0) {
        if (!(errstr = flux_future_error_string (f))) {
            errstr = "";
        }
        flux_jobtap_raise_exception (p, *orig_id, "DelegationFailure", 0, errstr);
        flux_future_destroy (wait_future);
        flux_future_destroy (event_future);
        flux_future_destroy (f);
        return;
    }
    *idcpy = delegated_id;
    if (flux_jobtap_job_aux_set (p, *orig_id, "flux::delegate::delegate_id", idcpy, free) < 0) {
        flux_log_error (h, "unable to save delegated jobId");
    }
    flux_future_destroy (f);
}

/*
 * Remove all dependencies from jobspec.
 *
 * Dependencies may reference jobids that the instance the job is
 * being sent to does not recognize.
 *
 * Also, if the 'delegate' dependency in particular were not removed,
 * one of two things would happen. If the instance the job is sent to
 * does not have this jobtap plugin loaded, then the job would be
 * rejected. Otherwise, if the instance DOES have this jobtap plugin
 * loaded, it would attempt to delegate to itself in an infinite
 * loop.
 */
static char *remove_dependency_and_encode (json_t *jobspec)
{
    char *encoded_jobspec;
    json_t *dependency_list = NULL;

    if (!(jobspec = json_deep_copy (jobspec))) {
        return NULL;
    }
    if (json_unpack (jobspec,
                     "{s:{s:{s:o}}}",
                     "attributes",
                     "system",
                     "dependencies",
                     &dependency_list)
            < 0
        || json_array_clear (dependency_list) < 0) {
        json_decref (jobspec);
        return NULL;
    }
    encoded_jobspec = json_dumps (jobspec, 0);
    json_decref (jobspec);
    return encoded_jobspec;
}

/*
 * Handle job.dependency.delegate requests
 */
static int depend_cb (flux_plugin_t *p, const char *topic, flux_plugin_arg_t *args, void *arg)
{
    flux_t *h = flux_jobtap_get_flux (p);
    flux_jobid_t *id;
    flux_t *delegated;
    const char *uri, *strategy = NULL;
    json_t *jobspec;
    char *encoded_jobspec = NULL;
    flux_future_t *jobid_future = NULL;
    struct cluster_config *config;

    if (!h || !(id = malloc (sizeof (flux_jobid_t)))) {
        return flux_jobtap_reject_job (p, args, "error processing delegate: %s", strerror (errno));
    }
    if (flux_plugin_arg_unpack (args,
                                FLUX_PLUGIN_ARG_IN,
                                "{s:I s:{s?s} s:o}",
                                "id",
                                id,
                                "dependency",
                                "value",
                                &strategy,
                                "jobspec",
                                &jobspec)
            < 0
        || flux_jobtap_job_aux_set (p, *id, "flux::delegate::jobid", id, free) < 0) {
        free (id);
        return flux_jobtap_reject_job (p,
                                       args,
                                       "error processing delegate: %s",
                                       flux_plugin_arg_strerror (args));
    }
    if (!(config = flux_plugin_aux_get (p, "flux::delegate::selection_config"))
        || !(uri = select_cluster (config, strategy))) {
        flux_log_error (h, "%s: could not select URI", idf58 (*id));
        return -1;
    }
    if (!(delegated = flux_open (uri, 0))) {
        flux_log_error (h, "%s: could not open URI %s", idf58 (*id), uri);
        return -1;
    }
    if (flux_jobtap_dependency_add (p, *id, "delegated") < 0
        || flux_jobtap_job_aux_set (p,
                                    *id,
                                    "flux::delegated_handle",
                                    delegated,
                                    (flux_free_f)flux_close)
               < 0
        || flux_set_reactor (delegated, flux_get_reactor (h)) < 0) {
        flux_log_error (h, "%s: flux_jobtap_dependency_add", idf58 (*id));
        flux_close (delegated);
        return -1;
    }
    // submit the job to the specified instance and attach a callback for fetching the
    // ID
    if (flux_jobtap_job_set_flag (p, FLUX_JOBTAP_CURRENT_JOB, "alloc-bypass") < 0) {
        flux_jobtap_raise_exception (p,
                                     FLUX_JOBTAP_CURRENT_JOB,
                                     "alloc",
                                     0,
                                     "Failed to set alloc-bypass: %s",
                                     strerror (errno));
        return -1;
    }
    if (!(encoded_jobspec = remove_dependency_and_encode (jobspec))
        || !(jobid_future = flux_job_submit (delegated, encoded_jobspec, 16, FLUX_JOB_WAITABLE))
        || flux_future_then (jobid_future, -1, submit_callback, p) < 0
        || flux_future_aux_set (jobid_future, "flux::jobid", id, NULL) < 0) {
        flux_log_error (h,
                        "%s: could not delegate job to specified Flux "
                        "instance",
                        idf58 (*id));
        flux_future_destroy (jobid_future);
        free (encoded_jobspec);
        return -1;
    }
    free (encoded_jobspec);
    return 0;
}

/*
 * Continuation callback, invoked when alloc-bypass R has been committed
 * to the KVS.
 *
 * Post the alloc (bypass: true) event for the job.
 */
static void alloc_continuation (flux_future_t *f, void *arg)
{
    flux_plugin_t *p = arg;
    flux_jobid_t *id;

    flux_t *h = flux_jobtap_get_flux (p);

    if (!f || !(id = flux_future_aux_get (f, "flux::jobid"))) {
        flux_log_error (h, "alloc_continuation: failed to get jobid from future");
        goto done;
    }

    if (flux_future_get (f, NULL) < 0) {
        flux_log_error (h, "alloc_continuation: flux_future_get failed for %s", idf58 (*id));

        flux_jobtap_raise_exception (p,
                                     *id,
                                     "alloc",
                                     0,
                                     "failed to commit R to kvs: %s",
                                     strerror (errno));
        goto done;
    }

    if (flux_jobtap_event_post_pack (p, *id, "alloc", "{s:b}", "bypass", true) < 0) {
        flux_log_error (h, "alloc_continuation: flux_jobtap_event_post_pack for %s", idf58 (*id));
        flux_jobtap_raise_exception (p,
                                     *id,
                                     "alloc",
                                     0,
                                     "failed to post alloc event: %s",
                                     strerror (errno));
        goto done;
    }

done:
    flux_future_destroy (f);
}

/*
 * job.state.sched callback. Generates an alloc-bypass R for the job and passes it to the
 * job manager. Then starts a KVS transaction to post the R to the KVS. Once that is
 * complete, the `alloc` event should be posted, but that is handled asynchronously.
 */
static int sched_cb (flux_plugin_t *p, const char *topic, flux_plugin_arg_t *args, void *arg)
{
    flux_t *h = flux_jobtap_get_flux (p);
    if (!h)
        return -1;
    flux_jobid_t *id;
    flux_kvs_txn_t *txn = NULL;
    json_t *R = NULL;
    char key[256];
    char hostname[HOST_NAME_MAX + 1];
    flux_future_t *alloc_future = NULL;

    if (!(id = flux_jobtap_job_aux_get (p, FLUX_JOBTAP_CURRENT_JOB, "flux::delegate::jobid")))
        return 0;

    if (gethostname (hostname, sizeof (hostname)) < 0) {
        flux_jobtap_raise_exception (p,
                                     FLUX_JOBTAP_CURRENT_JOB,
                                     "alloc",
                                     0,
                                     "delegate: gethostname: %s",
                                     flux_plugin_arg_strerror (args));
        flux_log_error (h, "gethostname for %s", idf58 (*id));
        return -1;
    }
    // Create a fake R without properties
    if (!(R = json_pack ("{s:i s:{s:[{s:s s:{s:s}}] s:f s:f s:[s]}}",
                         "version",
                         1,
                         "execution",
                         "R_lite",
                         "rank",
                         "0",
                         "children",
                         "core",
                         "0",
                         "starttime",
                         0.0,
                         "expiration",
                         0.0,
                         "nodelist",
                         hostname))
        || flux_plugin_arg_pack (args, FLUX_PLUGIN_ARG_OUT, "{s:O}", "R", R) < 0) {
        json_decref (R);
        flux_log_error (h, "failed to output fake R for %s", idf58 (*id));
        return flux_jobtap_raise_exception (p,
                                            *id,
                                            "alloc",
                                            0,
                                            "failed to post alloc event: %s",
                                            strerror (errno));
    }
    // Create KVS transaction posting the R
    if (!(txn = flux_kvs_txn_create ()) || flux_job_kvs_key (key, sizeof (key), *id, "R") < 0
        || flux_kvs_txn_pack (txn, 0, key, "O", R) < 0
        || !(alloc_future = flux_kvs_commit (h, NULL, 0, txn))
        || flux_future_then (alloc_future, -1, alloc_continuation, p) < 0) {
        flux_log_error (h, "failed processing R KVS transaction");
        flux_kvs_txn_destroy (txn);
        flux_future_destroy (alloc_future);
        json_decref (R);
        return flux_jobtap_raise_exception (p,
                                            *id,
                                            "alloc",
                                            0,
                                            "failed to post alloc event: %s",
                                            strerror (errno));
    }
    json_decref (R);
    flux_kvs_txn_destroy (txn);
    if (flux_future_aux_set (alloc_future, "flux::jobid", id, NULL) < 0) {
        flux_future_destroy (alloc_future);
        return flux_jobtap_raise_exception (p, *id, "alloc", 0, "flux_future_aux_set");
    }

    return 0;
}

/*
 * job.state.run callback. Sends `job-exec.override` RPCs to make the job skip
 * execution, since execution is handled by another Flux instance.
 */
static int run_cb (flux_plugin_t *p, const char *topic, flux_plugin_arg_t *args, void *arg)
{
    flux_t *h = flux_jobtap_get_flux (p);
    flux_jobid_t job_id;
    flux_future_t *run_future;

    if (!h)
        return -1;

    if (!flux_jobtap_job_aux_get (p, FLUX_JOBTAP_CURRENT_JOB, "flux::delegate::jobid"))
        return 0;

    if (flux_plugin_arg_unpack (args, FLUX_PLUGIN_ARG_IN, "{s:I}", "id", &job_id) < 0) {
        flux_jobtap_raise_exception (p,
                                     FLUX_JOBTAP_CURRENT_JOB,
                                     "alloc",
                                     0,
                                     "delegate: unpack: %s",
                                     flux_plugin_arg_strerror (args));
        flux_log_error (h, "flux_plugin_arg_unpack");
        return -1;
    }
    // send `job-exec.override` start and finish events. TODO: check the RPC return status
    // and handle errors.
    if (!(run_future = flux_rpc_pack (h,
                                      "job-exec.override",
                                      FLUX_NODEID_ANY,
                                      0,
                                      "{s:s s:I}",
                                      "event",
                                      "start",
                                      "jobid",
                                      job_id))) {
        flux_log_error (h, "flux_rpc_pack failed for %s job-exec.override: start", idf58 (job_id));
        return -1;
    }
    flux_future_destroy (run_future);
    if (!(run_future = flux_rpc_pack (h,
                                      "job-exec.override",
                                      FLUX_NODEID_ANY,
                                      0,
                                      "{s:s s:I}",
                                      "event",
                                      "finish",
                                      "jobid",
                                      job_id))) {
        flux_log_error (h,
                        "flux_rpc_pack failed for %s in job-exec.override: finish",
                        idf58 (job_id));
        return -1;
    }
    flux_future_destroy (run_future);

    return 0;
}
static int destroy_cb (flux_plugin_t *p, const char *topic, flux_plugin_arg_t *args, void *arg)
{
    flux_t *h = flux_jobtap_get_flux (p);
    flux_jobid_t job_id, *delegated_job_id;
    flux_future_t *cancel_future = NULL;
    flux_t *delegated_handle = NULL;
    if (!h)
        return -1;

    if (!(delegated_job_id =
              flux_jobtap_job_aux_get (p, FLUX_JOBTAP_CURRENT_JOB, "flux::delegate::delegate_id")))
        return 0;
    if (flux_plugin_arg_unpack (args, FLUX_PLUGIN_ARG_IN, "{s:I}", "id", &job_id) < 0) {
        flux_log_error (h, "flux_plugin_arg_unpack for jobid");
        return 0;
    }
    if (!(delegated_handle = flux_jobtap_job_aux_get (p, job_id, "flux::delegated_handle"))) {
        flux_log_error (h, "job is delegated but its handle not found");
        return -1;
    }
    if (!(cancel_future =
              flux_job_cancel (delegated_handle, *delegated_job_id, "Cancelled by parent cluster"))
        || flux_future_get (cancel_future, NULL) < 0) {
        flux_log_error (h,
                        "Unable to cancel job in child cluster %s",
                        flux_future_error_string (cancel_future));
        return 0;
    }
    return 0;
}

static const struct flux_plugin_handler tab[] = {
    {"job.dependency.delegate", depend_cb, NULL},
    {"job.state.sched", sched_cb, NULL},
    {"job.state.run", run_cb, NULL},
    {"job.destroy", destroy_cb, NULL},
    {0},
};

int flux_plugin_init (flux_plugin_t *p)
{
    struct cluster_config *config;
    flux_t *h = flux_jobtap_get_flux (p);

    if (!h || flux_plugin_register (p, "delegate", tab) < 0 || !(config = selection_init (h))
        || flux_plugin_aux_set (p,
                                "flux::delegate::selection_config",
                                config,
                                (flux_free_f)selection_destroy)
               < 0) {
        return -1;
    }
    return 0;
}
