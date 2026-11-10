/* Deterministic terminal/cleanup ordering tests using the real execution
 * allocator, reference counting, task queue, worker and terminal handlers.
 * Only worker/timer scheduling and error-record payloads are test doubles.
 * Including mr.c keeps private implementation details out of the public API. */
#include <assert.h>
#include <stdio.h>
#include "../../src/mr.c"

static void (*next_worker)(void *);
static void *next_arg;
static unsigned timers, callbacks;

int mr_thpool_add_work(mr_threadpool pool, void (*callback)(void *), void *arg) {
    (void)pool;
    assert(next_worker == NULL);
    next_worker = callback;
    next_arg = arg;
    return 0;
}

MR_LoopTaskCtx *MR_EventLoopAddTaskWithDelay(EventLoopTaskCallback callback,
                                            void *arg, size_t delay) {
    (void)callback; (void)arg; (void)delay;
    ++timers;
    return NULL;
}

void MR_EventLoopDelayTaskCancel(MR_LoopTaskCtx *task) {
    (void)task;
    assert(!"these completed executions must not have an idle timer");
}

Record *MR_ErrorRecordCreate(const char *error) {
    (void)error;
    return calloc(1, sizeof(Record));
}

void MR_RecordFree(Record *record) { free(record); }

static void done(ExecutionCtx *ctx, void *pd) {
    (void)ctx; (void)pd;
    ++callbacks;
}

static void run_worker(void) {
    assert(next_worker != NULL);
    void (*callback)(void *) = next_worker;
    void *arg = next_arg;
    next_worker = NULL;
    callback(arg);
}

static void check_cleanup(ExecutionTaskCallback terminal, int pending) {
    timers = callbacks = 0;
    assert(next_worker == NULL);
    Execution *e = MR_ExecutionAlloc();
    /* Keep the caller's reference so we can inspect the worker's ownership
     * release safely. Production MR_Run similarly adds a worker reference. */
    ++e->refCount;
    e->callbacks.done.callback = done;
    assert(MR_ExecutionInvokeCallback(e, &e->callbacks.done));
    e->callbacks.done.callback = NULL; /* already completed */
    e->disposalPending = pending != 0;

    MR_ExecutionAddTask(e, terminal, NULL);
    if (pending == 1)
        MR_ExecutionAddTask(e, MR_DisposeExecution, NULL);
    run_worker();
    assert(callbacks == 1); /* completion must never be delivered twice */
    assert(timers == 0);    /* terminal tasks must not re-arm an idle timer */
    if (pending) {
        assert(e->refCount == 2); /* scheduled disposal still owns the reference */
        if (pending == 2) {
            /* Event-loop deletion arrives after the terminal worker drained. */
            assert(next_worker == NULL);
            MR_ExecutionAddTask(e, MR_DisposeExecution, NULL);
        }
        run_worker();
    }
    assert(e->refCount == 1); /* exactly one worker ownership release */
    assert(next_worker == NULL);
    assert(callbacks == 1 && timers == 0);
    MR_FreeExecution(e); /* release observer; sanitizers check actual destruction */
}

int main(void) {
    ExecutionTaskCallback handlers[] = {
        MR_ExecutionTimedOutInternal, MR_ExecutionAbortedOnClusterChange
    };
    for (size_t i = 0; i < sizeof(handlers) / sizeof(handlers[0]); ++i) {
        for (int pending = 0; pending < 3; ++pending) {
            check_cleanup(handlers[i], pending);
            printf("%s, %s: PASS\n", i ? "abort" : "timeout",
                   pending == 0 ? "no disposal scheduled" :
                   pending == 1 ? "disposal queued" : "disposal arrives later");
        }
    }
    return 0;
}
