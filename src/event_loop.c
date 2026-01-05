/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "event_loop.h"
#include "mr_memory.h"
#include "common.h"
#include <pthread.h>
#include "../deps/hiredis/adapters/libevent.h"
#include <event2/thread.h>

#if defined(__linux__)
#include <sys/prctl.h>
#endif

struct {
    struct event_base *loop;
    pthread_t loopThread;
    volatile int isThreadStarted;
    pthread_mutex_t isThreadStartedLock;
    /* A single persistent event used to drain queued tasks. */
    struct event *taskEvent;
    /* Task queue (MPSC) feeding the event loop thread. */
    pthread_mutex_t taskQueueLock;
    struct MR_LoopTaskCtx* taskQueueHead;
    struct MR_LoopTaskCtx* taskQueueTail;
    volatile int taskEventArmed;
}evLoopCtx;

typedef struct MR_LoopTaskCtx{
    EventLoopTaskCallback callback;
    void* ctx;
    struct event *event;
    struct MR_LoopTaskCtx* next;
}MR_LoopTaskCtx;

static void* MR_Loop(void *arg);
static void MR_DrainTaskQueue(evutil_socket_t s, short what, void *arg);

static void MR_StartThread() {
    if (evLoopCtx.isThreadStarted) {
        return;
    }

    pthread_mutex_lock(&evLoopCtx.isThreadStartedLock);
    if (evLoopCtx.isThreadStarted) {
        pthread_mutex_unlock(&evLoopCtx.isThreadStartedLock);
        return;
    }

    pthread_create(&evLoopCtx.loopThread, NULL, MR_Loop, NULL);

    evLoopCtx.isThreadStarted = 1;
    pthread_mutex_unlock(&evLoopCtx.isThreadStartedLock);
}

static void MR_NewTask(evutil_socket_t s, short what, void *arg){
    MR_LoopTaskCtx* taskCtx = arg;
    taskCtx->callback(taskCtx->ctx);
    event_free(taskCtx->event);
    MR_FREE(taskCtx);
}

/* Runs on the event loop thread; drains queued immediate tasks in batches. */
static void MR_DrainTaskQueue(evutil_socket_t s, short what, void *arg){
    (void)s; (void)what; (void)arg;
    while (1) {
        MR_LoopTaskCtx* task = NULL;
        pthread_mutex_lock(&evLoopCtx.taskQueueLock);
        task = evLoopCtx.taskQueueHead;
        if (task) {
            evLoopCtx.taskQueueHead = task->next;
            if (!evLoopCtx.taskQueueHead) {
                evLoopCtx.taskQueueTail = NULL;
                evLoopCtx.taskEventArmed = 0;
            }
        } else {
            evLoopCtx.taskEventArmed = 0;
        }
        pthread_mutex_unlock(&evLoopCtx.taskQueueLock);

        if (!task) {
            break;
        }

        task->callback(task->ctx);
        MR_FREE(task);
    }
}

MR_LoopTaskCtx* MR_EventLoopAddTaskWithDelay(EventLoopTaskCallback callback, void* ctx, size_t delayMs) {
    MR_StartThread();
    MR_LoopTaskCtx* taskCtx = MR_ALLOC(sizeof(*taskCtx));
    taskCtx->callback = callback;
    taskCtx->ctx = ctx;
    taskCtx->next = NULL;
    taskCtx->event = event_new(evLoopCtx.loop,
                                    -1,
                                    0,
                                    MR_NewTask,
                                    taskCtx);
    struct timeval tv = {
            .tv_sec = delayMs / 1000,
            .tv_usec = (delayMs % 1000) * 1000,
    };
    event_add(taskCtx->event, &tv);
    return taskCtx;
}

void MR_EventLoopDelayTaskCancel(MR_LoopTaskCtx* dtCtx) {
    event_free(dtCtx->event);
    MR_FREE(dtCtx);
}

void MR_EventLoopAddTask(EventLoopTaskCallback callback, void* ctx) {
    MR_StartThread();
    MR_LoopTaskCtx* taskCtx = MR_ALLOC(sizeof(*taskCtx));
    taskCtx->callback = callback;
    taskCtx->ctx = ctx;
    taskCtx->event = NULL;
    taskCtx->next = NULL;

    int should_arm = 0;
    pthread_mutex_lock(&evLoopCtx.taskQueueLock);
    if (evLoopCtx.taskQueueTail) {
        evLoopCtx.taskQueueTail->next = taskCtx;
        evLoopCtx.taskQueueTail = taskCtx;
    } else {
        evLoopCtx.taskQueueHead = taskCtx;
        evLoopCtx.taskQueueTail = taskCtx;
    }
    if (!evLoopCtx.taskEventArmed) {
        evLoopCtx.taskEventArmed = 1;
        should_arm = 1;
    }
    pthread_mutex_unlock(&evLoopCtx.taskQueueLock);

    if (should_arm) {
        event_active(evLoopCtx.taskEvent, 0, 0);
    }
}

static void* MR_Loop(void *arg) {
    // el stands for event look, we avoid long names cause thread names are truncated to 15 chars.
#if defined(__linux__)
  /* Use prctl instead to prevent using _GNU_SOURCE flag and implicit declaration */
  prctl(PR_SET_NAME, xstr(MODULE_NAME)"-el");
#elif defined(__APPLE__) && defined(__MACH__)
  pthread_setname_np(xstr(MODULE_NAME)"-el");
#else
  err("thread_do(): pthread_setname_np is not supported on this system");
#endif
    while (1) {
        event_base_loop(evLoopCtx.loop, EVLOOP_NO_EXIT_ON_EMPTY);
    }
    return NULL;
}

struct event_base* MR_EventLoopGet() {
    MR_StartThread();
    return evLoopCtx.loop;
}

void MR_EventLoopStart() {
    evthread_use_pthreads();
    evLoopCtx.loop = (struct event_base*)event_base_new();
    evLoopCtx.isThreadStarted = 0;
    pthread_mutex_init(&(evLoopCtx.isThreadStartedLock), NULL);

    pthread_mutex_init(&(evLoopCtx.taskQueueLock), NULL);
    evLoopCtx.taskQueueHead = NULL;
    evLoopCtx.taskQueueTail = NULL;
    evLoopCtx.taskEventArmed = 0;

    /* Single event used to drain immediate task queue. */
    evLoopCtx.taskEvent = event_new(evLoopCtx.loop, -1, 0, MR_DrainTaskQueue, NULL);
}
