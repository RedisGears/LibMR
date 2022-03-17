#include "event_loop.h"
#include "mr_memory.h"
#include <pthread.h>
#include "../deps/hiredis/adapters/libevent.h"
#include <event2/thread.h>

struct {
    struct event_base *loop;
    pthread_t loopThread;
    struct event *taskEvent;
}evLoopCtx;

typedef struct MR_LoopTaskCtx{
    EventLoopTaskCallback callback;
    void* ctx;
    struct event *event;
}MR_LoopTaskCtx;

static void MR_NewTask(evutil_socket_t s, short what, void *arg){
    MR_LoopTaskCtx* taskCtx = arg;
    event_free(taskCtx->event);
    MR_FREE(taskCtx);
}

MR_LoopTaskCtx* MR_EventLoopAddTaskWithDelay(EventLoopTaskCallback callback, void* ctx, size_t delayMs) {
    MR_LoopTaskCtx* taskCtx = MR_ALLOC(sizeof(*taskCtx));
    taskCtx->callback = callback;
    taskCtx->ctx = ctx;
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
    MR_LoopTaskCtx* taskCtx = MR_ALLOC(sizeof(*taskCtx));
    taskCtx->callback = callback;
    taskCtx->ctx = ctx;
    taskCtx->event = event_new(evLoopCtx.loop,
                                    -1,
                                    0,
                                    MR_NewTask,
                                    taskCtx);
    event_active(taskCtx->event, 0, 0);
}

static void* MR_Loop(void *arg){
    while (1) {
        event_base_loop(evLoopCtx.loop, EVLOOP_NO_EXIT_ON_EMPTY);
    }
    return NULL;
}

struct event_base* MR_EventLoopGet() {
    return evLoopCtx.loop;
}

void MR_EventLoopStart() {
    evthread_use_pthreads();
    evLoopCtx.loop = (struct event_base*)event_base_new();
    pthread_create(&evLoopCtx.loopThread, NULL, MR_Loop, NULL);
}
