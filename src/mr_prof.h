#pragma once

#include <stdint.h>

// Lightweight, always-compiled timing counters for diagnosing LibMR/RTS distributed query stages.
// Off by default; enable via MRProf_SetEnabled(1).

// IMPORTANT: Redis loads modules into a single process. Depending on dlopen flags,
// global symbols may be interposed across modules, causing profiling counters to
// "not reset" or "not update" if another module exports the same symbol names.
// Keep MRProf symbols hidden so they stay local to the module that links LibMR.
#ifndef MRPROF_API
#if defined(__GNUC__) || defined(__clang__)
#define MRPROF_API __attribute__((visibility("hidden")))
#else
#define MRPROF_API
#endif
#endif

typedef enum MRProfStage {
    MRPROF_STAGE_TS_CMD_ENTRY = 0,
    MRPROF_STAGE_TS_COORD_MERGE_REPLY,

    // Redis main thread: execution of timeseries.INNERCOMMUNICATION command handler
    MRPROF_STAGE_MAIN_INNERCOMM_CMD,

    MRPROF_STAGE_EL_SENDMSG_TASK,
    // timeseries-el: actual enqueue of the async command to hiredis (redisAsyncCommand*)
    MRPROF_STAGE_EL_SEND_ASYNC_CMD,
    MRPROF_STAGE_EL_INNERCOMM_DISPATCH,

    // time between enqueueing a worker task into the thread pool and actually starting its execution
    MRPROF_STAGE_WORKER_QUEUE_WAIT,
    MRPROF_STAGE_WORKER_DESERIALIZE_EXEC,
    MRPROF_STAGE_WORKER_SET_RECORD,
    MRPROF_STAGE_WORKER_STEP_DONE,

    // Time from just-before ThreadSafeContextLock until just-after unlock (includes wait + hold).
    MRPROF_STAGE_RTS_CTX_LOCK,
    // Time spent waiting to acquire the lock (blocked).
    MRPROF_STAGE_RTS_CTX_LOCK_WAIT,
    // Time holding the lock (critical section).
    MRPROF_STAGE_RTS_CTX_LOCK_HOLD,
    MRPROF_STAGE_RTS_QUERYINDEX,
    MRPROF_STAGE_RTS_GETSERIES_LOOP,

    MRPROF_STAGE_MAX,
} MRProfStage;

typedef struct MRProfStageStat {
    uint64_t count;
    uint64_t total_ns;
} MRProfStageStat;

typedef struct MRProfSnapshot {
    int enabled;
    MRProfStageStat stages[MRPROF_STAGE_MAX];
} MRProfSnapshot;

MRPROF_API int MRProf_GetEnabled(void);
MRPROF_API void MRProf_SetEnabled(int enabled);
MRPROF_API void MRProf_Reset(void);
MRPROF_API void MRProf_GetSnapshot(MRProfSnapshot *out);

// Add an already-measured delta (nanoseconds) to a stage.
MRPROF_API void MRProf_AddDelta(MRProfStage stage, uint64_t delta_ns);

// Use Begin/End to avoid overhead when disabled.
MRPROF_API uint64_t MRProf_Begin(MRProfStage stage);
MRPROF_API void MRProf_End(MRProfStage stage, uint64_t start_ns);


