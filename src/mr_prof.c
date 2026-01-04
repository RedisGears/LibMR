/*
 * Minimal profiling counters for LibMR / RedisTimeSeries cluster execution.
 * Intended for debugging performance bottlenecks (scatter/gather, dispatch, keyspace lock, etc).
 */

#include "mr_prof.h"

#include <string.h>
#include <time.h>

static volatile int mrprof_enabled = 0;

// Keep as plain globals; updates are atomic via __atomic builtins.
static MRProfStageStat mrprof_stats[MRPROF_STAGE_MAX];

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
}

static inline uint64_t now_ns_if_enabled(void) {
    return MRProf_GetEnabled() ? now_ns() : 0;
}

int MRProf_GetEnabled(void) {
    return __atomic_load_n(&mrprof_enabled, __ATOMIC_RELAXED);
}

void MRProf_SetEnabled(int enabled) {
    __atomic_store_n(&mrprof_enabled, enabled ? 1 : 0, __ATOMIC_RELAXED);
}

void MRProf_Reset(void) {
    for (int i = 0; i < MRPROF_STAGE_MAX; ++i) {
        __atomic_store_n(&mrprof_stats[i].count, 0, __ATOMIC_RELAXED);
        __atomic_store_n(&mrprof_stats[i].total_ns, 0, __ATOMIC_RELAXED);
    }
}

void MRProf_GetSnapshot(MRProfSnapshot *out) {
    if (!out) {
        return;
    }
    out->enabled = MRProf_GetEnabled();
    for (int i = 0; i < MRPROF_STAGE_MAX; ++i) {
        out->stages[i].count = __atomic_load_n(&mrprof_stats[i].count, __ATOMIC_RELAXED);
        out->stages[i].total_ns = __atomic_load_n(&mrprof_stats[i].total_ns, __ATOMIC_RELAXED);
    }
}

void MRProf_AddDelta(MRProfStage stage, uint64_t delta_ns) {
    if (!delta_ns || !MRProf_GetEnabled()) {
        return;
    }
    if ((int)stage < 0 || (int)stage >= (int)MRPROF_STAGE_MAX) {
        return;
    }
    __atomic_add_fetch(&mrprof_stats[stage].count, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&mrprof_stats[stage].total_ns, delta_ns, __ATOMIC_RELAXED);
}

uint64_t MRProf_Begin(MRProfStage stage) {
    (void)stage;
    return now_ns_if_enabled();
}

void MRProf_End(MRProfStage stage, uint64_t start_ns) {
    if (!start_ns || !MRProf_GetEnabled()) {
        return;
    }
    if ((int)stage < 0 || (int)stage >= (int)MRPROF_STAGE_MAX) {
        return;
    }
    uint64_t delta = now_ns() - start_ns;
    __atomic_add_fetch(&mrprof_stats[stage].count, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&mrprof_stats[stage].total_ns, delta, __ATOMIC_RELAXED);
}


