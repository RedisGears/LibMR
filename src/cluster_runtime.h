/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef LIBMR_CLUSTER_RUNTIME_H
#define LIBMR_CLUSTER_RUNTIME_H

#include <stdbool.h>

static inline bool MR_IsEnterpriseRuntime(bool rlecVersionPresent,
                                          bool clusterEnabled,
                                          bool managedNativeCluster) {
    return rlecVersionPresent &&
           (!clusterEnabled || managedNativeCluster);
}

#endif
