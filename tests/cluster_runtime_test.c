/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "../src/cluster_runtime.h"

#include <assert.h>

int main(void) {
    /* OSS binaries always use the OSS transport. */
    assert(!MR_IsEnterpriseRuntime(false, false, false));
    assert(!MR_IsEnterpriseRuntime(false, true, false));
    assert(!MR_IsEnterpriseRuntime(false, true, true));

    /* Classic Enterprise uses the proxy-filtered transport. */
    assert(MR_IsEnterpriseRuntime(true, false, false));

    /* Preserve the standalone Enterprise-binary OSS-cluster test setup. */
    assert(!MR_IsEnterpriseRuntime(true, true, false));

    /* Enterprise-managed native clusters use the proxy-filtered transport. */
    assert(MR_IsEnterpriseRuntime(true, true, true));
    return 0;
}
