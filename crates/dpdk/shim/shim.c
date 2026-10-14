/*
 * The C half of the kernel-bypass media seam (ADR-0006; ADR-0002 zone 6).
 *
 * DPDK's data path is `static inline` and its structures change between
 * versions, so the Rust side includes no DPDK header at all: what it calls are
 * the functions below, compiled here against the *installed* headers, and
 * declared in `src/sys.rs` with signatures that belong to this file rather than
 * to DPDK.
 *
 * Two rules hold for everything added to this file:
 *
 *   - **Nothing prints.** A C `printf` and Rust's stdout interleave (D0
 *     measured it). Failures come back as a return code plus `rte_errno` and
 *     are worded on the Rust side, where the error type lives.
 *   - **Nothing allocates or blocks that the caller did not ask for.** The
 *     control plane is called a handful of times at startup; the data path is
 *     batched so that a pass crosses this boundary a constant number of times,
 *     not once per frame.
 */
#define _GNU_SOURCE
#include <pthread.h>

#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_lcore.h>

/* ---- EAL, once per process ------------------------------------------- */

int shim_eal_init(int argc, char **argv) { return rte_eal_init(argc, argv); }

int shim_eal_cleanup(void) { return rte_eal_cleanup(); }

int shim_rte_errno(void) { return rte_errno; }

const char *shim_rte_strerror(int err) { return rte_strerror(err); }

/*
 * The main thread's affinity, saved and put back. EAL pins the main thread to
 * its main lcore at init; the driver's own affinity discipline (G4-2) decides
 * where its threads run, so the crate keeps this pair for the caller's
 * "restore me" path. With the `--lcores` argument the driver computes from its
 * own cpuset, it restores nothing that had moved (D0).
 */
static cpu_set_t shim_saved_affinity;

int shim_save_affinity(void)
{
    return pthread_getaffinity_np(pthread_self(), sizeof(shim_saved_affinity), &shim_saved_affinity);
}

int shim_restore_affinity(void)
{
    return pthread_setaffinity_np(pthread_self(), sizeof(shim_saved_affinity), &shim_saved_affinity);
}

/* ---- a thread that EAL does not know about ---------------------------- */

/*
 * Registering a `std::thread` with EAL gives it a lcore id, which is what turns
 * on the mempool's per-lcore cache for it: without this, every allocation goes
 * to the shared ring instead of the thread's own cache (R7). It does **not**
 * move the thread: the affinity the caller set stays (D0).
 */
int shim_thread_register(void) { return rte_thread_register(); }

void shim_thread_unregister(void) { rte_thread_unregister(); }

unsigned shim_lcore_id(void) { return rte_lcore_id(); }

unsigned shim_lcore_count(void) { return rte_lcore_count(); }
