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
#include <stdint.h>
#include <string.h>

#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
#include <rte_ring.h>

/*
 * The name a port answers to, as this file carries it. DPDK's own length is
 * `RTE_ETH_NAME_MAX_LEN`; the assertion below is what keeps the two from
 * drifting apart silently.
 */
#define SHIM_DRIVER_NAME_MAX 64

typedef struct {
    char driver[SHIM_DRIVER_NAME_MAX];
    uint16_t max_rx_queues;
    uint16_t max_tx_queues;
    uint16_t max_mtu;
    uint16_t min_mtu;
    int socket_id;
    uint64_t rx_offloads;
    uint64_t tx_offloads;
} shim_port_info_t;

_Static_assert(RTE_ETH_NAME_MAX_LEN <= SHIM_DRIVER_NAME_MAX,
    "the shim's driver-name buffer is shorter than DPDK's");

/*
 * A frame on the way in: the handle Rust owns, the bytes it reads, and the
 * NIC's verdict on the checksums — a three-valued answer computed *here*, so
 * that no DPDK bit-field is interpreted on the Rust side.
 */
#define SHIM_CKSUM_NONE 0u
#define SHIM_CKSUM_GOOD 1u
#define SHIM_CKSUM_BAD  2u

typedef struct {
    void *mbuf;
    unsigned char *data;
    uint16_t len;
    uint32_t checksum;
} shim_rx_desc;

/*
 * A frame on the way out: allocated by the shim, written by Rust, handed over
 * in one burst. `room` is what Rust may write, `len` is what it wrote, and
 * `ol_flags` is the offload it wants for this frame (zero when the port has
 * none, which is what the virtual devices have).
 */
typedef struct {
    void *mbuf;
    unsigned char *data;
    uint16_t room;
    uint16_t len;
    uint64_t ol_flags;
} shim_tx_slot;

/* The batch bound: one call carries at most this many frames. */
#define SHIM_BURST_MAX 64

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

/* ---- the pool the frames come from ------------------------------------ */

/*
 * `data_room` of zero means DPDK's default, which holds an MTU of 1500 with
 * its headers. `socket_id` is passed through as given: the ports D0 measured
 * answer `-1` for their socket, and `-1` *is* `SOCKET_ID_ANY`.
 */
void *shim_pool_create(const char *name, unsigned count, unsigned cache, unsigned data_room, int socket_id)
{
    unsigned room = (data_room == 0) ? RTE_MBUF_DEFAULT_BUF_SIZE : data_room;

    return rte_pktmbuf_pool_create(name, count, cache, 0, (uint16_t)room, socket_id);
}

void shim_pool_free(void *pool) { rte_mempool_free(pool); }

unsigned shim_pool_avail(void *pool)
{
    return rte_mempool_avail_count(pool);
}

/* ---- the port, and its queues ----------------------------------------- */

unsigned shim_port_count(void) { return rte_eth_dev_count_avail(); }

/* The port with this name, or -1. A name is a PCI address or a vdev's name. */
int shim_port_by_name(const char *name)
{
    uint16_t port;

    return (rte_eth_dev_get_port_by_name(name, &port) == 0) ? (int)port : -1;
}

void shim_port_name(unsigned port, char *out, unsigned cap)
{
    if (rte_eth_dev_get_name_by_port((uint16_t)port, out) != 0)
    {
        out[0] = '\0';
        return;
    }
    out[cap - 1] = '\0';
}

int shim_port_info(unsigned port, shim_port_info_t *out)
{
    struct rte_eth_dev_info info;
    int rc = rte_eth_dev_info_get((uint16_t)port, &info);

    if (rc != 0)
    {
        return rc;
    }
    memset(out, 0, sizeof(*out));
    if (NULL != info.driver_name)
    {
        snprintf(out->driver, sizeof(out->driver), "%s", info.driver_name);
    }
    out->max_rx_queues = info.max_rx_queues;
    out->max_tx_queues = info.max_tx_queues;
    out->max_mtu = info.max_mtu;
    out->min_mtu = info.min_mtu;
    out->socket_id = rte_eth_dev_socket_id((uint16_t)port);
    out->rx_offloads = info.rx_offload_capa;
    out->tx_offloads = info.tx_offload_capa;

    return 0;
}

int shim_port_configure(unsigned port, unsigned nb_rx, unsigned nb_tx, uint64_t rx_offloads, uint64_t tx_offloads)
{
    struct rte_eth_conf conf;

    memset(&conf, 0, sizeof(conf));
    conf.rxmode.offloads = rx_offloads;
    conf.txmode.offloads = tx_offloads;

    return rte_eth_dev_configure((uint16_t)port, (uint16_t)nb_rx, (uint16_t)nb_tx, &conf);
}

int shim_rx_queue_setup(unsigned port, unsigned queue, unsigned descriptors, void *pool)
{
    return rte_eth_rx_queue_setup(
        (uint16_t)port, (uint16_t)queue, (uint16_t)descriptors,
        rte_eth_dev_socket_id((uint16_t)port), NULL, pool);
}

int shim_tx_queue_setup(unsigned port, unsigned queue, unsigned descriptors)
{
    return rte_eth_tx_queue_setup(
        (uint16_t)port, (uint16_t)queue, (uint16_t)descriptors,
        rte_eth_dev_socket_id((uint16_t)port), NULL);
}

int shim_port_start(unsigned port) { return rte_eth_dev_start((uint16_t)port); }

void shim_port_stop(unsigned port) { rte_eth_dev_stop((uint16_t)port); }

void shim_port_close(unsigned port) { rte_eth_dev_close((uint16_t)port); }

int shim_port_mac(unsigned port, unsigned char *out)
{
    struct rte_ether_addr addr;
    int rc = rte_eth_macaddr_get((uint16_t)port, &addr);

    if (rc == 0)
    {
        memcpy(out, addr.addr_bytes, 6);
    }

    return rc;
}

int shim_port_mtu(unsigned port, uint16_t *out) { return rte_eth_dev_get_mtu((uint16_t)port, out); }

int shim_port_set_mtu(unsigned port, uint16_t mtu) { return rte_eth_dev_set_mtu((uint16_t)port, mtu); }

/* ---- the data path, in batches ---------------------------------------- */

/*
 * The NIC's verdict, or "not judged" for a port that has no checksum offload —
 * which is every virtual device this slice runs on.
 */
static uint32_t shim_checksum_of(const struct rte_mbuf *mbuf)
{
    if (0 != (mbuf->ol_flags & (RTE_MBUF_F_RX_IP_CKSUM_BAD | RTE_MBUF_F_RX_L4_CKSUM_BAD)))
    {
        return SHIM_CKSUM_BAD;
    }
    if (0 != (mbuf->ol_flags & (RTE_MBUF_F_RX_IP_CKSUM_GOOD | RTE_MBUF_F_RX_L4_CKSUM_GOOD)))
    {
        return SHIM_CKSUM_GOOD;
    }

    return SHIM_CKSUM_NONE;
}

/*
 * One call, up to `max` frames: the whole of what a pass pays to cross into
 * the PMD, whatever the burst carries.
 */
unsigned shim_rx_burst(unsigned port, unsigned queue, shim_rx_desc *out, unsigned max)
{
    struct rte_mbuf *mbufs[SHIM_BURST_MAX];
    unsigned n, i;

    if (max > SHIM_BURST_MAX)
    {
        max = SHIM_BURST_MAX;
    }
    n = rte_eth_rx_burst((uint16_t)port, (uint16_t)queue, mbufs, (uint16_t)max);
    for (i = 0; i < n; i++)
    {
        out[i].mbuf = mbufs[i];
        out[i].data = rte_pktmbuf_mtod(mbufs[i], unsigned char *);
        out[i].len = rte_pktmbuf_data_len(mbufs[i]);
        out[i].checksum = shim_checksum_of(mbufs[i]);
    }

    return n;
}

/*
 * The same, for frames that came back out of a ring: the handle crossed, the
 * bytes did not. One call for the whole batch, for the same reason as above.
 */
void shim_mbuf_info_bulk(void *const *mbufs, shim_rx_desc *out, unsigned count)
{
    unsigned i;

    for (i = 0; i < count; i++)
    {
        struct rte_mbuf *mbuf = mbufs[i];

        out[i].mbuf = mbuf;
        out[i].data = rte_pktmbuf_mtod(mbuf, unsigned char *);
        out[i].len = rte_pktmbuf_data_len(mbuf);
        out[i].checksum = shim_checksum_of(mbuf);
    }
}

/*
 * Frames to write into: the handle, the bytes Rust may fill, and how many of
 * them there are. Zero when the pool had none — back pressure, not an error,
 * which the caller answers by trying again next pass.
 */
unsigned shim_tx_alloc(void *pool, shim_tx_slot *out, unsigned count)
{
    struct rte_mbuf *mbufs[SHIM_BURST_MAX];
    unsigned i;

    if (count > SHIM_BURST_MAX)
    {
        count = SHIM_BURST_MAX;
    }
    if (0 != rte_pktmbuf_alloc_bulk(pool, mbufs, count))
    {
        return 0;
    }
    for (i = 0; i < count; i++)
    {
        out[i].mbuf = mbufs[i];
        out[i].data = rte_pktmbuf_mtod(mbufs[i], unsigned char *);
        out[i].room = (uint16_t)rte_pktmbuf_tailroom(mbufs[i]);
        out[i].len = 0;
        out[i].ol_flags = 0;
    }

    return count;
}

/*
 * Hand the written frames to the NIC and answer how many left.
 *
 * The ones it did not take are freed **here**: the caller's count is then the
 * truth about what left this machine, which is what a sender's position
 * advances by. Their slots are cleared so that nothing frees them twice.
 */
unsigned shim_tx_burst(unsigned port, unsigned queue, shim_tx_slot *slots, unsigned count)
{
    struct rte_mbuf *mbufs[SHIM_BURST_MAX];
    unsigned sent, i;

    if (count > SHIM_BURST_MAX)
    {
        count = SHIM_BURST_MAX;
    }
    for (i = 0; i < count; i++)
    {
        struct rte_mbuf *mbuf = slots[i].mbuf;

        mbuf->data_len = slots[i].len;
        mbuf->pkt_len = slots[i].len;
        mbuf->ol_flags |= slots[i].ol_flags;
        mbufs[i] = mbuf;
    }

    sent = rte_eth_tx_burst((uint16_t)port, (uint16_t)queue, mbufs, (uint16_t)count);
    if (sent < count)
    {
        rte_pktmbuf_free_bulk(&mbufs[sent], count - sent);
    }
    for (i = sent; i < count; i++)
    {
        slots[i].mbuf = NULL;
        slots[i].data = NULL;
        slots[i].room = 0;
        slots[i].len = 0;
    }

    return sent;
}

/* Give frames back to their pool. Rust owns the handles; this is the way out. */
void shim_mbuf_free_bulk(void **mbufs, unsigned count)
{
    rte_pktmbuf_free_bulk((struct rte_mbuf **)mbufs, count);
}

/* ---- a ring, for the frames that cross threads ------------------------ */

void *shim_ring_create(const char *name, unsigned capacity)
{
    return rte_ring_create(name, capacity, SOCKET_ID_ANY, RING_F_SP_ENQ | RING_F_SC_DEQ);
}

void shim_ring_free(void *ring) { rte_ring_free(ring); }

unsigned shim_ring_enqueue_burst(void *ring, void *const *mbufs, unsigned count)
{
    return rte_ring_sp_enqueue_burst(ring, mbufs, count, NULL);
}

unsigned shim_ring_dequeue_burst(void *ring, void **mbufs, unsigned count)
{
    return rte_ring_sc_dequeue_burst(ring, mbufs, count, NULL);
}
