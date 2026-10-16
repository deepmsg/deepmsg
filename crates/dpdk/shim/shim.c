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
 *   - **A call whose failure a caller reads clears `rte_errno` first.** DPDK
 *     leaves it alone on some failures and stale on others, and an error that
 *     says "Success" or names an earlier call's failure is worse than none;
 *     `rte_errno` is reset at the top of those calls so what comes back is
 *     theirs.
 */
#define _GNU_SOURCE
#include <errno.h>
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
 * What a port's own counters are, cut down to the eight this build reads:
 * `rte_eth_stats` also carries a per-queue breakdown whose length is a
 * build-time constant, and Rust never sees any of it.
 */
typedef struct {
    uint64_t ipackets;
    uint64_t opackets;
    uint64_t ibytes;
    uint64_t obytes;
    uint64_t imissed;
    uint64_t ierrors;
    uint64_t oerrors;
    uint64_t rx_nombuf;
} shim_port_stats_t;

/* The link's state, with the bitfields already resolved. */
typedef struct {
    uint32_t speed_mbps;
    uint8_t up;
    uint8_t full_duplex;
} shim_port_link_t;

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
 *
 * `l2_len` and `l3_len` are read only when `ol_flags` asks for a checksum the
 * NIC is to compute: DPDK's contract is that the mbuf carries the two header
 * lengths for an offloaded frame (`rte_mbuf_core.h:283-296`, "fill l2_len and
 * l3_len in mbuf"), and they are **mbuf bitfields**, so they are set here like
 * every other DPDK layout detail and never on the Rust side.
 */
typedef struct {
    void *mbuf;
    unsigned char *data;
    uint16_t room;
    uint16_t len;
    uint64_t ol_flags;
    uint16_t l2_len;
    uint16_t l3_len;
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

/*
 * The two receive offloads this build asks about, by DPDK's own numbering.
 *
 * Functions rather than numbers the Rust side carries, because the numbering
 * is DPDK's: an offload is an `RTE_BIT64(n)` in a header that moves between
 * versions, and what the driver above this crate wants is the yes-or-no answer
 * — "does this port judge IPv4 checksums" — not a bit position it would have
 * to keep in step with a library it never includes.
 */
uint64_t shim_rx_offload_ipv4_checksum(void) { return RTE_ETH_RX_OFFLOAD_IPV4_CKSUM; }

uint64_t shim_rx_offload_udp_checksum(void) { return RTE_ETH_RX_OFFLOAD_UDP_CKSUM; }

/* The same two questions about the transmit side, which are different bits. */
uint64_t shim_tx_offload_ipv4_checksum(void) { return RTE_ETH_TX_OFFLOAD_IPV4_CKSUM; }

uint64_t shim_tx_offload_udp_checksum(void) { return RTE_ETH_TX_OFFLOAD_UDP_CKSUM; }

/*
 * The per-frame flags, which are mbuf flags rather than queue offload bits,
 * and which say what the NIC is to do with *this* frame:
 *
 *   - `RTE_MBUF_F_TX_IPV4` tells it the packet is IPv4 at all — a PMD checks
 *     this one before it looks at the others;
 *   - `RTE_MBUF_F_TX_IP_CKSUM` is the IPv4 header's own checksum;
 *   - `RTE_MBUF_F_TX_UDP_CKSUM` is the L4 one, and setting it is a promise
 *     that the L4 checksum field holds the *pseudo-header* sum
 *     (`rte_net.h:89-96`), which is what the caller wrote there.
 */
uint64_t shim_mbuf_tx_ipv4(void) { return RTE_MBUF_F_TX_IPV4; }

uint64_t shim_mbuf_tx_ip_checksum(void) { return RTE_MBUF_F_TX_IP_CKSUM; }

uint64_t shim_mbuf_tx_udp_checksum(void) { return RTE_MBUF_F_TX_UDP_CKSUM; }

int shim_port_configure(unsigned port, unsigned nb_rx, unsigned nb_tx, uint64_t rx_offloads, uint64_t tx_offloads)
{
    struct rte_eth_conf conf;

    rte_errno = 0; /* this call's failure, not an earlier one's */
    memset(&conf, 0, sizeof(conf));
    conf.rxmode.offloads = rx_offloads;
    conf.txmode.offloads = tx_offloads;

    return rte_eth_dev_configure((uint16_t)port, (uint16_t)nb_rx, (uint16_t)nb_tx, &conf);
}

int shim_rx_queue_setup(unsigned port, unsigned queue, unsigned descriptors, void *pool)
{
    rte_errno = 0;

    return rte_eth_rx_queue_setup(
        (uint16_t)port, (uint16_t)queue, (uint16_t)descriptors,
        rte_eth_dev_socket_id((uint16_t)port), NULL, pool);
}

int shim_tx_queue_setup(unsigned port, unsigned queue, unsigned descriptors)
{
    rte_errno = 0;

    return rte_eth_tx_queue_setup(
        (uint16_t)port, (uint16_t)queue, (uint16_t)descriptors,
        rte_eth_dev_socket_id((uint16_t)port), NULL);
}

int shim_port_start(unsigned port)
{
    rte_errno = 0;

    return rte_eth_dev_start((uint16_t)port);
}

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

int shim_port_stats(unsigned port, shim_port_stats_t *out)
{
    struct rte_eth_stats stats;

    rte_errno = 0; /* this call's failure, not an earlier one's */
    if (0 != rte_eth_stats_get((uint16_t)port, &stats))
    {
        return -1;
    }
    out->ipackets = stats.ipackets;
    out->opackets = stats.opackets;
    out->ibytes = stats.ibytes;
    out->obytes = stats.obytes;
    out->imissed = stats.imissed;
    out->ierrors = stats.ierrors;
    out->oerrors = stats.oerrors;
    out->rx_nombuf = stats.rx_nombuf;

    return 0;
}

/*
 * A port's extended statistics, which are whatever its PMD chooses to expose —
 * 148 of them on the X540, by hardware register name (measured through
 * `show port xstats` on that card, D6). Two callers' facts are in the shape:
 *
 *   - **The names come back once**, at startup, because they do not change: a
 *     device's set of statistics is a property of the PMD. The values are read
 *     against that list, by index, on the timer.
 *   - **The read is by id and into the caller's array** —
 *     `rte_eth_xstats_get_by_id` with a null id list, which is DPDK's own
 *     "give me all of them, in name order" — so no `rte_eth_xstat` array is
 *     ever built here and nothing is allocated on a path a timer runs.
 *
 * Which of them are kept is not decided here: a name is a policy, is judged
 * without a port, and lives on the Rust side (`dpdk_counters.rs`).
 */
#define SHIM_XSTAT_NAME_MAX 64

typedef struct {
    char name[SHIM_XSTAT_NAME_MAX];
} shim_xstat_name_t;

/*
 * **Equality, not `<=`.** The caller's array is handed straight to DPDK, so the
 * only shape that is safe to hand over is one the two agree on exactly; a
 * DPDK that changed its name length fails this build rather than writing past
 * a buffer of ours.
 */
_Static_assert(RTE_ETH_XSTATS_NAME_SIZE == SHIM_XSTAT_NAME_MAX,
    "the shim's xstat-name buffer is not the length DPDK's names are");

/*
 * **A device that has none answers zero, not an error.** `-ENOTSUP` is DPDK's
 * way of saying "this PMD does not do extended statistics", and a media that
 * refused to open over it would be a media that could not run on a device with
 * no registers to report. The two cases are merged here, where DPDK's
 * convention is known, and Rust sees only a count.
 *
 * A **guard rather than a path**, and that is measured: the virtual devices this
 * build tests on do not take it. `net_pcap` and `net_null` each report the same
 * thirteen statistics (`show port xstats` on both, and the child case in
 * `crates/driver/tests/dpdk_port.rs` reads them back through this shim) —
 * DPDK hands a PMD that implements none of its own the basic set.
 */
int shim_port_xstats_count(unsigned port)
{
    int count;

    rte_errno = 0;
    count = rte_eth_xstats_get_names((uint16_t)port, NULL, 0);
    if (-ENOTSUP == count)
    {
        return 0;
    }

    return count;
}

/*
 * The names, in the order the values come back in. DPDK's answer when the array
 * is too small is the count it needed rather than a failure, which is what the
 * caller's `n` is checked against.
 */
int shim_port_xstats_names(unsigned port, shim_xstat_name_t *out, unsigned n)
{
    rte_errno = 0;
    return rte_eth_xstats_get_names((uint16_t)port, (struct rte_eth_xstat_name *)out, n);
}

int shim_port_xstats_values(unsigned port, uint64_t *out, unsigned n)
{
    rte_errno = 0;
    return rte_eth_xstats_get_by_id((uint16_t)port, NULL, out, n);
}

/*
 * The link's state. `rte_eth_link` is a bitfield union in this version of DPDK
 * — `link_status` is three bits of a `uint32_t` — which is the sort of thing
 * this shim exists to keep on the C side of the boundary.
 *
 * The `_nowait` call: a link that is still auto-negotiating makes the blocking
 * one wait for it, and this is read on a timer by a conductor that has other
 * things to do.
 */
int shim_port_link(unsigned port, shim_port_link_t *out)
{
    struct rte_eth_link link;

    rte_errno = 0;
    if (0 != rte_eth_link_get_nowait((uint16_t)port, &link))
    {
        return -1;
    }
    out->speed_mbps = link.link_speed;
    out->up = 0 != link.link_status;
    out->full_duplex = 0 != link.link_duplex;

    return 0;
}

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
        if (0 != slots[i].ol_flags)
        {
            /* The lengths are part of the offload's contract, and a frame
             * that asks for none carries nothing for them to describe. */
            mbuf->l2_len = slots[i].l2_len;
            mbuf->l3_len = slots[i].l3_len;
        }
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
        slots[i].ol_flags = 0;
        slots[i].l2_len = 0;
        slots[i].l3_len = 0;
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
