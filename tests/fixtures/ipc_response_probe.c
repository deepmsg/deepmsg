/*
 * Does an IPC channel that carries `response-correlation-id` still reach a
 * plain `aeron:ipc` subscription?
 *
 * P2-0c measured that both of the reference's suites fail on
 * `aeron:ipc?control-mode=response` while passing on every UDP variant of the
 * same case, and that the two suites agree — a Java client with the archive
 * in-process and a C client with the archive in a process of its own. What the
 * client library does on that path is one thing, and it is small
 * (`AeronArchive.java:4043-4048`): when the response channel is a response
 * channel, it rewrites the *request* channel `aeron:ipc` into
 * `aeron:ipc?response-correlation-id=<registration id of the response
 * subscription>`. The driver pairs the two ends by that id
 * (`aeron_driver_conductor.c:1714-1729`, which needs both publications to name
 * each other's registrations).
 *
 * So the request publication that has to reach the archive's plain `aeron:ipc`
 * subscription carries one parameter a plain channel does not. If this driver
 * counts that parameter as part of the channel's identity, the two never meet
 * and the ConnectRequest is not delivered at all — which is exactly what a
 * connect timeout looks like from the client's side.
 *
 * This probe asks that one question of whichever driver it is pointed at. The
 * first case is the control: it differs from the second by the parameter and
 * nothing else, so the reading is the *difference* between two runs of this
 * program rather than any one number in it.
 *
 *     CASE plain         pub=aeron:ipc                                    -> sub=aeron:ipc
 *     CASE correlation   pub=aeron:ipc?response-correlation-id=<id>       -> sub=aeron:ipc
 *     CASE response      pub=aeron:ipc?control-mode=response|response-…   -> sub=aeron:ipc
 *     CASE sub-response  pub=aeron:ipc                                    -> sub=aeron:ipc?control-mode=response
 *
 * and then the chain those four only narrow down: a response subscription, the
 * request publication that names it, and the response publication that names
 * the request — which is what the archive actually builds. See `run_chain_case`.
 *
 * Each case gets a stream of its own, so a case cannot be read as another's
 * result. It exits 0 whenever it produced readings at all: a delivery is not a
 * verdict, the comparison is.
 *
 *     CASE <name> pub=<channel|failed:…> sub=<channel|failed:…> delivered=<0|1>
 *     DONE
 *
 * Usage: -d <aeron dir> [-s <first stream id>]
 */

#include <inttypes.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <aeronc.h>

#define PAYLOAD_LENGTH 64
#define DEFAULT_FIRST_STREAM_ID 7100

/* 200 polls of the subscription, 10 ms apart, is two seconds — long enough for
 * a link that is going to be made, short enough that four cases of a link that
 * is not cost eight seconds in a reading. */
#define POLLS_PER_CASE 200
#define POLL_SLEEP_US 10000

static volatile sig_atomic_t running = 1;
static int64_t delivered = 0;

static void handle_signal(int number)
{
    (void)number;
    running = 0;
}

static void on_fragment(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)clientd;
    (void)buffer;
    (void)length;
    (void)header;
    delivered++;
}

static int usage(const char *program)
{
    fprintf(stderr, "Usage: %s -d <aeron dir> [-s <first stream id>]\n", program);

    return 2;
}

/* Bring a subscription up, pumping the client conductor while it is made. */
static aeron_subscription_t *add_subscription(
    aeron_t *aeron, const char *channel, int32_t stream_id)
{
    aeron_async_add_subscription_t *async = NULL;
    aeron_subscription_t *subscription = NULL;

    if (aeron_async_add_subscription(&async, aeron, channel, stream_id, NULL, NULL, NULL, NULL) < 0)
    {
        fprintf(stderr, "aeron_async_add_subscription(%s): %s\n", channel, aeron_errmsg());
        return NULL;
    }

    while (running && NULL == subscription)
    {
        if (aeron_async_add_subscription_poll(&subscription, async) < 0)
        {
            fprintf(stderr, "aeron_async_add_subscription_poll: %s\n", aeron_errmsg());
            return NULL;
        }

        aeron_main_do_work(aeron);
    }

    return subscription;
}

static aeron_publication_t *add_publication(aeron_t *aeron, const char *channel, int32_t stream_id)
{
    aeron_async_add_publication_t *async = NULL;
    aeron_publication_t *publication = NULL;

    if (aeron_async_add_publication(&async, aeron, channel, stream_id) < 0)
    {
        fprintf(stderr, "aeron_async_add_publication(%s): %s\n", channel, aeron_errmsg());
        return NULL;
    }

    while (running && NULL == publication)
    {
        if (aeron_async_add_publication_poll(&publication, async) < 0)
        {
            fprintf(stderr, "aeron_async_add_publication_poll: %s\n", aeron_errmsg());
            return NULL;
        }

        aeron_main_do_work(aeron);
    }

    return publication;
}

static int64_t registration_id_of(aeron_subscription_t *subscription)
{
    aeron_subscription_constants_t constants;

    memset(&constants, 0, sizeof(constants));

    if (aeron_subscription_constants(subscription, &constants) < 0)
    {
        fprintf(stderr, "aeron_subscription_constants: %s\n", aeron_errmsg());
        return 0;
    }

    return constants.registration_id;
}

static int64_t publication_registration_id(aeron_publication_t *publication)
{
    aeron_publication_constants_t constants;

    memset(&constants, 0, sizeof(constants));

    if (aeron_publication_constants(publication, &constants) < 0)
    {
        fprintf(stderr, "aeron_publication_constants: %s\n", aeron_errmsg());
        return 0;
    }

    return constants.registration_id;
}

typedef struct
{
    const char *name;

    /* `%lld` is substituted with the subscription's registration id when
     * `pub_names_registration_id` is set, because that is what the client
     * library writes onto the request channel and the parameter's value is not
     * what is being asked about — its presence is. */
    const char *pub_channel;
    const char *sub_channel;
    int pub_names_registration_id;
}
probe_case_t;

static const probe_case_t CASES[] = {
    {"plain", "aeron:ipc", "aeron:ipc", 0},
    {"correlation", "aeron:ipc?response-correlation-id=%lld", "aeron:ipc", 1},
    {
        "response",
        "aeron:ipc?control-mode=response|response-correlation-id=%lld",
        "aeron:ipc",
        1
    },
    {"sub-response", "aeron:ipc", "aeron:ipc?control-mode=response", 0},
};

static const size_t CASE_COUNT = sizeof(CASES) / sizeof(CASES[0]);

/* One case, one line. Both channel strings are printed with the outcome, so
 * that what was actually asked is in the record rather than only the answer. */
static void run_case(aeron_t *aeron, const probe_case_t *probe, int32_t stream_id)
{
    char pub_channel[256];
    uint8_t payload[PAYLOAD_LENGTH];
    aeron_subscription_t *subscription;
    aeron_publication_t *publication;
    int offered = 0;
    int64_t id;

    memset(payload, 0xA5, sizeof(payload));

    /* The subscription first: one case names its registration id. */
    subscription = add_subscription(aeron, probe->sub_channel, stream_id);

    if (NULL == subscription)
    {
        printf(
            "CASE %s pub=n/a sub=%s failed:never-appeared delivered=0\n",
            probe->name,
            probe->sub_channel);
        fflush(stdout);
        return;
    }

    if (probe->pub_names_registration_id)
    {
        id = registration_id_of(subscription);
        snprintf(pub_channel, sizeof(pub_channel), probe->pub_channel, (long long)id);
    }
    else
    {
        snprintf(pub_channel, sizeof(pub_channel), "%s", probe->pub_channel);
    }

    publication = add_publication(aeron, pub_channel, stream_id);

    if (NULL == publication)
    {
        printf(
            "CASE %s pub=%s failed:never-appeared sub=%s delivered=0\n",
            probe->name,
            pub_channel,
            probe->sub_channel);
        fflush(stdout);
        aeron_subscription_close(subscription, NULL, NULL);
        return;
    }

    delivered = 0;

    for (int i = 0; i < POLLS_PER_CASE && running && 0 == delivered; i++)
    {
        /* **One** offer, retried while the channel has no link yet: a
         * publication with no subscriber answers NOT_CONNECTED, which is a wait
         * and not a result. Offering every time round was the first version,
         * and it delivered two fragments on a case whose question is whether
         * anything arrives at all — a count that says more about this loop than
         * about the driver. */
        if (0 == offered && aeron_publication_offer(publication, payload, PAYLOAD_LENGTH, NULL, NULL) > 0)
        {
            offered = 1;
        }

        aeron_subscription_poll(subscription, on_fragment, NULL, 10);
        aeron_main_do_work(aeron);

        if (0 == delivered)
        {
            usleep(POLL_SLEEP_US);
        }
    }

    printf(
        "CASE %s pub=%s sub=%s delivered=%" PRId64 "\n",
        probe->name,
        pub_channel,
        probe->sub_channel,
        delivered);
    fflush(stdout);

    aeron_publication_close(publication, NULL, NULL);
    aeron_subscription_close(subscription, NULL, NULL);
}

/* The chain the archive actually builds — and the reading this probe exists
 * for. The four simple cases above say a response *subscription* is not fed by
 * a plain publication, in either driver; this says what does feed it.
 *
 *     S  response subscription   aeron:ipc?control-mode=response                   (stream R)
 *     P  request publication     aeron:ipc?response-correlation-id=<S's id>        (stream C)
 *     Q  response publication    aeron:ipc?control-mode=response|response-correlation-id=<P's id>  (stream R)
 *
 * P is what the client makes (`AeronArchive.java:4043-4048` puts S's
 * registration id onto the request channel) and Q is what the archive makes
 * when a ConnectRequest arrives, naming the request publication it came from.
 * The driver is supposed to pair the three by those ids
 * (`aeron_driver_conductor.c:1714-1729`), and then a fragment offered on Q
 * reaches S — which is the whole of the archive client's connect path, minus
 * the archive.
 *
 * Built in that order because each id is needed to write the next channel. */
static void run_chain_case(
    aeron_t *aeron,
    const char *name,
    int32_t response_stream_id,
    int32_t request_stream_id,
    const char *response_extra)
{
    const char *sub_channel = "aeron:ipc?control-mode=response";
    char request_channel[256];
    char response_channel[256];
    uint8_t payload[PAYLOAD_LENGTH];
    aeron_subscription_t *subscription;
    aeron_publication_t *request;
    aeron_publication_t *response;
    int64_t sub_id;
    int64_t request_id;
    int offered = 0;

    memset(payload, 0xA5, sizeof(payload));

    subscription = add_subscription(aeron, sub_channel, response_stream_id);

    if (NULL == subscription)
    {
        printf("CASE %s sub=%s failed:never-appeared delivered=0\n", name, sub_channel);
        fflush(stdout);
        return;
    }

    sub_id = registration_id_of(subscription);
    snprintf(request_channel, sizeof(request_channel), "aeron:ipc?response-correlation-id=%lld", (long long)sub_id);

    request = add_publication(aeron, request_channel, request_stream_id);

    if (NULL == request)
    {
        printf("CASE %s sub=%s req=%s failed:never-appeared delivered=0\n", name, sub_channel, request_channel);
        fflush(stdout);
        aeron_subscription_close(subscription, NULL, NULL);
        return;
    }

    request_id = publication_registration_id(request);
    snprintf(
        response_channel,
        sizeof(response_channel),
        "aeron:ipc?control-mode=response%s|response-correlation-id=%lld",
        response_extra,
        (long long)request_id);

    response = add_publication(aeron, response_channel, response_stream_id);

    if (NULL == response)
    {
        printf(
            "CASE %s sub=%s req=%s resp=%s failed:never-appeared delivered=0\n",
            name,
            sub_channel,
            request_channel,
            response_channel);
        fflush(stdout);
        aeron_publication_close(request, NULL, NULL);
        aeron_subscription_close(subscription, NULL, NULL);
        return;
    }

    delivered = 0;

    for (int i = 0; i < POLLS_PER_CASE && running && 0 == delivered; i++)
    {
        if (0 == offered &&
            aeron_publication_offer(response, payload, PAYLOAD_LENGTH, NULL, NULL) > 0)
        {
            offered = 1;
        }

        aeron_subscription_poll(subscription, on_fragment, NULL, 10);
        aeron_main_do_work(aeron);

        if (0 == delivered)
        {
            usleep(POLL_SLEEP_US);
        }
    }

    printf(
        "CASE %s sub=%s req=%s resp=%s delivered=%" PRId64 "\n",
        name,
        sub_channel,
        request_channel,
        response_channel,
        delivered);
    fflush(stdout);

    aeron_publication_close(response, NULL, NULL);
    aeron_publication_close(request, NULL, NULL);
    aeron_subscription_close(subscription, NULL, NULL);
}

/* The value the archive actually pairs on, read from the driver rather than
 * computed here.
 *
 * `ArchiveConductor.java:478` writes `image.correlationId()` onto the response
 * publication, not the request publication's registration id — and an image's
 * correlation id is the figure this driver gave it when it linked the two. The
 * chain cases above cannot see a difference in that number, because they put
 * the publication's own registration id on both sides and are self-consistent
 * whatever the driver reports. This reads it: the image's correlation id has to
 * be the publication's registration id, or the archive will name a publication
 * that does not exist and the client's response subscription will wait for a
 * fragment that is addressed to nobody. */
static void run_image_correlation_case(aeron_t *aeron, int32_t stream_id)
{
    const char *channel = "aeron:ipc";
    aeron_subscription_t *subscription;
    aeron_publication_t *publication;
    aeron_image_t *image;
    aeron_image_constants_t constants;
    int64_t publication_id;
    int waited = 0;

    subscription = add_subscription(aeron, channel, stream_id);

    if (NULL == subscription)
    {
        printf("CASE image-correlation sub failed:never-appeared\n");
        fflush(stdout);
        return;
    }

    publication = add_publication(aeron, channel, stream_id);

    if (NULL == publication)
    {
        printf("CASE image-correlation pub failed:never-appeared\n");
        fflush(stdout);
        aeron_subscription_close(subscription, NULL, NULL);
        return;
    }

    publication_id = publication_registration_id(publication);

    for (waited = 0; waited < POLLS_PER_CASE && running; waited++)
    {
        if (aeron_subscription_image_count(subscription) > 0)
        {
            break;
        }

        aeron_main_do_work(aeron);
        usleep(POLL_SLEEP_US);
    }

    image = aeron_subscription_image_at_index(subscription, 0);

    if (NULL == image)
    {
        printf(
            "CASE image-correlation pub_registration=%lld image=never-appeared\n",
            (long long)publication_id);
        fflush(stdout);
        aeron_publication_close(publication, NULL, NULL);
        aeron_subscription_close(subscription, NULL, NULL);
        return;
    }

    memset(&constants, 0, sizeof(constants));

    if (aeron_image_constants(image, &constants) < 0)
    {
        fprintf(stderr, "aeron_image_constants: %s\n", aeron_errmsg());
    }

    printf(
        "CASE image-correlation pub_registration=%lld image_correlation=%lld match=%d\n",
        (long long)publication_id,
        (long long)constants.correlation_id,
        constants.correlation_id == publication_id);
    fflush(stdout);

    aeron_publication_close(publication, NULL, NULL);
    aeron_subscription_close(subscription, NULL, NULL);
}

int main(int argc, char **argv)
{
    const char *dir = NULL;
    int32_t first_stream_id = DEFAULT_FIRST_STREAM_ID;
    int option;
    int status = 1;

    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;

    while ((option = getopt(argc, argv, "d:s:")) != -1)
    {
        switch (option)
        {
            case 'd':
                dir = optarg;
                break;

            case 's':
                first_stream_id = (int32_t)strtoul(optarg, NULL, 0);
                break;

            default:
                return usage(argv[0]);
        }
    }

    if (NULL == dir)
    {
        return usage(argv[0]);
    }

    signal(SIGINT, handle_signal);

    if (aeron_context_init(&context) < 0)
    {
        fprintf(stderr, "aeron_context_init: %s\n", aeron_errmsg());
        goto cleanup;
    }

    if (aeron_context_set_dir(context, dir) < 0)
    {
        fprintf(stderr, "aeron_context_set_dir: %s\n", aeron_errmsg());
        goto cleanup;
    }

    if (aeron_init(&aeron, context) < 0)
    {
        fprintf(stderr, "aeron_init: %s\n", aeron_errmsg());
        goto cleanup;
    }

    if (aeron_start(aeron) < 0)
    {
        fprintf(stderr, "aeron_start: %s\n", aeron_errmsg());
        goto cleanup;
    }

    for (size_t i = 0; i < CASE_COUNT && running; i++)
    {
        run_case(aeron, &CASES[i], first_stream_id + (int32_t)i);
    }

    if (running)
    {
        /* The same chain with the parameters the archive actually puts on its
         * response publication: `strippedChannelBuilder` keeps `control-mode`
         * and the archive adds the term length, sparseness and MTU it resolved
         * (`ArchiveConductor.java:471-481`), where the plain `chain` above has
         * none of them. If the two readings differ, the parameters are the
         * difference. */
        run_chain_case(
            aeron,
            "chain",
            first_stream_id + (int32_t)CASE_COUNT,
            first_stream_id + (int32_t)CASE_COUNT + 1,
            "");
        run_chain_case(
            aeron,
            "chain-full",
            first_stream_id + (int32_t)CASE_COUNT + 2,
            first_stream_id + (int32_t)CASE_COUNT + 3,
            "|term-length=64k|sparse=true|mtu=1408");
        run_image_correlation_case(aeron, first_stream_id + (int32_t)CASE_COUNT + 4);
    }

    printf("DONE\n");
    fflush(stdout);
    status = 0;

cleanup:
    if (NULL != aeron)
    {
        aeron_close(aeron);
    }

    if (NULL != context)
    {
        aeron_context_close(context);
    }

    return status;
}
