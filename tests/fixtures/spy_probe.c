/*
 * A reference client that reads a local publication through `aeron-spy:`.
 *
 * The samples shipped with the reference never use the scheme — a
 * `BasicSubscriber` subscribes to a channel and reads what arrives on the wire
 * — so the only way to ask "does this driver serve `aeron-spy:` to a client
 * that shares no code with it?" is to be that client. This is it: linked
 * against the reference's own `libaeron` and pointed at whatever driver it is
 * given with `-d`.
 *
 * It is self-contained on purpose: it publishes, it subscribes to its own
 * channel so that the publication has a receiver to send to, and it spies on
 * the same channel with the prefix. What it prints is what it read *through
 * the spy* — a fragment the driver put in front of it from the publication's
 * log buffer rather than from a socket — and it exits 0 only if it read every
 * message it published, in order.
 *
 *     READY                       the spy has an image and the loop begins
 *     FRAGMENT <index>            one line per fragment the spy delivered
 *     DONE <count>                all of them, in order
 *
 * Every payload is `PAYLOAD_LENGTH` bytes whose first eight are the little
 * endian index of the message, so a fragment that arrives twice, out of order
 * or truncated is a failure rather than a count that happens to add up.
 *
 * Usage: -d <aeron dir> -c <channel> -s <stream id> [-n <messages>]
 */

#include <inttypes.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <aeronc.h>

#define PAYLOAD_LENGTH 1024
#define DEFAULT_MESSAGES 200

static volatile sig_atomic_t running = 1;

static int64_t next_expected = 0;
static int64_t received = 0;
static int out_of_order = 0;

static void handle_signal(int number)
{
    (void)number;
    running = 0;
}

/* What the spy delivered, checked as it arrives. */
static void on_spy_fragment(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)clientd;
    (void)header;

    int64_t index = 0;

    if (PAYLOAD_LENGTH != length || out_of_order)
    {
        out_of_order = 1;
        return;
    }

    memcpy(&index, buffer, sizeof(index));

    if (index != next_expected)
    {
        out_of_order = 1;
        return;
    }

    next_expected++;
    received++;
    printf("FRAGMENT %" PRId64 "\n", index);
    fflush(stdout);
}

/*
 * What the ordinary subscriber delivered — the same messages, arrived on the
 * wire rather than read out of the publication's buffer. It is polled so that
 * the publication's window keeps opening, and it is deliberately counted
 * *nowhere*: the two readers see the same stream, and one counter between them
 * would report every message twice.
 */
static void on_subscriber_fragment(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)clientd;
    (void)buffer;
    (void)length;
    (void)header;
}

static int usage(const char *program)
{
    fprintf(
        stderr,
        "Usage: %s -d <aeron dir> -c <channel> -s <stream id> [-n <messages>]\n",
        program);

    return 2;
}

/* A payload whose first eight bytes are the message's index. */
static void fill(uint8_t *payload, int64_t index)
{
    memset(payload, 0xA5, PAYLOAD_LENGTH);
    memcpy(payload, &index, sizeof(index));
}

/* Bring a subscription up, pumping the client conductor while it is made. */
static aeron_subscription_t *add_subscription(
    aeron_t *aeron, const char *channel, int32_t stream_id, int *ok)
{
    aeron_async_add_subscription_t *async = NULL;
    aeron_subscription_t *subscription = NULL;

    if (aeron_async_add_subscription(&async, aeron, channel, stream_id, NULL, NULL, NULL, NULL) < 0)
    {
        fprintf(stderr, "aeron_async_add_subscription: %s\n", aeron_errmsg());
        *ok = 0;
        return NULL;
    }

    while (running && NULL == subscription)
    {
        if (aeron_async_add_subscription_poll(&subscription, async) < 0)
        {
            fprintf(stderr, "aeron_async_add_subscription_poll: %s\n", aeron_errmsg());
            *ok = 0;
            return NULL;
        }

        aeron_main_do_work(aeron);
    }

    if (NULL == subscription)
    {
        fprintf(stderr, "the subscription never appeared\n");
        *ok = 0;
    }

    return subscription;
}

int main(int argc, char **argv)
{
    const char *dir = NULL;
    const char *channel = NULL;
    int32_t stream_id = 0;
    int64_t messages = DEFAULT_MESSAGES;
    int option;
    int status = 1;
    int ok = 1;

    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_publication_t *add_publication = NULL;
    aeron_publication_t *publication = NULL;
    aeron_subscription_t *subscriber = NULL;
    aeron_subscription_t *spy = NULL;
    char *spy_channel = NULL;
    uint8_t payload[PAYLOAD_LENGTH];

    while ((option = getopt(argc, argv, "d:c:s:n:")) != -1)
    {
        switch (option)
        {
            case 'd':
                dir = optarg;
                break;

            case 'c':
                channel = optarg;
                break;

            case 's':
                stream_id = (int32_t)strtoul(optarg, NULL, 0);
                break;

            case 'n':
                messages = (int64_t)strtoll(optarg, NULL, 0);
                break;

            default:
                return usage(argv[0]);
        }
    }

    if (NULL == dir || NULL == channel || 0 == stream_id)
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

    if (aeron_async_add_publication(&add_publication, aeron, channel, stream_id) < 0)
    {
        fprintf(stderr, "aeron_async_add_publication: %s\n", aeron_errmsg());
        goto cleanup;
    }

    while (running && NULL == publication)
    {
        if (aeron_async_add_publication_poll(&publication, add_publication) < 0)
        {
            fprintf(stderr, "aeron_async_add_publication_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }

        aeron_main_do_work(aeron);
    }

    if (NULL == publication)
    {
        fprintf(stderr, "the publication never appeared\n");
        goto cleanup;
    }

    /* The receiver: without one a unicast publication has nowhere to send, so
     * the producer's window never opens and there is nothing to spy on. */
    subscriber = add_subscription(aeron, channel, stream_id, &ok);

    if (NULL == subscriber)
    {
        goto cleanup;
    }

    spy_channel = malloc(strlen(channel) + 16);
    if (NULL == spy_channel)
    {
        fprintf(stderr, "out of memory\n");
        goto cleanup;
    }

    snprintf(spy_channel, strlen(channel) + 16, "aeron-spy:%s", channel);

    spy = add_subscription(aeron, spy_channel, stream_id, &ok);

    if (NULL == spy)
    {
        goto cleanup;
    }

    /* The spy's image comes from the driver, and until it does there is
     * nothing to read: a publication that has not been linked yet is not a
     * failure, it is a wait. */
    while (running && 0 == aeron_subscription_image_count(spy))
    {
        aeron_main_do_work(aeron);
    }

    if (0 == aeron_subscription_image_count(spy))
    {
        fprintf(stderr, "the spy was never given an image\n");
        goto cleanup;
    }

    printf("READY\n");
    fflush(stdout);

    int64_t offered = 0;

    while (running && received < messages)
    {
        while (offered < messages)
        {
            fill(payload, offered);

            if (aeron_publication_offer(publication, payload, PAYLOAD_LENGTH, NULL, NULL) < 0)
            {
                break;
            }

            offered++;
        }

        /* The receiver has to be polled as well, and not for tidiness: what
         * the publication's window is computed from is the positions its
         * readers report, so a reader left unpolled holds the producer back
         * and the spy never sees the last message. */
        aeron_subscription_poll(subscriber, on_subscriber_fragment, NULL, 10);
        aeron_subscription_poll(spy, on_spy_fragment, NULL, 10);
        aeron_main_do_work(aeron);
    }

    if (out_of_order)
    {
        fprintf(stderr, "a fragment was the wrong length or out of order\n");
        goto cleanup;
    }

    if (received != messages)
    {
        fprintf(stderr, "the spy read %" PRId64 " of %" PRId64 "\n", received, messages);
        goto cleanup;
    }

    printf("DONE %" PRId64 "\n", received);
    fflush(stdout);
    status = 0;

cleanup:
    free(spy_channel);

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
