/*
 * A reference client that adds a destination to its own publication.
 *
 * The reference's shipped samples do not touch the destination API — a
 * `BasicSubscriber` subscribes, a `BasicPublisher` publishes, and neither adds
 * a destination to anything — so the only way to ask "does this driver serve
 * `ADD_DESTINATION` to a client that is not ours?" is to be that client. This
 * is it: linked against the reference's own `libaeron`, pointed at whatever
 * driver it is given with `-d`.
 *
 * It prints one line per thing it got and exits 0 only if it got all of them:
 *
 *     PUBLICATION <session id>
 *     DESTINATION <registration id>
 *
 * The session and not the registration id on the first line, because the
 * reference's C client has no accessor for the latter — it is what the driver
 * answered the `ADD_PUBLICATION` with, and the client keeps it to itself.
 *
 * The destination's registration id is the correlation id of the command that
 * asked for it (`aeron_client_conductor_on_operation_success` completes a
 * registering resource by matching that, and
 * `aeron_async_destination_get_registration_id` is documented as returning
 * "correlation_id sent to driver").
 *
 * `-S` asks for a **subscription** instead of a publication and adds the
 * destination as a source to it (`aeron_subscription_async_add_destination`):
 * the receive side of a multi-destination channel, where the receiver is the
 * one that has to speak first.
 *
 * Usage: -d <aeron dir> -c <channel> -s <stream id> -D <destination uri> [-S]
 */

#include <inttypes.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <aeronc.h>

static volatile sig_atomic_t running = 1;

static void handle_signal(int number)
{
    (void)number;
    running = 0;
}

static int usage(const char *program)
{
    fprintf(
        stderr,
        "Usage: %s -d <aeron dir> -c <channel> -s <stream id> -D <destination uri>\n",
        program);

    return 2;
}

int main(int argc, char **argv)
{
    const char *dir = NULL;
    const char *channel = NULL;
    const char *destination_uri = NULL;
    int32_t stream_id = 0;
    int as_subscription = 0;
    int option;
    int status = 1;

    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_publication_t *add_publication = NULL;
    aeron_async_add_subscription_t *add_subscription = NULL;
    aeron_async_destination_t *add_destination = NULL;
    aeron_publication_t *publication = NULL;
    aeron_subscription_t *subscription = NULL;

    while ((option = getopt(argc, argv, "d:c:s:D:S")) != -1)
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

            case 'D':
                destination_uri = optarg;
                break;

            case 'S':
                as_subscription = 1;
                break;

            default:
                return usage(argv[0]);
        }
    }

    if (NULL == dir || NULL == channel || NULL == destination_uri)
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

    if (as_subscription)
    {
        if (aeron_async_add_subscription(
                &add_subscription, aeron, channel, stream_id, NULL, NULL, NULL, NULL) < 0)
        {
            fprintf(stderr, "aeron_async_add_subscription: %s\n", aeron_errmsg());
            goto cleanup;
        }

        while (running && NULL == subscription)
        {
            if (aeron_async_add_subscription_poll(&subscription, add_subscription) < 0)
            {
                fprintf(stderr, "aeron_async_add_subscription_poll: %s\n", aeron_errmsg());
                goto cleanup;
            }

            aeron_main_do_work(aeron);
        }

        if (NULL == subscription)
        {
            fprintf(stderr, "the subscription never appeared\n");
            goto cleanup;
        }

        printf("SUBSCRIPTION\n");
        fflush(stdout);

        if (aeron_subscription_async_add_destination(
                &add_destination, aeron, subscription, destination_uri) < 0)
        {
            fprintf(stderr, "aeron_subscription_async_add_destination: %s\n", aeron_errmsg());
            goto cleanup;
        }
    }
    else
    {
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

        printf("PUBLICATION %" PRId32 "\n", aeron_publication_session_id(publication));
        fflush(stdout);

        if (aeron_publication_async_add_destination(&add_destination, aeron, publication, destination_uri) < 0)
        {
            fprintf(stderr, "aeron_publication_async_add_destination: %s\n", aeron_errmsg());
            goto cleanup;
        }
    }

    int polled = 0;
    while (running && 0 == polled)
    {
        polled = as_subscription
            ? aeron_subscription_async_destination_poll(add_destination)
            : aeron_publication_async_destination_poll(add_destination);

        if (polled < 0)
        {
            fprintf(stderr, "aeron_async_destination_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }

        aeron_main_do_work(aeron);
    }

    if (0 == polled)
    {
        fprintf(stderr, "the destination was never answered\n");
        goto cleanup;
    }

    printf("DESTINATION %" PRId64 "\n", aeron_async_destination_get_registration_id(add_destination));
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
