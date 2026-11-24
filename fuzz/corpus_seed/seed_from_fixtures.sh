#!/bin/sh
# Fill `fuzz/corpus/<target>/` from the committed seeds and the repository's
# own golden fixtures. Idempotent: an existing file is left alone, so a corpus
# the fuzzer has grown is never overwritten.
set -eu

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(dirname "$here")
fixtures="$root/../tests/fixtures"

seed_dir() {
    target=$1
    src="$here/$target"
    dst="$root/corpus/$target"
    [ -d "$src" ] || return 0
    mkdir -p "$dst"
    for file in "$src"/*; do
        [ -f "$file" ] || continue
        name=$(basename "$file")
        [ -e "$dst/$name" ] || cp "$file" "$dst/$name"
    done
    echo "$target: $(ls "$dst" | wc -l) seed(s)"
}

# The one file the fuzzer needs that is text: the URIs the driver is given.
seed_dir 01_channel_uri

# Golden bytes the repository already has.
mkdir -p "$root/corpus/06_userspace_packet"
for file in "$fixtures"/frame/*.bin; do
    [ -f "$file" ] || continue
    name=$(basename "$file")
    [ -e "$root/corpus/06_userspace_packet/$name" ] ||
        cp "$file" "$root/corpus/06_userspace_packet/$name"
done
echo "06_userspace_packet: $(ls "$root/corpus/06_userspace_packet" | wc -l) seed(s)"

# The CnC regions and the catalog the reference's own writers produced.
mkdir -p "$root/corpus/03_cnc_rings_and_logs"
for name in cnc-header.bin counters-metadata.bin; do
    [ -e "$root/corpus/03_cnc_rings_and_logs/$name" ] ||
        cp "$fixtures/$name" "$root/corpus/03_cnc_rings_and_logs/$name"
done
echo "03_cnc_rings_and_logs: $(ls "$root/corpus/03_cnc_rings_and_logs" | wc -l) seed(s)"

mkdir -p "$root/corpus/09_archive_catalog_segment"
[ -e "$root/corpus/09_archive_catalog_segment/archive.catalog" ] ||
    cp "$fixtures/catalog/archive.catalog" \
        "$root/corpus/09_archive_catalog_segment/archive.catalog"
echo "09_archive_catalog_segment: $(ls "$root/corpus/09_archive_catalog_segment" | wc -l) seed(s)"

# Crafted headers. The fuzzer has to find a schema id, a template id and a
# block length before any decoder is reached; these seeds put it there, and
# each is only the eight-byte SBE header — the truncation is the point.
crafted() {
    target=$1
    name=$2
    mkdir -p "$root/corpus/$target"
    [ -e "$root/corpus/$target/$name" ] || printf "$3" > "$root/corpus/$target/$name"
}

# Schema 101, template 61 (keep-alive), block length 16, version 12.
crafted 07_sbe_archive_codecs 01-keep-alive-header '\020\000\075\000\145\000\014\000'
# Schema 101, template 58 (auth-connect), block length 16, version 12.
crafted 07_sbe_archive_codecs 02-auth-connect-header '\020\000\072\000\145\000\014\000'

# One well-formed request per control template the adapter dispatches: an
# eight-byte header, the template's fixed block, and one zero-length prefix
# for each variable-length field. These are valid messages, so the fuzzer
# starts from the decode path rather than from the guard's rejection.
sbe_request() {
    name=$1 tid=$2 block=$3 vars=$4 version=${5:-14}
    path="$root/corpus/07_sbe_archive_codecs/$name"
    mkdir -p "$(dirname "$path")"
    if [ -e "$path" ]; then return 0; fi

    byte() { printf "\\$(printf '%03o' "$1")"; }

    {
        byte $((block & 255)); byte $((block >> 8))
        byte $((tid & 255)); byte $((tid >> 8))
        byte 101; byte 0
        byte $((version & 255)); byte $((version >> 8))
    } > "$path"

    i=0
    while [ "$i" -lt "$block" ]; do printf '\000' >> "$path"; i=$((i + 1)); done
    i=0
    while [ "$i" -lt "$vars" ]; do printf '\000\000\000\000' >> "$path"; i=$((i + 1)); done
}

sbe_request t003-close-session 3 8 0
sbe_request t004-start-recording 4 24 1
sbe_request t005-stop-recording 5 20 1
sbe_request t006-replay 6 56 1
sbe_request t007-stop-replay 7 24 0
sbe_request t008-list-recordings 8 28 0
sbe_request t009-list-recordings-for-uri 9 32 1
sbe_request t010-list-recording 10 24 0
sbe_request t012-recording-position 12 24 0
sbe_request t013-truncate-recording 13 32 0
sbe_request t014-stop-recording-subscription 14 24 0
sbe_request t015-stop-position 15 24 0
sbe_request t016-find-last-matching 16 32 1
sbe_request t017-list-recording-subscriptions 17 32 1
sbe_request t018-bounded-replay 18 60 1
sbe_request t019-stop-all-replays 19 24 0
sbe_request t051-stop-replication 51 24 0
sbe_request t052-start-position 52 24 0
sbe_request t053-detach-segments 53 32 0
sbe_request t054-delete-detached-segments 54 24 0
sbe_request t055-purge-segments 55 32 0
sbe_request t056-attach-segments 56 24 0
sbe_request t058-auth-connect 58 16 3
sbe_request t060-challenge-response 60 16 1
sbe_request t061-keep-alive 61 16 0
sbe_request t063-start-recording-2 63 28 1
sbe_request t064-extend-recording-2 64 36 1
sbe_request t065-stop-recording-by-identity 65 24 0
sbe_request t066-replicate-2 66 68 5
sbe_request t067-max-recorded-position 67 24 0
sbe_request t068-archive-id 68 16 0
sbe_request t104-purge-recording 104 24 0
sbe_request t105-replay-token 105 24 0
sbe_request t107-update-channel 107 24 1

# The same templates at the versions before their fields arrived: the fixed
# block is shorter, and the guard must still accept them.
sbe_request v06-replay-short-block 6 44 1 6
sbe_request v09-bounded-replay-short-block 18 52 1 9
sbe_request v07-replicate-2-before-session-id 66 64 3 7
sbe_request v11-auth-connect-before-client-info 58 16 2 11
# Schema 111, template 78 (backup response), block length 60, version 0.
crafted 08_sbe_cluster_codecs 01-backup-response-header '\074\000\116\000\157\000\000\000'

# One well-formed message per cluster template a production entry point
# decodes: header, the template's current fixed block, and one zero-length
# prefix per variable-length field. Group-bearing templates get their header
# only; the fuzzer walks the group from there.
cluster_message() {
    name=$1 tid=$2 block=$3 vars=$4
    path="$root/corpus/08_sbe_cluster_codecs/$name"
    mkdir -p "$(dirname "$path")"
    if [ -e "$path" ]; then return 0; fi

    byte() { printf "\\$(printf '%03o' "$1")"; }

    {
        byte $((block & 255)); byte $((block >> 8))
        byte $((tid & 255)); byte $((tid >> 8))
        byte 111; byte 0
        byte 17; byte 0
    } > "$path"

    i=0
    while [ "$i" -lt "$block" ]; do printf '\000' >> "$path"; i=$((i + 1)); done
    i=0
    while [ "$i" -lt "$vars" ]; do printf '\000\000\000\000' >> "$path"; i=$((i + 1)); done
}

cluster_message c001-session-message-header 1 24 0
cluster_message c002-session-event 2 44 1
cluster_message c003-session-connect 3 16 3
cluster_message c004-session-close 4 16 0
cluster_message c005-session-keep-alive 5 16 0
cluster_message c006-new-leader-event 6 20 1
cluster_message c007-challenge 7 16 1
cluster_message c008-challenge-response 8 16 1
cluster_message c026-admin-request 26 28 1
cluster_message c027-admin-response 27 24 2
cluster_message c030-close-session 30 8 0
cluster_message c031-schedule-timer 31 16 0
cluster_message c032-cancel-timer 32 8 0
cluster_message c033-service-ack 33 36 0
cluster_message c034-cluster-members-query 34 12 0
cluster_message c041-cluster-members-response 41 12 2
cluster_message c043-cluster-members-extended 43 24 0
cluster_message c050-canvass-position 50 32 0
cluster_message c051-request-vote 51 32 0
cluster_message c052-vote 52 36 0
cluster_message c053-new-leadership-term 53 96 0
cluster_message c054-append-position 54 21 0
cluster_message c055-commit-position 55 20 0
cluster_message c056-catchup-position 56 20 1
cluster_message c057-stop-catchup 57 12 0
cluster_message c075-termination-position 75 16 0
cluster_message c076-termination-ack 76 20 0
cluster_message c077-backup-query 77 24 2
cluster_message c078-backup-response 78 60 0
cluster_message c079-heartbeat-request 79 12 2
cluster_message c081-standby-snapshot 81 16 0
cluster_message c100-snapshot-marker 100 40 0
cluster_message c102-client-session 102 12 2
cluster_message c103-cluster-session 103 40 1
cluster_message c104-timer 104 16 0
cluster_message c105-consensus-module 105 28 0
cluster_message c107-pending-message-tracker 107 24 0

# One well-formed message per archive response template the client pollers
# decode: header, fixed block, one zero-length prefix per variable field.
archive_response() {
    name=$1 tid=$2 block=$3 vars=$4
    path="$root/corpus/10_archive_poller_events/$name"
    mkdir -p "$(dirname "$path")"
    if [ -e "$path" ]; then return 0; fi

    byte() { printf "\\$(printf '%03o' "$1")"; }

    {
        byte $((block & 255)); byte $((block >> 8))
        byte $((tid & 255)); byte $((tid >> 8))
        byte 101; byte 0
        byte 14; byte 0
    } > "$path"

    i=0
    while [ "$i" -lt "$block" ]; do printf '\000' >> "$path"; i=$((i + 1)); done
    i=0
    while [ "$i" -lt "$vars" ]; do printf '\000\000\000\000' >> "$path"; i=$((i + 1)); done
}

archive_response r001-control-response 1 32 1
archive_response r024-recording-signal-event 24 44 0
archive_response r022-recording-descriptor 22 80 3
archive_response r023-subscription-descriptor 23 28 1
archive_response r059-challenge 59 20 1
archive_response r101-recording-started 101 24 2
archive_response r102-recording-progress 102 24 0
archive_response r103-recording-stopped 103 24 0

# The two mark-file headers: schema id, template 200, the current fixed block,
# a semantic version (the block's first field), and one zero-length prefix per
# variable field.
mark_header() {
    target=$1 name=$2 schema=$3 block=$4 vars=$5 version=$6 semver=$7
    path="$root/corpus/$target/$name"
    mkdir -p "$(dirname "$path")"
    if [ -e "$path" ]; then return 0; fi

    byte() { printf "\\$(printf '%03o' "$1")"; }

    {
        byte $((block & 255)); byte $((block >> 8))
        byte 200; byte 0
        byte $((schema & 255)); byte $((schema >> 8))
        byte $((version & 255)); byte $((version >> 8))
        byte $((semver & 255)); byte $(((semver >> 8) & 255))
        byte $(((semver >> 16) & 255)); byte $(((semver >> 24) & 255))
    } > "$path"

    i=4
    while [ "$i" -lt "$block" ]; do printf '\000' >> "$path"; i=$((i + 1)); done
    i=0
    while [ "$i" -lt "$vars" ]; do printf '\000\000\000\000' >> "$path"; i=$((i + 1)); done
}

mark_header 13_mark_files cluster-mark 110 76 6 1 256
mark_header 13_mark_files archive-mark 100 56 4 2 196864

# A DATA frame header (32 bytes total) with no payload behind it.
crafted 05_udp_protocol_frames 01-data-header \
    '\040\000\000\000\000\300\001\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000'

# Every target needs a `corpus/<target>/` even when it has no seed: the smoke
# recipe hands libFuzzer an explicit corpus path, and libFuzzer refuses one
# that does not exist. The targets without crafted seeds (`02_logbuffer_scan`,
# `04_cnc_command_decode`, `11_cluster_recording_log`, `12_state_machine_rings`)
# had none, so the scheduled smoke failed on the first of them (2026-11-24);
# the pull-request tier only compiles the targets, so it never saw this.
#
# A target can also carry durable seeds of its own (`corpus_seed/<target>/`),
# which is where a crash from the nightly run is kept: the replay is what keeps
# the corpus from losing the shape that found it. `seed_dir` copies whatever
# is there, and does nothing for a target without one.
for target in "$root"/fuzz_targets/*.rs; do
    name=$(basename "$target" .rs)
    mkdir -p "$root/corpus/$name"
    seed_dir "$name"
done
