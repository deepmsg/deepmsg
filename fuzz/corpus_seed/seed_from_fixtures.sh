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
# Schema 111, template 78 (backup response), block length 60, version 0.
crafted 08_sbe_cluster_codecs 01-backup-response-header '\074\000\116\000\157\000\000\000'
# A DATA frame header (32 bytes total) with no payload behind it.
crafted 05_udp_protocol_frames 01-data-header \
    '\040\000\000\000\000\300\001\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000'
