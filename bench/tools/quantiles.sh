#!/usr/bin/env bash
#
# One row of quantiles per histogram file, in nanoseconds.
#
#   quantiles.sh [--skip N] [LABEL=PATH ...]
#
# A bare path is labelled with its basename, so that `quantiles.sh cell.hdr`
# works without a label being invented at every call site.
#
# The decoder is the harness's own HdrHistogram (`org.HdrHistogram`), taken from
# the jar its build produces — which is also the reason this reads the Java
# client's files and this build's files with the same code.
#
#   BENCHMARKS_DIR   where to find the harness and its jar (see lib/checkouts.sh)
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$here")
# shellcheck source=../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"

_classpath() {
    local jar
    jar=$(ls "$BENCHMARKS_DIR"/benchmarks-all/build/libs/benchmarks.jar 2>/dev/null | head -1)
    if [ -n "$jar" ]; then
        echo "$jar"
        return 0
    fi
    # The shaded jar is what a batch normally has; without it, the same classes
    # are in the Gradle cache and this still works.
    jar=$(ls "${HOME}"/.gradle/caches/modules-2/files-2.1/org.hdrhistogram/HdrHistogram/*/*/HdrHistogram-*.jar \
        "${HOME}"/.gradle/caches/modules-2/files-2.1/org.agrona/agrona/*/*/agrona-*.jar 2>/dev/null | tr '\n' ':')
    if [ -z "$jar" ]; then
        echo "no harness jar and no HdrHistogram in the Gradle cache" >&2
        echo "  build the harness first, or set BENCHMARKS_DIR (see bench/lib/checkouts.sh)" >&2
        return 1
    fi
    echo "${jar%:}"
}

# A path wins over `LABEL=PATH`, and it has to: the harness names its results
# `<file>_rate=<R>_batch=<B>_length=<L>.hdr` and the reference names them with
# more of the same, so `=` in an argument is far more often part of a file name
# than it is a label. Only what is not a file this shell can see is split.
args=()
for argument in "$@"; do
    if [ "$argument" = "--skip" ]; then
        args+=("$argument")
    elif [ "${argument#--}" != "$argument" ]; then
        args+=("$argument")             # --skip N is split by the reader itself
    elif [ -f "$argument" ] || [ -f "$argument.hdr" ]; then
        # A path: labelled here, and the label may not carry a `=` — the reader
        # splits `LABEL=PATH` at the first one, and a harness file name is full
        # of them (`..._rate=501K_batch=1_length=32.hdr`).
        local_label=$(basename "$argument" .hdr)
        args+=("${local_label//=/_}=$argument")
    else
        args+=("$argument")             # already LABEL=PATH, with no `=` in the label
    fi
done

exec java -cp "$(_classpath)" "$here/HdrQuantiles.java" "${args[@]}"
