# Cluster codec goldens (schema 111, version 18)

Two messages encoded by the **reference's own** generated Java codecs from
`aeron-all-1.54.2.jar`, with no edit of any kind. They cover the fields that
schema 18 added:

| File | Message | Values |
|---|---|---|
| `001-023-cluster-action-request.bin` | `ClusterActionRequest` (23), block 40 | term 5, log position 1024, timestamp 7, `SNAPSHOT`, flags 0, **term base 4096** |
| `003-100-snapshot-marker.bin` | `SnapshotMarker` (100), block 48 | type 1, log position 1024, term 5, index 0, `BEGIN`, milliseconds, app version 7, **term base 4096** |

Generated on 2026-11-26 by `GenerateCluster.java` (fork `aeron-deepmsg`, branch
`deepmsg-tests`, directory `deepmsg-tests/generators/sbe-cluster/`), compiled
and run against the same jar:

    javac -proc:none -cp aeron-all-1.54.2.jar -d /tmp/gencluster GenerateCluster.java
    java --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
         --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
         -cp aeron-all-1.54.2.jar:/tmp/gencluster GenerateCluster <out-dir>

`tests/cluster_codec_golden.rs` compares this build's generated encoder bytes
against them and decodes them with this build's decoder. A mismatch is this
build's generator wiring, not the reference's opinion.
