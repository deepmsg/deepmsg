package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Allocates real cluster counters on a real (embedded, Java) media driver and
 * dumps the metadata records the driver wrote, so the Rust encoders can be
 * compared against the reference's own bytes rather than against a reading of
 * its source.
 *
 * <p>Usage: {@code GenCounters <fixture-dir>}
 *
 * <p>Writes {@code counters-metadata.bin} (one 512-byte metadata record per
 * case, in the order of {@code counters.tsv}) and {@code counters.tsv}.
 *
 * <p>Only the metadata region is dumped. The values region carries the client
 * registration id, which changes every run and which none of the key, label or
 * finder contracts depend on; the Rust test supplies a zeroed values region of
 * the same record count.
 *
 * <p>The class lives in {@code io.aeron.cluster.service} because
 * {@code ClusterCounters.allocateServiceErrorCounter} is package-private. The
 * session counter is allocated through the real {@code ClusterSession} by
 * reflection, because {@code addSessionCounter} is private.
 */
public final class GenCounters
{
    private static final int CLUSTER_ID = 7;
    private static final int OTHER_CLUSTER_ID = 9;
    private static final int SERVICE_ID = 3;
    private static final String VERSION = "1.53.2";
    private static final String COMMIT = "664f58e705";

    private static final class Case
    {
        final String name;
        final int typeId;
        final int clusterId;
        final int serviceId;
        final int keyLength;
        final String version;
        final String commit;
        final int counterId;

        Case(
            final String name,
            final int typeId,
            final int clusterId,
            final int serviceId,
            final int keyLength,
            final String version,
            final String commit,
            final int counterId)
        {
            this.name = name;
            this.typeId = typeId;
            this.clusterId = clusterId;
            this.serviceId = serviceId;
            this.keyLength = keyLength;
            this.version = version;
            this.commit = commit;
            this.counterId = counterId;
        }
    }

    public static void main(final String[] args) throws Exception
    {
        final File dir = new File(args[0]);
        dir.mkdirs();
        final File driverDir = Files.createTempDirectory("gen-counters-driver").toFile();

        final MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(driverDir.getAbsolutePath())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));

        try (Aeron aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
            final List<Case> cases = new ArrayList<>();

            cases.add(new Case(
                "cluster-state",
                AeronCounters.CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
                CLUSTER_ID,
                -1,
                4,
                null,
                null,
                ClusterCounters.allocate(
                    aeron, buffer, "Consensus Module state",
                    AeronCounters.CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, CLUSTER_ID).id()));

            // A second counter of the same type and a different cluster id: the
            // finder must pick by key, not by type alone.
            cases.add(new Case(
                "cluster-state-other-cluster",
                AeronCounters.CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID,
                OTHER_CLUSTER_ID,
                -1,
                4,
                null,
                null,
                ClusterCounters.allocate(
                    aeron, buffer, "Consensus Module state",
                    AeronCounters.CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, OTHER_CLUSTER_ID).id()));

            cases.add(new Case(
                "cluster-errors-versioned",
                AeronCounters.CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID,
                CLUSTER_ID,
                -1,
                4,
                VERSION,
                COMMIT,
                ClusterCounters.allocateVersioned(
                    aeron, buffer, "Cluster Errors",
                    AeronCounters.CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID, CLUSTER_ID, VERSION, COMMIT).id()));

            cases.add(new Case(
                "service-counter",
                AeronCounters.CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID,
                CLUSTER_ID,
                SERVICE_ID,
                8,
                null,
                null,
                ClusterCounters.allocateServiceCounter(
                    aeron, buffer, "Cluster container max cycle time in ns",
                    AeronCounters.CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID, CLUSTER_ID, SERVICE_ID).id()));

            cases.add(new Case(
                "service-error-versioned",
                AeronCounters.CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID,
                CLUSTER_ID,
                SERVICE_ID,
                8,
                ClusteredServiceContainerVersion.VERSION,
                ClusteredServiceContainerVersion.GIT_SHA,
                ClusterCounters.allocateServiceErrorCounter(aeron, buffer, CLUSTER_ID, SERVICE_ID).id()));

            final long[] snapshotRecordingIds = { 11, 12, 13 };
            cases.add(new Case(
                "recovery-state",
                RecoveryState.RECOVERY_STATE_TYPE_ID,
                CLUSTER_ID,
                -1,
                32 + snapshotRecordingIds.length * 8,
                null,
                null,
                RecoveryState.allocate(
                    aeron, 5, 1024, 1_700_000_000_000L, CLUSTER_ID, snapshotRecordingIds).id()));

            final long sessionId = 1_234_567_890_123L;
            final String sessionInfo = "test client";
            cases.add(new Case(
                "session",
                AeronCounters.CLUSTER_SESSION_TYPE_ID,
                CLUSTER_ID,
                -1,
                12,
                null,
                null,
                allocateSessionCounter(aeron, buffer, CLUSTER_ID, sessionId, sessionInfo)));

            final CountersReader counters = aeron.countersReader();
            final File metadataFile = new File(dir, "counters-metadata.bin");
            final StringBuilder tsv = new StringBuilder();
            tsv.append("# Generated by crates/cluster/common/tests/fixtures/regen/GenCounters.java\n");
            tsv.append("# against the reference (Aeron 1.53.2, 664f58e705) on an embedded Java media driver.\n");
            tsv.append("# Columns: case, type_id, cluster_id, service_id, key_length, key_hex, version, commit, label\n");
            tsv.append("# The binary holds one 512-byte metadata record per case, in this order.\n");

            try (OutputStream out = new FileOutputStream(metadataFile))
            {
                final byte[] record = new byte[512];

                for (final Case c : cases)
                {
                    final int id = c.counterId;
                    final int offset = CountersReader.metaDataOffset(id);
                    counters.metaDataBuffer().getBytes(offset, record);
                    out.write(record);

                    final byte[] key = new byte[c.keyLength];
                    counters.metaDataBuffer().getBytes(offset + CountersReader.KEY_OFFSET, key);

                    final byte[] labelBytes = new byte[record.length];
                    final int labelOffset = CountersReader.LABEL_OFFSET + Integer.BYTES;
                    final int labelLength = counters.metaDataBuffer().getInt(
                        offset + CountersReader.LABEL_OFFSET);
                    System.arraycopy(record, labelOffset, labelBytes, 0, labelLength);
                    final String label = new String(labelBytes, 0, labelLength, StandardCharsets.US_ASCII);

                    tsv.append(c.name).append('\t')
                        .append(c.typeId).append('\t')
                        .append(c.clusterId).append('\t')
                        .append(c.serviceId).append('\t')
                        .append(c.keyLength).append('\t')
                        .append(hex(key)).append('\t')
                        .append(c.version == null ? "-" : c.version).append('\t')
                        .append(c.commit == null ? "-" : c.commit).append('\t')
                        .append(label).append('\n');
                }
            }

            try (OutputStream out = new FileOutputStream(new File(dir, "counters.tsv")))
            {
                out.write(tsv.toString().getBytes(StandardCharsets.US_ASCII));
            }

            System.out.println("wrote " + metadataFile + " and counters.tsv, " + cases.size() + " cases");
        }
        finally
        {
            driver.close();
        }
    }

    /**
     * {@code ClusterSession.asyncConnect} is package-private in a different
     * package; the constructor and the method are called reflectively. The
     * instance is real, so the bytes are the reference's.
     */
    private static int allocateSessionCounter(
        final Aeron aeron,
        final UnsafeBuffer buffer,
        final int clusterId,
        final long sessionId,
        final String sessionInfo) throws Exception
    {
        final Class<?> sessionClass = Class.forName("io.aeron.cluster.ClusterSession");
        final Constructor<?> ctor = sessionClass.getDeclaredConstructor(
            int.class, long.class, int.class, String.class, String.class);
        ctor.setAccessible(true);
        final Object session = ctor.newInstance(0, sessionId, 10001, "aeron:udp?endpoint=localhost:0", sessionInfo);

        final Method connect = sessionClass.getDeclaredMethod(
            "asyncConnect", Aeron.class, org.agrona.MutableDirectBuffer.class, int.class);
        connect.setAccessible(true);
        connect.invoke(session, aeron, buffer, clusterId);

        // asyncConnect only registers the counter; the driver allocates it when
        // its conductor processes the command. There is no other session
        // counter on this driver, so the first one of type 241 is ours.
        final CountersReader counters = aeron.countersReader();
        final long deadlineMs = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadlineMs)
        {
            for (int i = 0, size = counters.maxCounterId(); i < size; i++)
            {
                if (counters.getCounterState(i) == CountersReader.RECORD_ALLOCATED &&
                    counters.getCounterTypeId(i) == AeronCounters.CLUSTER_SESSION_TYPE_ID)
                {
                    return i;
                }
            }
            Thread.sleep(10);
        }

        throw new IllegalStateException("session counter not allocated within 5 s");
    }

    private static String hex(final byte[] bytes)
    {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (final byte b : bytes)
        {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
