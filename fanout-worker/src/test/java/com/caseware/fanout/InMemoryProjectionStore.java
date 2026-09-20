package com.caseware.fanout;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.caseware.fanout.ProjectionRow.Verification;

/**
 * The projection as a map, with the conditional writes and the key-ordered paging the real store promises. Rows
 * are scanned in insertion order, and a cursor names a position in that order rather than a row, so a page is
 * still correct when the rows before it have been verified away.
 */
final class InMemoryProjectionStore implements ProjectionStore {

    private record Row(long order, String templateId, FirmId firmId, long seq, Verification verification, String baseVersion) {}

    private final Map<EngagementId, Row> rows = new LinkedHashMap<>();
    private long nextOrder;
    private int scansThatThrow;
    private int readsThatThrow;

    synchronized List<EngagementId> seed(String templateId, FirmId firmId, int count) {
        List<EngagementId> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            EngagementId id = new EngagementId(firmId + "-" + i);
            rows.put(id, new Row(nextOrder++, templateId, firmId, 1, Verification.UNVERIFIED, null));
            ids.add(id);
        }
        return ids;
    }

    /** What an event from the engagement system does to a row: the sequence moves on. */
    synchronized void bumpSeq(EngagementId id) {
        Row row = rows.get(id);
        rows.put(id, new Row(row.order(), row.templateId(), row.firmId(), row.seq() + 1, row.verification(), row.baseVersion()));
    }

    synchronized void archive(EngagementId id) {
        rows.remove(id);
    }

    /** The next {@code n} row reads throw, the way a throttled DynamoDB call does. */
    synchronized void failReads(int n) {
        readsThatThrow = n;
    }

    /** The next {@code n} page scans throw. */
    synchronized void failScans(int n) {
        scansThatThrow = n;
    }

    synchronized long count(Verification verification) {
        return rows.values().stream().filter(row -> row.verification() == verification).count();
    }

    /** The answer this worker exists to produce, per engagement, for every row that has one. */
    synchronized Map<EngagementId, String> baseVersions() {
        Map<EngagementId, String> versions = new LinkedHashMap<>();
        rows.forEach((id, row) -> {
            if (row.baseVersion() != null) versions.put(id, row.baseVersion());
        });
        return versions;
    }

    @Override
    public synchronized Page unverifiedRows(String templateId, Optional<String> after, int limit) {
        if (scansThatThrow > 0) {
            scansThatThrow--;
            throw new IllegalStateException("projection scan throttled");
        }
        long from = after.map(Long::parseLong).orElse(-1L);
        List<ProjectionRow> all = rows.entrySet().stream()
                .filter(e -> e.getValue().templateId().equals(templateId))
                .filter(e -> e.getValue().verification() == Verification.UNVERIFIED)
                .filter(e -> e.getValue().order() > from)
                .sorted((a, b) -> Long.compare(a.getValue().order(), b.getValue().order()))
                .map(e -> toRow(e.getKey(), e.getValue()))
                .toList();
        if (all.isEmpty()) return Page.last(List.of());
        List<ProjectionRow> page = all.subList(0, Math.min(limit, all.size()));
        if (page.size() == all.size()) return Page.last(page);
        long last = rows.get(page.get(page.size() - 1).engagementId()).order();
        return new Page(page, Optional.of(Long.toString(last)));
    }

    @Override
    public synchronized Optional<ProjectionRow> read(EngagementId engagementId) {
        if (readsThatThrow > 0) {
            readsThatThrow--;
            throw new IllegalStateException("projection read throttled");
        }
        return Optional.ofNullable(rows.get(engagementId)).map(row -> toRow(engagementId, row));
    }

    @Override
    public synchronized boolean recordVerified(EngagementId engagementId, String baseVersion, long expectedSeq) {
        return update(engagementId, expectedSeq, Verification.VERIFIED, baseVersion);
    }

    @Override
    public synchronized boolean recordDeadLettered(EngagementId engagementId, long expectedSeq) {
        return update(engagementId, expectedSeq, Verification.DEAD_LETTERED, null);
    }

    private boolean update(EngagementId id, long expectedSeq, Verification to, String baseVersion) {
        Row row = rows.get(id);
        if (row == null || row.verification() != Verification.UNVERIFIED || row.seq() != expectedSeq) return false;
        rows.put(id, new Row(row.order(), row.templateId(), row.firmId(), row.seq(), to, baseVersion));
        return true;
    }

    private static ProjectionRow toRow(EngagementId id, Row row) {
        return new ProjectionRow(id, row.firmId(), row.seq(), row.verification());
    }
}
