package com.caseware.fanout;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.caseware.fanout.ProjectionRow.Verification;

/** The projection as a map, with the conditional writes the real store promises. Rows keep insertion order. */
final class InMemoryProjectionStore implements ProjectionStore {

    private record Row(String templateId, FirmId firmId, long seq, Verification verification, String baseVersion) {}

    private final Map<EngagementId, Row> rows = new LinkedHashMap<>();

    synchronized List<EngagementId> seed(String templateId, FirmId firmId, int count) {
        List<EngagementId> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            EngagementId id = new EngagementId(firmId + "-" + i);
            rows.put(id, new Row(templateId, firmId, 1, Verification.UNVERIFIED, null));
            ids.add(id);
        }
        return ids;
    }

    /** What an event from the engagement system does to a row: the sequence moves on. */
    synchronized void bumpSeq(EngagementId id) {
        Row row = rows.get(id);
        rows.put(id, new Row(row.templateId(), row.firmId(), row.seq() + 1, row.verification(), row.baseVersion()));
    }

    synchronized void archive(EngagementId id) {
        rows.remove(id);
    }

    synchronized long count(Verification verification) {
        return rows.values().stream().filter(row -> row.verification() == verification).count();
    }

    synchronized Optional<String> baseVersionOf(EngagementId id) {
        return Optional.ofNullable(rows.get(id)).map(Row::baseVersion);
    }

    @Override
    public synchronized List<ProjectionRow> unverifiedRows(String templateId) {
        return rows.entrySet().stream()
                .filter(e -> e.getValue().templateId().equals(templateId))
                .filter(e -> e.getValue().verification() == Verification.UNVERIFIED)
                .map(e -> toRow(e.getKey(), e.getValue()))
                .toList();
    }

    @Override
    public synchronized Optional<ProjectionRow> read(EngagementId engagementId) {
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
        rows.put(id, new Row(row.templateId(), row.firmId(), row.seq(), to, baseVersion));
        return true;
    }

    private static ProjectionRow toRow(EngagementId id, Row row) {
        return new ProjectionRow(id, row.firmId(), row.seq(), row.verification());
    }
}
