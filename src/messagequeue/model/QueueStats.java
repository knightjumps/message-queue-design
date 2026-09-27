package messagequeue.model;

import java.util.Map;

public record QueueStats(Map<String, Long> pendingByType, Map<Integer, Long> pendingByPriority,
                         long inFlight, long retryScheduled, long dlqSize) {
}
