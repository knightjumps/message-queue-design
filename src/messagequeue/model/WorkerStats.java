package messagequeue.model;

public record WorkerStats(String workerId, String currentJobId, long successes, long handlerFailures) {
}
