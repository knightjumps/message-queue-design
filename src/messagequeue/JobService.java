package messagequeue;

import messagequeue.model.Claim;
import messagequeue.model.JobSpec;
import messagequeue.model.JobView;
import messagequeue.model.QueueStats;
import messagequeue.model.Result;
import messagequeue.model.Status;
import messagequeue.model.WorkerStats;

import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;

/**
 * In-memory, thread-safe pull queue. Every state transition holds this monitor.
 */
public final class JobService {
    private final Map<String, Job> jobs = new LinkedHashMap<>();
    private final Map<String, WorkerState> workers = new HashMap<>();
    private final Map<String, Duration> leaseByType = new HashMap<>();
    private final LongSupplier clock;
    private final Duration lease, baseBackoff, maxBackoff, aging;
    private long nextSequence;

    public JobService() {
        this(System::nanoTime, Duration.ofSeconds(30), Duration.ofSeconds(1),
                Duration.ofMinutes(1), Duration.ofSeconds(30));
    }

    public JobService(LongSupplier clock, Duration lease, Duration baseBackoff, Duration maxBackoff, Duration aging) {
        this.clock = Objects.requireNonNull(clock);
        this.lease = positive(lease);
        this.baseBackoff = positive(baseBackoff);
        this.maxBackoff = positive(maxBackoff);
        this.aging = positive(aging);
        if (maxBackoff.compareTo(baseBackoff) < 0) throw new IllegalArgumentException("backoff cap below base");
    }

    private static Duration positive(Duration d) {
        if (d == null || d.isZero() || d.isNegative()) throw new IllegalArgumentException("positive duration required");
        d.toNanos();
        return d;
    }

    public synchronized void setLeaseDuration(String type, Duration duration) {
        if (type == null || type.isBlank()) throw new IllegalArgumentException("type required");
        leaseByType.put(type, positive(duration));
    }

    public synchronized String submit(JobSpec spec) {
        Objects.requireNonNull(spec);
        String id = UUID.randomUUID().toString();
        jobs.put(id, new Job(id, spec, clock.getAsLong(), nextSequence++));
        return id;
    }

    public synchronized void registerWorker(String id, Set<String> types) {
        if (id == null || id.isBlank() || types == null || types.isEmpty() || types.stream().anyMatch(t -> t == null || t.isBlank()))
            throw new IllegalArgumentException("worker id and types required");
        if (workers.putIfAbsent(id, new WorkerState(Set.copyOf(types))) != null)
            throw new IllegalArgumentException("duplicate worker id");
    }

    private WorkerState worker(String id) {
        WorkerState w = workers.get(id);
        if (w == null) throw new IllegalArgumentException("unknown worker: " + id);
        return w;
    }

    public synchronized Optional<Claim> claim(String workerId) {
        long now = clock.getAsLong();
        advance(now);
        WorkerState w = worker(workerId);
        if (w.currentJobId != null) return Optional.empty();
        Job best = null;
        int bestPriority = -1;
        for (Job j : jobs.values()) {
            if (j.status != Status.PENDING || !w.types.contains(j.spec.type())) continue;
            int effective = (int) Math.min(9, j.spec.priority() + Math.max(0, now - j.readyAt) / aging.toNanos());
            if (best == null || effective > bestPriority || (effective == bestPriority && j.sequence < best.sequence)) {
                best = j;
                bestPriority = effective;
            }
        }
        if (best == null) return Optional.empty();
        Duration d = leaseByType.getOrDefault(best.spec.type(), lease);
        best.status = Status.CLAIMED;
        best.owner = workerId;
        best.token = UUID.randomUUID().toString();
        best.claimedAt = now;
        best.expiresAt = now + d.toNanos();
        w.currentJobId = best.id;
        return Optional.of(new Claim(best.id, best.spec.type(), best.spec.payload(), best.token, d));
    }

    public synchronized boolean renew(String jobId, String token) {
        long now = clock.getAsLong();
        advance(now);
        Job j = jobs.get(jobId);
        if (!owns(j, token)) return false;
        j.expiresAt = now + leaseByType.getOrDefault(j.spec.type(), lease).toNanos();
        return true;
    }

    public synchronized boolean complete(String jobId, String token, Result result) {
        Objects.requireNonNull(result);
        long now = clock.getAsLong();
        advance(now);
        Job j = jobs.get(jobId);
        if (!owns(j, token)) return false;
        WorkerState w = workers.get(j.owner);
        w.currentJobId = null;
        j.owner = null;
        j.token = null;
        j.expiresAt = 0;
        switch (result.kind()) {
            case SUCCESS -> {
                j.status = Status.SUCCEEDED;
                w.successes++;
            }
            case PERMANENT_FAILURE -> {
                j.status = Status.FAILED;
                j.lastError = result.error();
                w.failures++;
            }
            case TRANSIENT_FAILURE -> {
                j.lastError = result.error();
                w.failures++;
                if (j.retriesUsed < j.spec.maxRetries()) {
                    j.retriesUsed++;
                    j.status = Status.RETRY_SCHEDULED;
                    j.readyAt = now + backoff(j.retriesUsed);
                } else j.status = Status.FAILED;
            }
        }
        return true;
    }

    private long backoff(int retry) {
        long value = baseBackoff.toNanos(), cap = maxBackoff.toNanos();
        for (int i = 1; i < retry && value < cap; i++) value = value > cap / 2 ? cap : value * 2;
        return Math.min(value, cap);
    }

    private boolean owns(Job j, String token) {
        return j != null && j.status == Status.CLAIMED && token != null && token.equals(j.token);
    }

    /**
     * Called by polling claims and reads; no separate scheduler assigns work.
     */
    private void advance(long now) {
        for (Job j : jobs.values()) {
            if (j.status == Status.RETRY_SCHEDULED && now >= j.readyAt) {
                j.status = Status.PENDING;
                j.sequence = nextSequence++;
            } else if (j.status == Status.CLAIMED && now >= j.expiresAt) {
                WorkerState w = workers.get(j.owner);
                if (w != null && j.id.equals(w.currentJobId)) w.currentJobId = null;
                j.status = Status.PENDING;
                j.leaseExpirations++;
                j.owner = null;
                j.token = null;
                j.readyAt = j.expiresAt;
                j.expiresAt = 0;
                j.sequence = nextSequence++;
            }
        }
    }

    private static JobView view(Job j) {
        return new JobView(j.id, j.spec.type(), j.spec.priority(), j.status, j.retriesUsed,
                j.leaseExpirations, j.owner, j.lastError);
    }

    public synchronized JobView getJob(String id) {
        advance(clock.getAsLong());
        Job j = jobs.get(id);
        if (j == null) throw new IllegalArgumentException("unknown job");
        return view(j);
    }

    public synchronized List<JobView> listDlq() {
        advance(clock.getAsLong());
        return jobs.values().stream().filter(j -> j.status == Status.FAILED).map(JobService::view).toList();
    }

    /**
     * Claims older than a threshold, including claims still being renewed by a hung handler.
     */
    public synchronized List<JobView> stuckJobs(Duration threshold) {
        positive(threshold);
        long now = clock.getAsLong();
        advance(now);
        return jobs.values().stream()
                .filter(j -> j.status == Status.CLAIMED && now - j.claimedAt >= threshold.toNanos())
                .map(JobService::view).toList();
    }

    public synchronized WorkerStats workerStats(String id) {
        advance(clock.getAsLong());
        WorkerState w = worker(id);
        return new WorkerStats(id, w.currentJobId, w.successes, w.failures);
    }

    public synchronized QueueStats stats() {
        advance(clock.getAsLong());
        Map<String, Long> types = new HashMap<>();
        Map<Integer, Long> priorities = new HashMap<>();
        long inflight = 0, retrying = 0, failed = 0;
        for (Job j : jobs.values())
            switch (j.status) {
                case PENDING -> {
                    types.merge(j.spec.type(), 1L, Long::sum);
                    priorities.merge(j.spec.priority(), 1L, Long::sum);
                }
                case CLAIMED -> inflight++;
                case RETRY_SCHEDULED -> retrying++;
                case FAILED -> failed++;
                case SUCCEEDED -> {
                }
            }
        return new QueueStats(Map.copyOf(types), Map.copyOf(priorities), inflight, retrying, failed);
    }
}
