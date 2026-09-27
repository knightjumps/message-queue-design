import messagequeue.JobService;
import messagequeue.JobWorker;
import messagequeue.model.JobSpec;
import messagequeue.model.Result;
import messagequeue.model.Status;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** See README.md for the compile and test commands. */
public class JobServiceTest {
    private static final class Fixture {
        final AtomicLong time = new AtomicLong();
        final JobService service = new JobService(time::get, Duration.ofSeconds(5),
                Duration.ofSeconds(1), Duration.ofSeconds(8), Duration.ofSeconds(1));
        void advance(Duration d) { time.addAndGet(d.toNanos()); }
        String add(String type, int priority, int retries) {
            return service.submit(new JobSpec(type, Map.of(), priority, retries));
        }
    }
    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    private static void state(Fixture f, String id, Status expected) {
        check(f.service.getJob(id).status() == expected, id + " expected " + expected);
    }
    private static void testConcurrentClaim() throws Exception {
        Fixture f = new Fixture();
        f.service.registerWorker("a", Set.of("email"));
        f.service.registerWorker("b", Set.of("email"));
        f.add("email", 5, 3);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> a = pool.submit(() -> { start.await(); return f.service.claim("a").isPresent(); });
            Future<Boolean> b = pool.submit(() -> { start.await(); return f.service.claim("b").isPresent(); });
            start.countDown();
            check(a.get(2, TimeUnit.SECONDS) != b.get(2, TimeUnit.SECONDS), "exactly one claim");
            check(f.service.stats().inFlight() == 1, "one in flight");
        } finally { pool.shutdownNow(); }
    }
    private static void testExpiryAndFencing() {
        Fixture f = new Fixture();
        f.service.registerWorker("a", Set.of("email"));
        f.service.registerWorker("b", Set.of("email"));
        String id = f.add("email", 5, 0);
        var first = f.service.claim("a").orElseThrow();
        f.advance(Duration.ofSeconds(5));
        var second = f.service.claim("b").orElseThrow();
        check(second.jobId().equals(id), "reclaimed after expiry");
        check(!f.service.complete(id, first.token(), Result.success()), "stale completion rejected");
        check(f.service.complete(id, second.token(), Result.success()), "new owner completes");
        state(f, id, Status.SUCCEEDED);
        check(f.service.getJob(id).leaseExpirations() == 1, "expiry counted");
    }
    private static void testRenewal() {
        Fixture f = new Fixture();
        f.service.registerWorker("a", Set.of("email"));
        f.service.registerWorker("b", Set.of("email"));
        String id = f.add("email", 5, 1);
        var claim = f.service.claim("a").orElseThrow();
        f.advance(Duration.ofSeconds(4));
        check(f.service.renew(id, claim.token()), "renew accepted");
        f.advance(Duration.ofSeconds(4));
        check(f.service.claim("b").isEmpty(), "renewed lease still exclusive");
        check(f.service.stuckJobs(Duration.ofSeconds(7)).size() == 1, "old renewed claim is surfaced");
        check(f.service.complete(id, claim.token(), Result.success()), "owner completes");
    }
    private static void testRetryAndDlq() {
        Fixture f = new Fixture();
        f.service.registerWorker("a", Set.of("email"));
        String id = f.add("email", 5, 2);
        var first = f.service.claim("a").orElseThrow();
        check(f.service.complete(id, first.token(), Result.transientFailure("timeout")), "first failure");
        state(f, id, Status.RETRY_SCHEDULED);
        check(f.service.claim("a").isEmpty(), "not due immediately");
        f.advance(Duration.ofMillis(999));
        check(f.service.claim("a").isEmpty(), "not due early");
        f.advance(Duration.ofMillis(1));
        var second = f.service.claim("a").orElseThrow();
        f.service.complete(id, second.token(), Result.transientFailure("timeout"));
        f.advance(Duration.ofSeconds(1));
        check(f.service.claim("a").isEmpty(), "second backoff is two seconds");
        f.advance(Duration.ofSeconds(1));
        var third = f.service.claim("a").orElseThrow();
        f.service.complete(id, third.token(), Result.transientFailure("timeout"));
        state(f, id, Status.FAILED);
        check(f.service.getJob(id).retriesUsed() == 2, "two retries after initial attempt");
        check(f.service.listDlq().size() == 1 && f.service.stats().dlqSize() == 1, "DLQ entry");
        check(f.service.workerStats("a").handlerFailures() == 3, "failure attempts counted");
    }
    private static void testPermanentFailure() {
        Fixture f = new Fixture();
        f.service.registerWorker("a", Set.of("email"));
        String id = f.add("email", 5, 3);
        var claim = f.service.claim("a").orElseThrow();
        f.service.complete(id, claim.token(), Result.permanentFailure("invalid address"));
        state(f, id, Status.FAILED);
        check(f.service.getJob(id).retriesUsed() == 0, "no retries after permanent failure");
    }
    private static void testPriorityAgingAndTypes() {
        Fixture f = new Fixture();
        f.service.registerWorker("a", Set.of("email"));
        String low = f.add("email", 0, 0);
        String wrongType = f.add("image", 9, 0);
        String high = f.add("email", 9, 0);
        var first = f.service.claim("a").orElseThrow();
        check(first.jobId().equals(high), "high priority first");
        f.service.complete(high, first.token(), Result.success());
        f.advance(Duration.ofSeconds(9));
        String newerHigh = f.add("email", 9, 0);
        var second = f.service.claim("a").orElseThrow();
        check(second.jobId().equals(low), "old low priority aged and wins FIFO tie");
        f.service.complete(low, second.token(), Result.success());
        check(f.service.claim("a").orElseThrow().jobId().equals(newerHigh), "supported type only");
        check(f.service.stats().pendingByType().get("image") == 1, "unsupported job still pending");
        check(f.service.getJob(wrongType).status() == Status.PENDING, "image untouched");
    }
    private static void testWorkerHeartbeat() throws Exception {
        JobService service = new JobService(System::nanoTime, Duration.ofMillis(60),
                Duration.ofMillis(10), Duration.ofSeconds(1), Duration.ofSeconds(1));
        try (JobWorker worker = new JobWorker(service, "w", Map.of("slow", payload -> {
            Thread.sleep(180); return Result.success();
        }))) {
            worker.start();
            String id = service.submit(new JobSpec("slow", Map.of()));
            long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            while (service.getJob(id).status() != Status.SUCCEEDED && System.nanoTime() < deadline)
                Thread.sleep(5);
            check(service.getJob(id).status() == Status.SUCCEEDED, "slow handler completed");
            check(service.getJob(id).leaseExpirations() == 0, "heartbeat preserved lease");
        }
    }
    public static void main(String[] args) throws Exception {
        testConcurrentClaim(); testExpiryAndFencing(); testRenewal();
        testRetryAndDlq(); testPermanentFailure(); testPriorityAgingAndTypes(); testWorkerHeartbeat();
        System.out.println("All JobService tests passed");
    }
}
