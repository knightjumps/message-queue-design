package messagequeue;

import messagequeue.model.Claim;
import messagequeue.model.Result;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * A worker pulls jobs and renews leases while its handler runs.
 */
public final class JobWorker implements AutoCloseable {
    private final JobService service;
    private final String id;
    private final Map<String, JobHandler> handlers;
    private final ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "job-heartbeat");
        t.setDaemon(true);
        return t;
    });
    private Thread thread;
    private volatile boolean running;

    public JobWorker(JobService service, String id, Map<String, JobHandler> handlers) {
        this.service = Objects.requireNonNull(service);
        this.id = id;
        this.handlers = Map.copyOf(handlers);
        service.registerWorker(id, this.handlers.keySet());
    }

    public synchronized void start() {
        if (thread != null) throw new IllegalStateException("already started");
        running = true;
        thread = new Thread(this::loop, "job-worker-" + id);
        thread.start();
    }

    private void loop() {
        while (running && !Thread.currentThread().isInterrupted()) {
            var next = service.claim(id);
            if (next.isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            Claim job = next.get();
            long interval = Math.max(1, job.leaseDuration().toMillis() / 3);
            ScheduledFuture<?> heartbeat = heartbeats.scheduleAtFixedRate(
                    () -> service.renew(job.jobId(), job.token()), interval, interval, TimeUnit.MILLISECONDS);
            try {
                Result result;
                try {
                    result = handlers.get(job.type()).run(job.payload());
                    if (result == null) result = Result.transientFailure("handler returned null");
                } catch (Exception e) {
                    result = Result.transientFailure(e.toString());
                }
                service.complete(job.jobId(), job.token(), result);
            } finally {
                heartbeat.cancel(false);
            }
        }
    }

    @Override
    public synchronized void close() {
        running = false;
        if (thread != null) thread.interrupt();
        heartbeats.shutdownNow();
        if (thread != null && thread != Thread.currentThread()) {
            try {
                thread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
