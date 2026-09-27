# In-process background job service

Requires Java 17 or newer. From this directory:

```sh
javac -d out src/messagequeue/model/*.java src/messagequeue/*.java src/Main.java test/*.java
java -cp out JobServiceTest
java -cp out Main
```

`messagequeue.model` contains the public job records and enums. `messagequeue.JobService` owns the in-memory queue and synchronizes every state transition. Package-private `Job` and `WorkerState` hold mutable internal state. `JobWorker` registers its supported types and pulls one eligible job at a time using `JobHandler`. It invokes handlers outside the service lock and renews the lease from a separate heartbeat thread. `Main` is a small usage example.

## Decisions

- A 30-second lease is the default; `setLeaseDuration(type, duration)` overrides it by type. Workers heartbeat every third of a lease. Each claim receives a new token. Renewals and completions with an expired or stale token are rejected.
- Higher base priority (0–9) normally wins. Waiting pending jobs gain one effective priority point every 30 seconds, capped at 9. Equal effective priorities use FIFO order by when the job became ready. This avoids indefinite starvation under continuing capacity and a finite backlog ahead of a job.
- `maxRetries` counts retries after the first handler invocation. Transient failures retry after 1, 2, 4, ... seconds, capped at 60 seconds. Permanent failures and exhausted retries enter the DLQ. Lease expiry returns directly to pending and does not consume a retry.
- `FAILED` is the terminal state represented in `listDlq()`; there is no duplicate DLQ storage. Pending counts exclude scheduled retries. `stuckJobs(threshold)` reports old claims even if they are still being renewed.
- Workers poll every 10 milliseconds. Each claim or read advances due retries and expired leases, so there is no dispatcher that pushes work to workers. An injected monotonic clock makes time-based behavior testable.

## Limits

State is in memory. A full process crash loses queued jobs, so recovery across restarts requires persistent storage. Lease fencing protects queue state but cannot undo external effects from a worker that finishes late. Handlers should receive a domain idempotency key in their payload for those effects; alternatively, extend the handler context to expose the job ID. Repeated lease expiry can redeliver indefinitely; `leaseExpirations` exposes this for monitoring. The implementation scans jobs during claim and maintenance, which is simple for an interview-sized in-process queue but would need indexing for large backlogs.
