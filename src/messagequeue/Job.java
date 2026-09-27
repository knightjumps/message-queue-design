package messagequeue;

import messagequeue.model.JobSpec;
import messagequeue.model.Status;

final class Job {
    final String id;
    final JobSpec spec;
    Status status = Status.PENDING;
    int retriesUsed, leaseExpirations;
    String lastError, owner, token;
    long readyAt, sequence, claimedAt, expiresAt;

    Job(String id, JobSpec spec, long now, long sequence) {
        this.id = id;
        this.spec = spec;
        this.readyAt = now;
        this.sequence = sequence;
    }
}
