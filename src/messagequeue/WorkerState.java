package messagequeue;

import java.util.Set;

final class WorkerState {
    final Set<String> types;
    String currentJobId;
    long successes, failures;

    WorkerState(Set<String> types) {
        this.types = types;
    }
}
