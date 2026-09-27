package messagequeue.model;

import java.util.Map;
import java.util.Objects;

public record JobSpec(String type, Map<String, Object> payload, int priority, int maxRetries) {
    public JobSpec {
        if (type == null || type.isBlank()) throw new IllegalArgumentException("type required");
        payload = Map.copyOf(Objects.requireNonNull(payload));
        if (priority < 0 || priority > 9 || maxRetries < 0)
            throw new IllegalArgumentException("invalid priority/retries");
    }

    public JobSpec(String type, Map<String, Object> payload) {
        this(type, payload, 5, 3);
    }
}
