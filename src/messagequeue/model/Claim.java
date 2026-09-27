package messagequeue.model;

import java.time.Duration;
import java.util.Map;

public record Claim(String jobId, String type, Map<String, Object> payload, String token, Duration leaseDuration) {
}
