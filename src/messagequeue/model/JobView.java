package messagequeue.model;

public record JobView(String id, String type, int priority, Status status, int retriesUsed,
                      int leaseExpirations, String ownerWorkerId, String lastError) {
}
