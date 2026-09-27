import messagequeue.JobService;
import messagequeue.JobWorker;
import messagequeue.model.JobSpec;
import messagequeue.model.Result;
import messagequeue.model.Status;

import java.util.Map;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        JobService service = new JobService();
        try (JobWorker worker = new JobWorker(service, "email-worker", Map.of(
                "send_email", payload -> {
                    System.out.println("Sending email to " + payload.get("to"));
                    return Result.success();
                }))) {
            worker.start();
            String id = service.submit(new JobSpec("send_email", Map.of("to", "user@example.com")));
            for (int i = 0; i < 100 && service.getJob(id).status() != Status.SUCCEEDED; i++)
                Thread.sleep(10);
            System.out.println("Job " + id + ": " + service.getJob(id).status());
            System.out.println(service.workerStats("email-worker"));
        }
    }
}
