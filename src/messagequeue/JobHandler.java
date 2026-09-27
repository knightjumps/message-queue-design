package messagequeue;

import messagequeue.model.Result;

import java.util.Map;

@FunctionalInterface
public interface JobHandler {
    Result run(Map<String, Object> payload) throws Exception;
}
