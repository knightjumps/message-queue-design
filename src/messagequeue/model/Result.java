package messagequeue.model;

import java.util.Objects;

public record Result(ResultKind kind, String error) {
    public Result {
        Objects.requireNonNull(kind);
    }

    public static Result success() {
        return new Result(ResultKind.SUCCESS, null);
    }

    public static Result transientFailure(String error) {
        return new Result(ResultKind.TRANSIENT_FAILURE, error);
    }

    public static Result permanentFailure(String error) {
        return new Result(ResultKind.PERMANENT_FAILURE, error);
    }
}
