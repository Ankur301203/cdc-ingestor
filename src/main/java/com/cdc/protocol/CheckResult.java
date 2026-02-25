package com.cdc.protocol;

public record CheckResult(
    boolean success,
    String message
) {
    public static CheckResult ok(String message) {
        return new CheckResult(true, message);
    }

    public static CheckResult error(String message) {
        return new CheckResult(false, message);
    }
}
