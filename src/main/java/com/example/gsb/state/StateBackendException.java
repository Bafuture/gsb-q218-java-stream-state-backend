package com.example.gsb.state;

/** Unchecked wrapper for snapshot IO failures. */
public class StateBackendException extends RuntimeException {

    public StateBackendException(String message, Throwable cause) {
        super(message, cause);
    }
}
