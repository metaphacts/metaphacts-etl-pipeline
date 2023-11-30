/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.util.Optional;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Helper class to manage the {@link LambdaLogger} using a thread-local variable
 * and make it accessable where required.
 */
@ApplicationScoped
public class LambdaLoggerManager {
    private final InheritableThreadLocal<LambdaLogger> lambdaLoggerTL = new InheritableThreadLocal<>();

    public LambdaLoggerManager() {
    }

    /**
     * Set the logger for the current thread.
     * 
     * @param logger lambda logger to use
     */
    public void set(LambdaLogger logger) {
        lambdaLoggerTL.set(logger);
    }

    /**
     * Unset (remove) the logger for the current thread.
     */
    public void remove() {
        lambdaLoggerTL.remove();
    }

    /**
     * Get the logger for the current thread.
     * 
     * @return lambda logger for the current thread or <code>empty</code> when not
     *         available
     */
    public Optional<LambdaLogger> get() {
        return Optional.ofNullable(lambdaLoggerTL.get());
    }
}
