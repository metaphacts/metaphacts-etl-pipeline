/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3BatchEvent.Task;

public class TaskContext {
    protected final Context context;
    protected final Task task;

    public TaskContext(Context context, Task task) {
        this.context = context;
        this.task = task;
    }

    public Context getContext() {
        return context;
    }

    public LambdaLogger getLogger() {
        return context.getLogger();
    }

    public Task getTask() {
        return task;
    }
}
