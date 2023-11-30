/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import com.amazonaws.services.lambda.runtime.events.S3BatchEvent;
import com.amazonaws.services.lambda.runtime.events.S3BatchEvent.Job;
import com.amazonaws.services.lambda.runtime.events.S3BatchEvent.Task;
import com.amazonaws.services.lambda.runtime.events.S3BatchResponse;
import com.amazonaws.services.lambda.runtime.events.S3BatchResponse.Result;
import com.amazonaws.services.lambda.runtime.events.S3BatchResponse.ResultCode;
import com.google.gson.Gson;

/**
 * Helper methods for testing S3BatchOperations.
 */
public class S3BatchOperationsTestUtils {
    /**
     * Create a S3BatchEvent from a list of tasks.
     * 
     * <p>
     * Invocation id and job id are set using hard-coded constants.
     * </p>
     * 
     * @param tasks tasks to add to the event
     * @return batch event
     */
    public static S3BatchEvent batchEvent(Task... tasks) {
        String invocationId = "YXNkbGZqYWRmaiBhc2RmdW9hZHNmZGpmaGFzbGtkaGZza2RmaAo";
        String jobId = "f3cc4f60-61f6-4a2b-8a21-d07600c373ce";
        return batchEvent(invocationId, jobId, tasks);
    }

    /**
     * Create a S3BatchEvent from a list of tasks.
     * 
     * @param invocationId invocation id
     * @param jobId        job id
     * @param tasks        tasks to add to the event
     * @return batch event
     */
    public static S3BatchEvent batchEvent(String invocationId, String jobId, Task... tasks) {
        S3BatchEvent event = S3BatchEvent.builder()
                .withInvocationId(invocationId)
                .withInvocationSchemaVersion("1")
                .withJob(Job.builder()
                        .withId(jobId)
                            .build())
                .withTasks(Arrays.asList(tasks))
                .build();
        return event;
    }
    /**
     * Create a Task object to be used to create a S3BatchEvent
     * 
     * @param bucketArn ARN of S3 bucket
     * @param key       Key of file within the bucket
     * @return S3BatchOperations task
     */
    public static Task task(String bucketArn, String key) {
        return Task.builder().withS3BucketArn(bucketArn).withS3Key(key).withS3VersionId("1").build();
    }

    /**
     * Create a {@link Matcher} which verifies that all tasks in a S3BatchReponse
     * have been executed successfully.
     * 
     * @param event S3BatchEvent to compare to
     * 
     * @return {@link Matcher}
     */
    public static org.hamcrest.Matcher<java.lang.String> successfulS3BatchEvent(S3BatchEvent event) {
        return new S3BatchResponseMatcher(event) {

            @Override
            protected boolean matches(S3BatchEvent event, S3BatchResponse response, Task task, Result result,
                    int index) {

                if (ResultCode.Succeeded.equals(result.getResultCode())) {
                    return true;
                }
                reportIssue(event, response, task, result, "task failed");
                return false;
            }

        };
    }

    /**
     * Matcher for S3BatchResponses.
     * 
     * <p>
     * The response may either be passed directly as instance of
     * {@link S3BatchResponse} or as string (body of a HTTP response) in which case
     * it will be parsed to a response object
     * </p>
     * 
     * <p>
     * To implement actual matching behavior, overwrite one of the
     * <code>match</code> variants. The default implementation calls
     * {@link #matches(S3BatchEvent, S3BatchResponse, Task, Result, int)} for each
     * task and returns the aggregated result.
     * </p>
     */
    public static abstract class S3BatchResponseMatcher extends BaseMatcher<String> {

        private S3BatchEvent event;
        private List<String> issues = new ArrayList<>();

        public S3BatchResponseMatcher(S3BatchEvent event) {
            this.event = event;
        }

        public S3BatchEvent getBatchEvent() {
            return event;
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            description.appendText("S3BatchResponse");
            if (item != null) {
                description.appendText("was ").appendValue(item);
            }
            description.appendText("Task results:");
            for (String issue : issues) {
                description.appendText("\n").appendText(issue);
            }
        }

        @Override
        public final void describeTo(Description description) {
            describeMismatch(null, description);
        }

        protected void reportIssue(S3BatchEvent event, S3BatchResponse response, String issue) {
            issues.add(issue);
        }

        protected void reportIssue(S3BatchEvent event, S3BatchResponse response, Task task, Result result,
                String issue) {
            StringBuilder b = new StringBuilder();
            b.append("task ").append(task.getTaskId());
            b.append(" for file ").append(task.getS3Key()).append(": ");
            b.append(issue);
            issues.add(b.toString());
        }

        @Override
        public boolean matches(Object actual) {
            if (actual instanceof CharSequence) {
                try (StringReader reader = new StringReader(actual.toString())) {
                    Gson gson = new Gson();
                    S3BatchResponse response = gson.fromJson(reader, S3BatchResponse.class);
                    return matches(event, response);
                }
            }
            else if (actual instanceof S3BatchResponse response) {
                return matches(event, response);
            }
            return false;
        }

        protected boolean matches(S3BatchEvent event, S3BatchResponse response) {
            List<Task> tasks = event.getTasks();
            List<Result> results = response.getResults();

            if (tasks.size() != results.size()) {
                reportIssue(event, response, "result count does not match task count");
                return false;
            }

            boolean matches = true;
            for (int t = 0; t < tasks.size(); t++) {
                Task task = tasks.get(t);
                Result result = results.get(t);
                if (!matches(event, response, task, result, t)) {
                    matches = false;
                }
            }
            // aggregated result of all tasks
            return matches;
        }

        protected boolean matches(S3BatchEvent event, S3BatchResponse response, Task task, Result result, int index) {
            // override to customize behavior
            return false;
        }
    }
}
