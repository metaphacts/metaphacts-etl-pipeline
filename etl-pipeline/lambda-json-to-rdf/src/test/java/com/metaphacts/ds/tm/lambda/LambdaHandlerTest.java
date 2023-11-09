package com.metaphacts.ds.tm.lambda;

import static io.restassured.RestAssured.given;

import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.lambda.runtime.events.S3BatchEvent;
import com.amazonaws.services.lambda.runtime.events.S3BatchEvent.Job;
import com.amazonaws.services.lambda.runtime.events.S3BatchEvent.Task;

import io.quarkus.test.junit.QuarkusTest;

// TODO try to make this work within the builder container
@Disabled("Disabled for now as it requires Docker for the test environment which is not available within the Docker container used to build and run this test")
//@QuarkusTest
public class LambdaHandlerTest {

    @Test
    public void testSimpleLambdaSuccess() throws Exception {
        // you test your lambdas by invoking on http://localhost:8081
        // this works in dev mode too
        String bucketArn = "arn:aws:s3:::source-bucket";
        
        List<Task> tasks = List.of(
                            task(bucketArn, "records_000000000.jsonl.gz"),
                            task(bucketArn, "records_000000001.jsonl"));
        S3BatchEvent event = S3BatchEvent.builder()
                                .withInvocationId("YXNkbGZqYWRmaiBhc2RmdW9hZHNmZGpmaGFzbGtkaGZza2RmaAo")
                                .withInvocationSchemaVersion("1")
                                .withJob(Job.builder()
                                            .withId("f3cc4f60-61f6-4a2b-8a21-d07600c373ce")
                                            .build())
                                .withTasks(tasks)
                                .build();
        given()
                .contentType("application/json")
                .accept("application/json")
                .body(event)
                .when()
                .post()
                .then()
                .statusCode(200)
                // TODO check results
                ;
    }

    public Task task(String bucketArn, String key) {
        return Task.builder()
                    .withS3BucketArn(bucketArn)
                    .withS3Key(key)
                    .withS3VersionId("1")
                    .build();
    }

}
