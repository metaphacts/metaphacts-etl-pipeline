/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import static com.metaphacts.etl.lambda.S3BatchOperationsTestUtils.batchEvent;
import static com.metaphacts.etl.lambda.S3BatchOperationsTestUtils.createS3Bucket;
import static com.metaphacts.etl.lambda.S3BatchOperationsTestUtils.listS3BucketContent;
import static com.metaphacts.etl.lambda.S3BatchOperationsTestUtils.listS3Buckets;
import static com.metaphacts.etl.lambda.S3BatchOperationsTestUtils.successfulS3BatchEvent;
import static com.metaphacts.etl.lambda.S3BatchOperationsTestUtils.task;
import static com.metaphacts.etl.lambda.S3BatchOperationsTestUtils.uploadToS3;
import static io.restassured.RestAssured.given;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.lambda.runtime.events.S3BatchEvent;
import com.google.gson.Gson;

import io.restassured.response.ValidatableResponse;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.s3.S3Client;

// TODO try to make this work within the builder container
//@org.junit.jupiter.api.Disabled("Disabled for now as it requires Docker for the test environment which is not available within the Docker container used to build and run this test")
@io.quarkus.test.junit.QuarkusTest
public class LambdaHandlerTest {

    private static final String MAPPINGS_BUCKET = "mappings-bucket";
    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String OUTPUT_BUCKET = "output-bucket";

    @Inject
    S3Client s3;

    @Test
    public void testSimpleLambdaSuccess() throws Exception {
        // you test your lambdas by invoking on http://localhost:8081
        // this works in dev mode too
        String sourceBucket = SOURCE_BUCKET; // "arn:aws:s3:::source-bucket";
        String outputBucket = OUTPUT_BUCKET;
        
        bootstrapMappings();

        S3BatchEvent event = batchEvent(
                task(sourceBucket, "publications/0000001/records_000000001.jsonl"),
                task(sourceBucket, "authors/authors.jsonl"));
        ValidatableResponse response = given()
                .contentType("application/json")
                .accept("application/json")
                .body(event)
                .when()
                .post()
                .then()
                .statusCode(200);

        System.out.println("Output Content:");
        listS3BucketContent(s3, outputBucket).forEach(System.out::println);

        // TODO verify results
        response.body(successfulS3BatchEvent(event));
    }

    @BeforeEach
    public void bootstrapTestData() {
        bootstrapTestData(SOURCE_BUCKET, MAPPINGS_BUCKET, OUTPUT_BUCKET);
    }

    protected void bootstrapMappings() {
        MappingConfig mappingsConfig = new MappingConfig();
        mappingsConfig.addMappings(
                new MappingSpec("publications")
                        .withMappingFiles("publications.ttl")
                        .withSourceFileIncludePattern("publications/.*/records_*.jsonl")
                        .withProcessingHints("json-hierarchy", "deletion-detection", "root-to-list"),
                new MappingSpec("researchers")
                        .withMappingFiles("authors.ttl")
                        .withSourceFileIncludePattern("authors/.*.jsonl")
                        .withProcessingHints("json-hierarchy", "deletion-detection", "root-to-list"));
        Gson gson = new Gson();
        String mappingsConfigJson = gson.toJson(mappingsConfig);
        System.out.println(mappingsConfigJson);
    }

    protected void bootstrapTestData(String sourceBucket, String mappingsBucket, String outputBucket) {
        // bootstrap test data
        createS3Bucket(s3, sourceBucket);
        createS3Bucket(s3, mappingsBucket);
        createS3Bucket(s3, outputBucket);

        Path mappingsFolder = Path.of("src/test");

        // upload mapping files to S3
        List.of("mappings/mappings.json",
                "mappings/authors.ttl",
                "mappings/publications.ttl").forEach(file -> {
                    Path p = Path.of(file);
                    System.out.println("uploading " + file);
                    uploadToS3(s3, mappingsBucket, p.toString(), mappingsFolder.resolve(p));
                });

        // upload source files to S3
        Path sourceFolder = Path.of("src/test/source-data/jsonl");
        List.of("publications/0000001/records_000000001.jsonl",
                "authors/authors.jsonl").forEach(file -> {
                    Path p = Path.of(file);
                    System.out.println("uploading " + file);
                    uploadToS3(s3, sourceBucket, p.toString(), sourceFolder.resolve(p));
                });

        System.out.println("Buckets:");
        listS3Buckets(s3).forEach(System.out::println);

        System.out.println("Source Content:");
        listS3BucketContent(s3, sourceBucket).forEach(System.out::println);
        System.out.println("Mappings Content:");
        listS3BucketContent(s3, mappingsBucket).forEach(System.out::println);

    }
}
