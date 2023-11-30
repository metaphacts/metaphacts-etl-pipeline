/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Helper class for file related functionality
 */
@ApplicationScoped
public class FileHelper {
    public static final String EXTENSION_GZ = ".gz";
    public static final String EXTENSION_JSONL = ".jsonl";
    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    private static final Logger logger = LoggerFactory.getLogger(FileHelper.class);

    @Inject
    S3Client s3;

    public FileHelper() {

    }

    /**
     * Resolve a filename or URI.
     * 
     * <p>
     * If the relative (to the process' current folder) or absolute file name points
     * to an existing file it is resolved to a {@code file:} URI. Otherwise it is
     * interpreted as URI, but only if it has a scheme/protocol such as
     * {@code http:}, {@code https:}, {@code file:}, or {@code s3:}.
     * </p>
     * 
     * @param fileName file name or URI to resolve.
     * @return the resolved URI or <code>empty</code> if it could not be resolved
     */
    public Optional<URI> resolveFileOrURI(String fileName) {
        if ((fileName == null) || fileName.isBlank()) {
            return Optional.empty();
        }
        fileName = fileName.trim();
        // interpret as local file name
        try {
            final Path currentDir = Paths.get("").toRealPath();
            File file = currentDir.resolve(fileName).toFile();
            if (file.isFile()) {
                // file exists
                return Optional.of(file.toURI());
            }
        } catch (Exception e) {
            logger.warn("Failed to resolve target from local file {}: {}!", fileName, e.getMessage());
        }
        // interpret as URL
        try {
            URI uri = URI.create(fileName);
            // only return URIs with a scheme/protocol
            // plain (and existing) file paths would have been resolved above already
            if (uri.getScheme() != null) {
                return Optional.of(uri);
            }
        } catch (Exception e) {
            logger.warn("Failed to resolve target from url {}: {}!", fileName, e.getMessage());
        }
        return Optional.empty();
    }

    /**
     * Open a file and return an {@link InputStream}.
     * 
     * <p>
     * This method supports URLs like {@code http:}, {@code https:}, or
     * {@code file:} as well as direct paths (without {@code file:}.
     * </p>
     * 
     * <p>
     * {@code s3:} URLs are also supported, assuming that the S3Client is set up to
     * properly resolve them in the configured region and with the provided
     * credentials. Region, credentials, etc. are picked up automatically from the
     * environment as implemented using the auto-discovery mechanism of the AWS SDK.
     * </p>
     * 
     * <p>
     * If the path in the provided URI ends in {@value #EXTENSION_GZ}, the stream is
     * automatically wrapped in a {@link GZIPInputStream}.
     * </p>
     * 
     * @param uri URI of the file to open
     * @return the input stream
     * @throws IOException in case of errors
     */
    public InputStream openInputStream(URI uri) throws IOException {
        InputStream sourceStream = null;
        if ("s3".equalsIgnoreCase(uri.getScheme())) {
            // s3: URL
            S3Utilities s3Utilities = s3.utilities();
            S3Uri s3Uri = s3Utilities.parseUri(uri);
            if (!s3Uri.bucket().isPresent() || !s3Uri.key().isPresent()) {
                throw new IOException("S3 url does not contain bucket");
            }

            GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(s3Uri.bucket().get())
                    .key(s3Uri.key().get()).build();

            ResponseInputStream<GetObjectResponse> response = s3.getObject(getObjectRequest);
            sourceStream = response;
        } else if ("file".equalsIgnoreCase(uri.getScheme())) {
            File f = new File(uri);
            sourceStream = new FileInputStream(f);
        } else {
            URL url = uri.toURL();
            sourceStream = url.openStream();
        }
        if (hasExtension(uri.getPath(), EXTENSION_GZ)) {
            sourceStream = new GZIPInputStream(sourceStream);
        }

        return sourceStream;
    }

    /**
     * Open a file and return a {@link BufferedReader}.
     * 
     * <p>
     * This method supports URLs like {@code http:}, {@code https:}, or
     * {@code file:} as well as direct paths (without {@code file:}.
     * </p>
     * 
     * <p>
     * {@code s3:} URLs are also supported, assuming that the S3Client is set up to
     * properly resolve them in the configured region and with the provided
     * credentials. Region, credentials, etc. are picked up automatically from the
     * environment as implemented using the auto-discovery mechanism of the AWS SDK.
     * </p>
     * 
     * <p>
     * If the path in the provided URI ends in {@value #EXTENSION_GZ}, the stream is
     * automatically wrapped in a {@link GZIPInputStream}.
     * </p>
     * 
     * @param uri URI of the file to open
     * @return the {@link BufferedReader}
     * @throws IOException in case of errors
     */
    public BufferedReader openInputReader(URI uri) throws IOException {
        return openInputReader(openInputStream(uri));
    }

    /**
     * Open a file and return an {@link InputStream}.
     * 
     * <p>
     * If the path ends in {@value #EXTENSION_GZ}, the stream is automatically
     * wrapped in a {@link GZIPInputStream}.
     * </p>
     * 
     * @param sourceFile path of the file to read.
     * @return the input stream
     * @throws IOException in case of errors
     */
    public InputStream openInputStream(Path sourceFile) throws IOException {
        InputStream sourceStream = new FileInputStream(sourceFile.toFile());
        if (hasExtension(sourceFile, EXTENSION_GZ)) {
            sourceStream = new GZIPInputStream(sourceStream);
        }
        return sourceStream;
    }

    /**
     * Open a file and return a {@link BufferedReader}.
     * 
     * <p>
     * If the path ends in {@value #EXTENSION_GZ}, the stream is automatically
     * wrapped in a {@link GZIPInputStream}.
     * </p>
     * 
     * @param sourceFile path of the file to read.
     * @return the {@link BufferedReader}
     * @throws IOException in case of errors
     */
    public BufferedReader openInputReader(Path sourceFile) throws IOException {
        return openInputReader(openInputStream(sourceFile));
    }

    /**
     * Open a stream and return a {@link BufferedReader}.
     * 
     * @param sourceStream the stream from which to read.
     * @return the {@link BufferedReader}
     * @throws IOException in case of errors
     */
    public BufferedReader openInputReader(InputStream sourceStream) throws IOException {
        BufferedReader sourceReader = new BufferedReader(new InputStreamReader(sourceStream, CHARSET_UTF8));
        return sourceReader;
    }

    /**
     * Open a file for writing and return a {@link OutputStream}.
     * 
     * <p>
     * If the path ends in {@value #EXTENSION_GZ}, the stream is automatically
     * wrapped in a {@link GZIPOutputStream}.
     * </p>
     * 
     * @param sourceFile path of the file to read.
     * @return the {@link BufferedReader}
     * @throws IOException in case of errors
     * @param outputPath
     * @return
     * @throws IOException
     */
    public OutputStream openOutputFile(Path outputPath) throws IOException {
        OutputStream out = new FileOutputStream(outputPath.toFile());

        if (hasExtension(outputPath, EXTENSION_GZ)) {
            out = new GZIPOutputStream(out);
        }

        return out;
    }

    /**
     * Find all file {@link Path}s in the file tree starting from given
     * {@link Path}.
     *
     * @param paths the {@link List} of {@link Path}s to search through.
     * @return the {@link List} of file {@link Path}s.
     */
    public static List<Path> resolveFilePaths(List<Path> paths) {
        return paths.stream().flatMap(path -> {
            try (Stream<Path> walk = Files.walk(path)) {
                return walk.filter(Files::isRegularFile).collect(Collectors.toList()).stream();
            } catch (IOException exception) {
                throw new RuntimeException(String.format("Exception occurred while reading path %s", path), exception);
            }
        }).collect(Collectors.toList());
    }

    /**
     * Determines whether a file name has a specific file extension.
     * 
     * @param path      path or filename to check
     * @param extension file extension to check for
     * @return <code>true</code> if the filename has the provided extensions,
     *         <code>false</code> when not
     */
    public static boolean hasExtension(Path path, String extension) {
        String name = path.toString();
        return hasExtension(name, extension);
    }

    /**
     * Determines whether a file name has a specific file extension.
     * 
     * @param fileName  path or filename to check
     * @param extension file extension to check for
     * @return <code>true</code> if the filename has the provided extensions,
     *         <code>false</code> when not
     */
    public static boolean hasExtension(String fileName, String extension) {
        return fileName.toLowerCase().endsWith(extension);
    }

    /**
     * Removes the specified file extension from a file name. When the file name
     * does not have the provided extension, it is returned unchanged.
     * 
     * @param fileName  path or filename to strip
     * @param extension file extension to remove
     * @return stripped or unchanged fileName
     */
    public static String stripExtension(String fileName, String extension) {
        if (hasExtension(fileName, extension)) {
            // strip extension
            return fileName.substring(0, fileName.length() - extension.length());
        }
        // return unchanged
        return fileName;
    }
    
    /**
     * Upload data to a S3 bucket.
     * 
     * @param bucket        bucket to upload to
     * @param key           key (path) within the bucket
     * @param stream        input stream from which to read data
     * @param contentLength length of the data to read
     */
    public void uploadToS3(String bucket, String key, InputStream stream, long contentLength) {
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(key).build();
        /* PutObjectResponse response = */s3.putObject(request,
                RequestBody.fromInputStream(stream, contentLength));
    }

    /**
     * Upload data to a S3 bucket.
     * 
     * @param bucket    bucket to upload to
     * @param key       key (path) within the bucket
     * @param localPath path to file to upload. The path is interpreted relative to
     *                  the process' current directory
     */
    public void uploadToS3(String bucket, String key, Path localPath) {
        PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(key).build();
        /* PutObjectResponse response = */s3.putObject(request, RequestBody.fromFile(localPath));
    }

    /**
     * Create S3 bucket
     * 
     * @param bucket   bucket to create
     */
    public void createS3Bucket(String bucket) {
        CreateBucketRequest request = CreateBucketRequest.builder().bucket(bucket).build();
        /* CreateBucketResponse response = */s3.createBucket(request);
    }

    /**
     * List S3 buckets
     *
     * @return list of buckets
     */
    public List<String> listS3Buckets() {
        ListBucketsRequest request = ListBucketsRequest.builder().build();
        ListBucketsResponse response = s3.listBuckets(request);
        return response.buckets().stream().map(b -> b.name()).collect(Collectors.toList());
    }

    /**
     * List contents of S3 bucket
     * 
     * @param bucket bucket of which to list content
     * @return list of keys in the bucket
     */
    public List<String> listS3BucketContent(String bucket) {
        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucket).build();
        ListObjectsV2Response response = s3.listObjectsV2(request);
        return response.contents().stream().map(c -> c.key()).collect(Collectors.toList());
    }
}
