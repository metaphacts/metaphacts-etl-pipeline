/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.NamespaceAware;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.events.S3BatchEvent;
import com.amazonaws.services.lambda.runtime.events.S3BatchEvent.Task;
import com.amazonaws.services.lambda.runtime.events.S3BatchResponse;
import com.amazonaws.services.lambda.runtime.events.S3BatchResponse.Result;
import com.amazonaws.services.lambda.runtime.events.S3BatchResponse.Result.ResultBuilder;
import com.amazonaws.services.lambda.runtime.events.S3BatchResponse.ResultCode;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import io.carml.engine.rdf.RdfRmlMapper;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.internal.resource.S3ArnConverter;
import software.amazon.awssdk.services.s3.internal.resource.S3BucketResource;
import software.amazon.awssdk.services.s3.internal.resource.S3Resource;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@Named("convert2rdf")
public class ConvertToRDFLambda implements RequestStreamHandler {

    private static final String DEFAULT_DATASET = "default";
    private static final String DEFAULT_MAPPINGS_FILE = "mappings.json";

    private static final Logger logger = LoggerFactory.getLogger(ConvertToRDFLambda.class);

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final ConversionStats listener = new ConversionStats();

    @Inject
    S3Client s3;
    @Inject
    FileHelper fileHelper;
    @Inject
    MappingManager mappingManager;
    @Inject
    LambdaLoggerManager lambdaLoggerManager;

    @ConfigProperty(name = "process.lines", defaultValue = "-1")
    Integer processLines;
    @ConfigProperty(name = "input.dir", defaultValue = "/tmp/input")
    String inputDir;
    @ConfigProperty(name = "download.dir", defaultValue = "/tmp/download")
    String downloadDir;
    @ConfigProperty(name = "download.enabled", defaultValue = "true")
    Boolean downloadEnabled;
    @ConfigProperty(name = "download.delete", defaultValue = "true")
    Boolean downloadDelete;
    @ConfigProperty(name = "upload.bucket", defaultValue = "output-bucket")
    String uploadBucket;
    @ConfigProperty(name = "upload.enabled", defaultValue = "true")
    Boolean uploadEnabled;
    @ConfigProperty(name = "upload.delete", defaultValue = "true")
    Boolean uploadDelete;
    @ConfigProperty(name = "mappings.dir", defaultValue = "mappings")
    String mappingsDir;

    @ConfigProperty(name = "output.dir", defaultValue = "/tmp/output")
    String rdfOutputDir;
    @ConfigProperty(name = "output.format", defaultValue = "trig")
    String rdfOutputFormat;
    @ConfigProperty(name = "output.compressed", defaultValue = "true")
    Boolean rdfOutputCompressed;
    @ConfigProperty(name = "output.context.base", defaultValue = "https://example.com/")
    String contextBaseNamespace;

    Path resolvedInputDir;
    Path resolvedDownloadDir;
    Path resolvedMappingsDir;
    Path resolvedRdfOutputDir;
    RDFFormat resolvedRdfOutputFormat;

    @Inject
    SpecialCases specialCases;

    public ConvertToRDFLambda() {
    }

    @PostConstruct
    protected void init() throws IOException {
        final Path currentDir = Paths.get("").toRealPath();

        resolvedInputDir = currentDir.resolve(inputDir);
        ensureFolderExists(resolvedInputDir).toRealPath();
        resolvedDownloadDir = currentDir.resolve(downloadDir);
        ensureFolderExists(resolvedDownloadDir).toRealPath();
        resolvedMappingsDir = currentDir.resolve(mappingsDir);
        ensureFolderExists(resolvedMappingsDir).toRealPath();
        resolvedRdfOutputDir = currentDir.resolve(rdfOutputDir);
        ensureFolderExists(resolvedRdfOutputDir).toRealPath();
        resolvedRdfOutputFormat = Rio.getWriterFormatForFileName("xxx." + rdfOutputFormat)
                                        .or(() -> Rio.getWriterFormatForMIMEType(rdfOutputFormat))
                                        .orElse(RDFFormat.NQUADS);

        if (uploadEnabled && StringUtils.isAllBlank(uploadBucket)) {
            // no bucket configured
            logger.warn("Output files will not be uploaded to S3: no upload bucket configured!");
            uploadEnabled = false;
        }

        prepareMappers();
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        final int reportInterval = 100;
        final LambdaLogger lambdaLogger = context.getLogger();

        // read batch request from input stream
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, FileHelper.CHARSET_UTF8))) {
            try (PrintWriter writer = new PrintWriter(
                    new BufferedWriter(new OutputStreamWriter(outputStream, FileHelper.CHARSET_UTF8)))) {
                S3BatchEvent request = gson.fromJson(reader, S3BatchEvent.class);
                if (request == null) {
                    logger.warn("invalid input, expecting a S3 Batch event");
                    lambdaLogger.log("invalid input, expecting a S3 Batch event");
                    // write response
                    S3BatchResponse response = S3BatchResponse.builder()
                                                    .withInvocationSchemaVersion("1")
                                                    .withTreatMissingKeysAs(ResultCode.TemporaryFailure)
                                                    .build();
                    writer.write(gson.toJson(response));
                    return;
                }
                List<Task> tasks = request.getTasks();


                logger.info("Processing batch request with {} RDF conversion tasks", tasks.size());
                lambdaLogger.log("Processing batch request with " + tasks.size() + " RDF conversion tasks");

                List<Result> results = new ArrayList<>();
                for (Task task : tasks) {
                    boolean success = true;
                    Result result = null;

                    listener.startInputFile(task.getS3Key());
                    try {
                        logger.info("Processing key {}", task.getS3Key());
                        lambdaLogger.log("Processing key " + task.getS3Key());
                        TaskContext tctx = new TaskContext(context, task);
                        result = processTask(tctx);
                    } catch (Exception e) {
                        result = Result.builder()
                                        .withTaskId(task.getTaskId())
                                        .withResultCode(ResultCode.TemporaryFailure)
                                        .withResultString(e.getMessage()).build();
                        logger.warn("Failed to process task {}: {}", task.getS3Key(), e.getMessage());
                        logger.debug("Details:", e);
                        lambdaLogger.log("Failed to process task " + task.getS3Key() + ": " + e.getMessage());
                    }
                    results.add(result);
                    if (result.getResultCode() != ResultCode.Succeeded) {
                        success = false;
                        logger.warn("Failed to process task {}: {}", task.getS3Key(), result.getResultString());
                        lambdaLogger.log("Failed to process task " + task.getS3Key() + ": " + result.getResultString());
                    }
                    listener.endInputFile(success);

                    // report back every N input files
                    if (listener.getInputFileCount() % reportInterval == 0) {
                        logger.info(listener.getSummary());
                        lambdaLogger.log(listener.getSummary());
                    }
                }

                logger.info("Finished processing batch request");
                logger.info(listener.getSummary());
                lambdaLogger.log("Finished processing batch request");
                lambdaLogger.log(listener.getSummary());

                // write response
                S3BatchResponse response = S3BatchResponse.fromS3BatchEvent(request)
                                                            .withResults(results)
                                                            .withTreatMissingKeysAs(ResultCode.TemporaryFailure)
                                                            .build();
                writer.write(gson.toJson(response));
                if (writer.checkError()) {
                    logger.warn("WARNING: writer encountered an error.");
                    lambdaLogger.log("WARNING: writer encountered an error.");
                }
            } catch (IllegalStateException | JsonSyntaxException exception) {
                logger.warn("Failed to process batch request: {}", exception.toString());
                logger.debug("Details: ", exception);
                lambdaLogger.log("Failed to process batch request: " + exception.toString());
            }
        }
    }

    private Result processTask(TaskContext context) {
        Task task = context.getTask();
        ResultBuilder result = Result.builder()
                .withTaskId(task.getTaskId());
        
        try {
            // determine mapping for target file
            String taskFileName = context.getTask().getS3Key();
            Optional<Mapping> mappingHolder = mappingManager.getMappingFor(taskFileName);
            if (mappingHolder.isEmpty()) {
                result.withResultCode(ResultCode.TemporaryFailure)
                        .withResultString("Failed: no matching mapping found");
                return result.build();
            }
            Mapping mapping = mappingHolder.get();

            // download file to local folder (or access file directly if available)
            Optional<Path> sourceFileHolder = downloadFile(context);
            if (sourceFileHolder.isPresent()) {
                Path sourceFile = sourceFileHolder.get();
                try {
                    lambdaLoggerManager.set(context.getLogger());
                    long statementCount = processFile(context, mapping, sourceFile);
                    result.withResultCode(ResultCode.Succeeded).withResultString("successfully processed file "
                            + task.getS3Key() + " with " + statementCount + " RDF statements");
                } finally {
                    if (sourceFile.startsWith(resolvedDownloadDir) && downloadDelete) {
                        // file was downloaded, so we delete it after processing
                        deleteFile(sourceFile);
                    }
                    lambdaLoggerManager.remove();
                }
            } else {
                result.withResultCode(ResultCode.TemporaryFailure).withResultString("Failed: file not found");
            }
        } catch (Exception e) {
            result.withResultCode(ResultCode.TemporaryFailure)
                    .withResultString("Failed: " + e.getMessage());
        }

        return result.build();
    }

    public long processFile(TaskContext tctx, Mapping mapping, Path sourceFile) throws IOException {
        logger.debug("Processing file {}", sourceFile.toString());

        MappingSpec mappingSpec = mapping.getMappingSpec();
        String dataset = mappingSpec.getDatasetIri();
        String type = mappingSpec.getId();

        long aggregatedSize = 0;
        try (BufferedReader sourceReader = fileHelper.openInputReader(sourceFile)) {
            Path outputPath = resolveRDFFile(tctx, mapping, sourceFile, type);
            Path outputPathDelete = Paths.get(outputPath.toString() + SpecialCases.SUFFIX_DELETE);
            // determine the target named graph for the source file
            Path inputFile = Path.of(tctx.getTask().getS3Key());

            Resource targetContext = targetContextForSource(mappingSpec, inputFile, type, Optional.empty());
            // Add dataset to Context for datasets different from default
            if ((dataset == null) || !DEFAULT_DATASET.equals(dataset)) {
                targetContext = targetContextForSource(mappingSpec, inputFile, type, Optional.ofNullable(dataset));
            }

            logger.debug("Saving statements from file {} to file {} with context {}", inputFile, outputPath,
                    targetContext.stringValue());
            // open CSV file for delete IRIs list
            try (PrintWriter outDelete = new PrintWriter(fileHelper.openOutputFile(outputPathDelete))) {
                // open RDF file for output
                try (OutputStream out = fileHelper.openOutputFile(outputPath)) {
                    // create RDF writer and write pre-amble with namespace declarations
                    RDFWriter writer = openRDFFile(tctx, mapping, out, targetContext, mappingManager.getNamespaces());

                    // process file: iterate over each line in input file
                    // TODO refactor to allow processing complete file
                    String line;
                    long lineNumber = 0;
                    while ((line = sourceReader.readLine()) != null) {
                        lineNumber++;
                        if (lineNumber % 1000 == 0) {
                            logger.debug("Processed {} lines", lineNumber);
                        }
                        if (processLines >= 0 && lineNumber > processLines) {
                            logger.debug(
                                    "finished processing the first {} lines of the file, skipping the remaining content");
                            break;
                        }

                        if (!specialCases.processLine(tctx, mapping, line)) {
                            // continue with next line
                            continue;
                        }

                        // process line
                        listener.startDocument();
                        boolean success = true;
                        Model model = null;
                        try {
                            model = processLine(tctx, sourceFile, mapping, line);

                            boolean addTriplesToOutput = specialCases.saveProcessTriples(tctx, mapping, sourceFile, model,
                                    outDelete);

                            if (addTriplesToOutput) {
                                aggregatedSize += writeRDF(writer, model);
                            }
                        } catch (Exception e) {
                            success = false;
                            logger.warn("Failed to process batch request in line {}: {}", lineNumber, e.toString());
                            logger.trace("Failed line {}:", lineNumber);
                            logger.trace(line);
                            logger.trace("Details: ", e);

                            LambdaLogger lambdaLogger = tctx.getLogger();
                            lambdaLogger.log("Failed to process batch request: " + e.toString());
                            lambdaLogger.log("Failed line:");
                            lambdaLogger.log(line);
                        }
                        listener.endDocument(success, (model != null ? model.size() : 0));
                    }
                    logger.debug("Processed {} lines", lineNumber);
                    endRDF(writer);
                }
            }
            boolean saveResults = specialCases.saveResults(tctx, mapping);
            if (saveResults) {
                // upload to S3
                Optional<Path> outputFile = uploadFile(tctx, mapping, sourceFile, outputPath);
                if (uploadDelete) {
                    deleteFile(outputPath);
                }
                if (outputFile.isPresent()) {
                    specialCases.onUploadFile(tctx, mapping, sourceFile, outputPathDelete, outputFile.get());
                    if (uploadDelete) {
                        deleteFile(outputPathDelete);
                    }
                }
            }

            return aggregatedSize;
        }
    }

    private Path resolveRDFFile(TaskContext tctx, Mapping mapping, Path sourceFile, String type) throws IOException {
        Path inputFile = Path.of(tctx.getTask().getS3Key());

        // define file name in output folder
        RDFFormat outputFormat = resolvedRdfOutputFormat;
        Path outputFile = outputFileForSource(tctx, mapping, inputFile, outputFormat);

        Path outputPath = resolvedRdfOutputDir.resolve(outputFile);
        outputPath = outputPath.toAbsolutePath();
        ensureFolderExists(outputPath.getParent());
        return outputPath;
    }

    private RDFWriter openRDFFile(TaskContext tctx, Mapping mapping, OutputStream outputStream, Resource targetContext,
            Model namespaces) throws IOException {
        RDFFormat outputFormat = resolvedRdfOutputFormat;

        var settings = new WriterConfig();
        settings.set(BasicWriterSettings.PRETTY_PRINT, true);

        RDFWriter writer = Rio.createWriter(outputFormat, outputStream);
        writer.setWriterConfig(settings);

        // determine named graph for result data
        if (targetContext != null) {
            writer = FixedContextRDFWriterWrapper.fixedContextFor(writer, targetContext);
        }

        // write statements to file
        writer.startRDF();

        if (namespaces instanceof NamespaceAware) {
            for (Namespace nextNamespace : ((NamespaceAware) namespaces).getNamespaces()) {
                writer.handleNamespace(nextNamespace.getPrefix(), nextNamespace.getName());
            }
        }
        return writer;
    }

    private Path ensureFolderExists(Path outputFolder) {
        if (!Files.isDirectory(outputFolder)) {
            try {
                logger.debug("Creating folder {}", outputFolder);
                Files.createDirectories(outputFolder);
            } catch (IOException e) {
                logger.warn("Failed to create folder {}: {}", outputFolder, e.getMessage());
                logger.debug("Details: ", e);
                throw new RuntimeException(String.format("Failed to create folder %s", outputFolder), e);
            }
        }
        return outputFolder;
    }

    private Resource targetContextForSource(MappingSpec mappingSpec, Path sourceFile, String type,
            Optional<String> dataset) {
        IRI targetContext = null;
        String datasetIri = mappingSpec.getDatasetIri();
        if (datasetIri != null) {
            targetContext = Values.iri(datasetIri);
        } else {
            targetContext = Values.iri(contextBaseNamespace, type + "/");
            if (dataset.isPresent()) {
                targetContext = Values.iri(contextBaseNamespace, dataset.get() + "/" + type + "/");
            }
        }
        return targetContext;
    }

    private Path outputFileForSource(TaskContext tctx, Mapping mapping, Path sourcePath, RDFFormat outputFormat) {
        String sourceFile = sourcePath.toString();
        // strip .gz ending
        sourceFile = FileHelper.stripExtension(sourceFile, FileHelper.EXTENSION_GZ);
        // strip .jsonl ending
        sourceFile = FileHelper.stripExtension(sourceFile, FileHelper.EXTENSION_JSONL);

        // append RDF file format extension
        String outputFile = sourceFile + "." + outputFormat.getDefaultFileExtension();

        if (rdfOutputCompressed) {
            outputFile = outputFile + FileHelper.EXTENSION_GZ;
        }

        return Path.of(outputFile);
    }

    private Model processLine(TaskContext tctx, Path sourceFile, Mapping mapping, String line)
            throws IOException {
        Model out = new LinkedHashModel();

        line = specialCases.preprocessLine(line, out);

        boolean performMapping = specialCases.performMapping(line);

        if (performMapping) {
            // perform mapping for document
            try (StringInputStream input = new StringInputStream(line)) {
                RdfRmlMapper rmlMapper = mapping.getMapper();
                Model model = rmlMapper.mapToModel(input);
                out.addAll(model);
            }
        }
        return out;
    }

    private Optional<Path> downloadFile(TaskContext context) {
        String bucket = getSourceBucket(context);
        String key = context.getTask().getS3Key();
        logger.debug("Downloading file {}/{}", bucket, key);

        // for now we try to find the file locally
        Path localFile = resolvedInputDir.resolve(key);
        if (localFile.toFile().exists()) {
            logger.debug("Skipped downloading file from bucket {}, using local file {}", bucket,
                    key);
            return Optional.of(localFile);
        }

        if (downloadEnabled) {
            // download file
            try {
                localFile = resolvedDownloadDir.resolve(key);
                ensureFolderExists(localFile.getParent());
                GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket)
                        .key(key).build();

                try (ResponseInputStream<GetObjectResponse> response = s3.getObject(getObjectRequest)) {
                    try (OutputStream out = new FileOutputStream(localFile.toFile())) {
                        // write to local file
                        IOUtils.copy(response, out);
                    }
                    return Optional.of(localFile);
                }
            } catch (Exception e) {
                logger.warn("Failed to download file {}/{}: {}", bucket, key,
                        e.getMessage());
                logger.debug("Details: ", e);
                return Optional.empty();
            }
        }

        logger.warn("Skipped file {}/{}: not found", bucket, key);
        return Optional.empty();
    }

    private String getSourceBucket(TaskContext context) {
        String s3BucketArn = context.getTask().getS3BucketArn();
        try {
            S3Resource s3res = S3ArnConverter.create().convertArn(Arn.fromString(s3BucketArn));
            if (s3res instanceof S3BucketResource) {
                return ((S3BucketResource) s3res).bucketName();
            }
        } catch (Exception e) {
            // bucket does not seem to represent an ARN, ignore and use it directly
        }
        // get last past part of ARN
        int pos = s3BucketArn.lastIndexOf(':');
        if (pos >= 0) {
            return s3BucketArn.substring(pos + 1);
        }
        return s3BucketArn;
    }

    private Optional<Path> uploadFile(TaskContext tctx, Mapping mapping, Path sourceFile, Path localPath) {
        if (!uploadEnabled) {
            // skip uploading
            logger.trace("Skipped uploading file {}: upload disabled", localPath);
            return Optional.empty();
        }
        logger.debug("Uploading file {} to {}", localPath, uploadBucket);
        tctx.getLogger().log("Uploading file " + localPath + " to " + uploadBucket);

        Path inputFile = Path.of(tctx.getTask().getS3Key());

        // define file name in output folder
        RDFFormat outputFormat = resolvedRdfOutputFormat;
        Path outputFile = outputFileForSource(tctx, mapping, inputFile, outputFormat);
        String key = outputFile.toString();
        
        // upload to S3
        try {
            PutObjectRequest putRequest = PutObjectRequest.builder().bucket(uploadBucket).key(key).build();
            PutObjectResponse putResponse = s3.putObject(putRequest, RequestBody.fromFile(localPath));
            logger.trace("Successfully uploaded file {}/{}: {}", uploadBucket, key, putResponse);

            return Optional.of(outputFile);
        } catch (Exception e) {
            logger.warn("Failed to upload file {}/{}: {}", uploadBucket, key, e.getMessage());
            logger.debug("Details: ", e);
            return Optional.empty();
        }
    }

    private void deleteFile(Path localPath) {
        // file was uploaded, so we delete it after processing
        logger.debug("Deleting file {}", localPath);
        try {
            boolean deleted = localPath.toFile().delete();
            if (!deleted) {
                logger.warn("Failed to delete file {}!", localPath);
            }
        } catch (Exception e) {
            logger.warn("Failed to delete file {}: {}", localPath, e.getMessage());
            logger.debug("Details: ", e);
        }
    }

    private void prepareMappers() {
        Optional<URI> mappingConfigURIHolder = getMappingConfigURI();
        if (!mappingConfigURIHolder.isPresent()) {
            // TODO lambdaLogger?
            logger.warn("No or no valid mapping config file, using empty mapping config!");
            return;
        }
        mappingManager.prepareMappers(mappingConfigURIHolder.get(), resolvedInputDir);
    }

    private Optional<URI> getMappingConfigURI() {
        if ((mappingsDir != null) && !mappingsDir.isBlank()) {
            String fileName = mappingsDir.trim();
            if (fileName.endsWith("/")) {
                // URI seems to point to a folder
                // try well-known file name in specified mappings directory
                fileName = fileName + DEFAULT_MAPPINGS_FILE;
            }
            Optional<URI> mappingFileURI = fileHelper.resolveFileOrURI(fileName);
            if (mappingFileURI.isPresent()) {
                return mappingFileURI;
            }
        }

        return Optional.empty();
    }

    private long writeRDF(RDFWriter writer, Model model) {

        for (final Statement st : model) {
            writer.handleStatement(st);
        }
        return model.size();
    }

    private void endRDF(RDFWriter writer) {
        writer.endRDF();
    }

    static class StringInputStream extends ByteArrayInputStream {
        public StringInputStream(String s) {
            super(s.getBytes(StandardCharsets.UTF_8));
        }
    }
}
