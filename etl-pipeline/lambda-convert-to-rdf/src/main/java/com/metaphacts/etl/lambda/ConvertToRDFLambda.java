/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

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
import org.eclipse.rdf4j.rio.RDFHandler;
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
import com.metaphacts.etl.lambda.MappingSpec.LineProcessingMode;

import io.carml.engine.rdf.RdfRmlMapper;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.s3.internal.resource.S3ArnConverter;
import software.amazon.awssdk.services.s3.internal.resource.S3BucketResource;
import software.amazon.awssdk.services.s3.internal.resource.S3Resource;

@Named("convert2rdf")
public class ConvertToRDFLambda implements RequestStreamHandler {

    private static final String DEFAULT_DATASET = "default";
    private static final String DEFAULT_MAPPINGS_FILE = "mappings.json";

    private static final Logger logger = LoggerFactory.getLogger(ConvertToRDFLambda.class);

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final ConversionStats listener = new ConversionStats();

    @Inject
    FileHelper fileHelper;
    @Inject
    MappingManager mappingManager;
    @Inject
    LambdaLoggerManager lambdaLoggerManager;

    @ConfigProperty(name = "process.error", defaultValue = "permanent")
    String processErrorResultCode;
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
    String outputDir;
    @ConfigProperty(name = "output.format", defaultValue = "trig")
    String rdfOutputFormat;
    @ConfigProperty(name = "output.compressed", defaultValue = "true")
    Boolean rdfOutputCompressed;
    @ConfigProperty(name = "output.context.base", defaultValue = "https://example.com/")
    String contextBaseNamespace;
    @ConfigProperty(name = "output.batchsize", defaultValue = "1000")
    int outputBatchSize;

    Path resolvedInputDir;
    Path resolvedDownloadDir;
    Path resolvedOutputDir;
    RDFFormat resolvedRdfOutputFormat;

    ResultCode errorResult = ResultCode.PermanentFailure;

    @Inject
    SpecialCases specialCases;

    public ConvertToRDFLambda() {
    }

    @PostConstruct
    protected void init() throws IOException {
        final Path currentDir = Paths.get("").toRealPath();

        resolvedInputDir = currentDir.resolve(inputDir);
        fileHelper.ensureFolderExists(resolvedInputDir).toRealPath();
        resolvedDownloadDir = currentDir.resolve(downloadDir);
        fileHelper.ensureFolderExists(resolvedDownloadDir).toRealPath();
        resolvedOutputDir = currentDir.resolve(outputDir);
        fileHelper.ensureFolderExists(resolvedOutputDir).toRealPath();
        resolvedRdfOutputFormat = Rio.getWriterFormatForFileName("xxx." + rdfOutputFormat)
                                        .or(() -> Rio.getWriterFormatForMIMEType(rdfOutputFormat))
                                        .orElse(RDFFormat.NQUADS);
        if (processErrorResultCode != null && processErrorResultCode.toLowerCase().startsWith("temp")) {
            errorResult = ResultCode.TemporaryFailure;
        }

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
                            .withTreatMissingKeysAs(errorResult)
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
                                .withResultCode(errorResult)
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
                        .withTreatMissingKeysAs(errorResult)
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

    protected String strackTraceToString(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String stacktrace = sw.toString();
        return stacktrace;
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
                result.withResultCode(errorResult)
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
                result.withResultCode(errorResult).withResultString("Failed: file not found");
            }
        } catch (Exception e) {
            final LambdaLogger lambdaLogger = context.getLogger();
            lambdaLogger.log("Failed to process task " + task.getS3Key() + ": " + e.getMessage());
            // lambdaLogger.log("Details: " + strackTraceToString(e));
            result.withResultCode(errorResult)
                    .withResultString("Failed: " + e.getMessage());
        }

        return result.build();
    }

    public long processFile(TaskContext tctx, Mapping mapping, Path sourceFile) throws Exception {
        logger.debug("Processing file {}", sourceFile.toString());

        MappingSpec mappingSpec = mapping.getMappingSpec();
        String dataset = mappingSpec.getDatasetIri();
        String type = mappingSpec.getId();

        long aggregatedSize = 0;

        // try (BufferedReader sourceReader = fileHelper.openInputReader(sourceFile)) {
        try (InputStream sourceStream = fileHelper.openInputStream(sourceFile)) {
            Path outputPath = resolveOutputFile(tctx, mapping, sourceFile, type);
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
                    if (mapping.getMappingSpec().hasProcessingHint(ProcessingHints.COPY_FILE)) {
                        // copy data unchanged
                        IOUtils.copy(sourceStream, out);
                    }
                    else {
                        // create RDF writer and write pre-amble with namespace declarations
                        RDFWriter writer = openRDFFile(tctx, mapping, out, targetContext, mappingManager.getNamespaces());
    
                        boolean processLineByLine = shouldProcessLineByLine(tctx, sourceFile, mapping);
                        if (processLineByLine) {
                            aggregatedSize = processLines(tctx, sourceFile, mapping, sourceStream, writer, outDelete);
                        } else {
                            aggregatedSize = processDocument(tctx, sourceFile, mapping, sourceStream, writer);
                        }
    
                        endRDF(writer);
                    }
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

    private boolean shouldProcessLineByLine(TaskContext tctx, Path sourceFile, Mapping mapping) {
        LineProcessingMode mode = mapping.getMappingSpec().getLineProcessingMode();
        switch (mode) {
        case auto:
            return isJSONLFile(sourceFile);
        case document:
            return false;
        case line:
            return true;
        }
        return false;
    }

    private boolean isJSONLFile(Path sourceFile) {
        String path = sourceFile.toString();
        path = FileHelper.stripExtension(path, FileHelper.EXTENSION_GZ);
        return FileHelper.hasExtension(path, FileHelper.EXTENSION_JSONL);
    }

    private Path resolveOutputFile(TaskContext tctx, Mapping mapping, Path sourceFile, String type) throws IOException {
        Path inputFile = Path.of(tctx.getTask().getS3Key());

        // define file name in output folder
        RDFFormat outputFormat = resolvedRdfOutputFormat;
        Path outputFile = outputFileForSource(tctx, mapping, inputFile, outputFormat);

        Path outputPath = resolvedOutputDir.resolve(outputFile);
        outputPath = outputPath.toAbsolutePath();
        fileHelper.ensureFolderExists(outputPath.getParent());
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
        String outputFile = sourcePath.toString();
        if (mapping.getMappingSpec().hasProcessingHint(ProcessingHints.COPY_FILE)) {
            // returned filename unchanged
            return Path.of(outputFile);
        }
        
        String sourceFile = sourcePath.toString();
        // strip .gz ending
        sourceFile = FileHelper.stripExtension(sourceFile, FileHelper.EXTENSION_GZ);
        // strip .jsonl ending
        sourceFile = FileHelper.stripExtension(sourceFile, FileHelper.EXTENSION_JSONL);

        // append RDF file format extension
        outputFile = sourceFile + "." + outputFormat.getDefaultFileExtension();

        if (rdfOutputCompressed) {
            outputFile = outputFile + FileHelper.EXTENSION_GZ;
        }

        return Path.of(outputFile);
    }

    private long processDocument(TaskContext tctx, Path sourceFile, Mapping mapping, InputStream sourceStream,
            RDFWriter writer) throws Exception {
        long aggregatedSize = 0;

        listener.startDocument();
        boolean success = true;

        try (InputStream input = sourceStream) {

            // perform mapping on whole document
            BatchingRDFWriter batchingWriter = new BatchingRDFWriter(writer, outputBatchSize);
            aggregatedSize += performMapping(tctx, sourceFile, mapping, input, batchingWriter);

        } catch (Exception e) {
            success = false;
            logger.warn("Failed to process batch request: {}", e.toString());
            logger.trace("Details: ", e);

            LambdaLogger lambdaLogger = tctx.getLogger();
            lambdaLogger.log("Failed to process batch request: " + e.toString());

            throw e;
        }
        listener.endDocument(success, aggregatedSize);

        return aggregatedSize;
    }

    private long processLines(TaskContext tctx, Path sourceFile, Mapping mapping, InputStream sourceStream,
            RDFWriter writer, PrintWriter outDelete) throws Exception {
        long errors = 0;
        long successes = 0;
        AtomicLong aggregatedSize = new AtomicLong();
        // convert stream to reader to process line by line
        try (BufferedReader sourceReader = fileHelper.openInputReader(sourceStream)) {
            // process file: iterate over each line in input file
            String line;
            long lineNumber = 0;

            while ((line = sourceReader.readLine()) != null) {
                lineNumber++;
                if (lineNumber % 1000 == 0) {
                    logger.debug("Processed {} lines", lineNumber);
                }
                if (processLines >= 0 && lineNumber > processLines) {
                    logger.debug("finished processing the first {} lines of the file, skipping the remaining content");
                    break;
                }

                if (!specialCases.processLine(tctx, mapping, line)) {
                    // continue with next line
                    continue;
                }

                // process line
                listener.startDocument();
                boolean success = true;
                try {
                    Optional.ofNullable(processLine(tctx, sourceFile, mapping, line)).ifPresent(model -> {

                        boolean addTriplesToOutput = specialCases.saveProcessTriples(tctx, mapping, sourceFile, model,
                                outDelete);

                        if (addTriplesToOutput) {
                            long statements = writeRDF(writer, model);
                            aggregatedSize.addAndGet(statements);
                        }
                    });
                    successes++;
                } catch (Exception e) {
                    success = false;
                    errors++;
                    logger.warn("Failed to process batch request in line {}: {}", lineNumber, e.toString());
                    logger.trace("Failed line {}:", lineNumber);
                    logger.trace(line);
                    logger.trace("Details: ", e);

                    LambdaLogger lambdaLogger = tctx.getLogger();
                    lambdaLogger.log("Failed to process batch request: " + e.toString());
                    lambdaLogger.log("Failed line:");
                    lambdaLogger.log(line);
                    //lambdaLogger.log("Details: " + strackTraceToString(e));
                }
                listener.endDocument(success, aggregatedSize.get());
            }
            logger.debug("Processed {} lines", lineNumber);
        }

        if ((aggregatedSize.get() > 0) && (errors > 0) && (successes == 0)) {
            throw new Exception("Failed to process " + errors + " lines without successful conversions!");
        }

        return aggregatedSize.get();
    }

    private Model processLine(TaskContext tctx, Path sourceFile, Mapping mapping, String line)
            throws IOException {
        Model out = new LinkedHashModel();

        line = specialCases.preprocessLine(line, out);

        boolean performMapping = specialCases.performMapping(line);

        if (performMapping) {
            // perform mapping for document
            try (StringInputStream input = new StringInputStream(line)) {
                Model model = performMapping(tctx, sourceFile, mapping, input);
                out.addAll(model);
            }
        }
        return out;
    }

    /**
     * Perform RDF mapping and return a {@link Model}.
     * 
     * @param tctx       task context
     * @param sourceFile path to source file
     * @param mapping    mapping to apply
     * @param input      input stream
     * @return {@link Model} containing generated RDF statements
     * @throws IOException in case of errors
     */
    private Model performMapping(TaskContext tctx, Path sourceFile, Mapping mapping, InputStream input)
            throws IOException {
        Model out = new LinkedHashModel();

        // perform mapping for document
        try {
            Optional<RdfRmlMapper> rmlMapper = mapping.getMapper();
            if (!rmlMapper.isPresent()) {
                throw new IllegalArgumentException("no RDF mappings available for " + mapping.getType());
            }
            Model model = rmlMapper.get().mapToModel(input);
            out.addAll(model);
        } finally {
            try {
                input.close();
            } catch (Exception e) {
                logger.warn("Failed to close input stream: " + e.getMessage());
                logger.debug("Details: ", e);
            }
        }

        return out;
    }
    
    /**
     * Perform RDF mapping and forward statements to an {@link RDFHandler} (e.g. a
     * {@link RDFWriter}).
     * 
     * @param tctx       task context
     * @param sourceFile path to source file
     * @param mapping    mapping to apply
     * @param input      input stream
     * @param handler    handler for generated RDF statements
     * @return number of mapped statements
     * @throws IOException in case of errors
     */
    private long performMapping(TaskContext tctx, Path sourceFile, Mapping mapping, InputStream input,
            RDFHandler handler)
            throws IOException {
        // perform mapping for document
        AtomicLong count = new AtomicLong(0);
        try {
            Optional<RdfRmlMapper> rmlMapper = mapping.getMapper();
            if (!rmlMapper.isPresent()) {
                throw new IllegalArgumentException("no RDF mappings available for " + mapping.getType());
            }
            Flux<Statement> flux = rmlMapper.get().map(input);
            // send each element to provided handler
            flux.subscribe(statement -> {
                handler.handleStatement(statement);
                count.incrementAndGet();
            });
        } finally {
            try {
                input.close();
            } catch (Exception e) {
                logger.warn("Failed to close input stream: " + e.getMessage());
                logger.debug("Details: ", e);
            }
        }

        return count.get();
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
                fileHelper.downloadFile(bucket, key, localFile);
                return Optional.of(localFile);
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
            fileHelper.uploadToS3(uploadBucket, key, localPath);

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
        if (model == null || model.isEmpty()) {
            return 0;
        }

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
