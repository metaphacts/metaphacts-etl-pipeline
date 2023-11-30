/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.NamespaceAware;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.util.Statements;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
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
import io.carml.logicalsourceresolver.CsvResolver;
import io.carml.logicalsourceresolver.JsonPathResolver;
import io.carml.logicalsourceresolver.XPathResolver;
import io.carml.model.TriplesMap;
import io.carml.util.ModelSerializer;
import io.carml.util.RmlMappingLoader;
import io.carml.util.RmlNamespaces;
import io.carml.vocab.Carml;
import io.carml.vocab.Rdf;
import io.carml.vocab.Rml;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.internal.resource.S3ArnConverter;
import software.amazon.awssdk.services.s3.internal.resource.S3BucketResource;
import software.amazon.awssdk.services.s3.internal.resource.S3Resource;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@Named("convert2rdf")
public class ConvertToRDFLambda implements RequestStreamHandler {

    public enum LiteralConversionMode {
        noValidation, validateAndFail, validateAndFallbackToString, validateAndFallbackToPlaceholder;
    }

    private static final String DEFAULT_DATASET = "default";
    private static final String EXTENSION_GZ = ".gz";
    private static final String EXTENSION_JSONL = ".jsonl";
    private static final String DEFAULT_MAPPINGS_FILE = "mappings.json";

    private static final Logger logger = LoggerFactory.getLogger(ConvertToRDFLambda.class);

    private static final String BASE_URI = "http://base.metaphacts.com/";
    private static final IRI BASE_IRI = Values.iri(BASE_URI);
    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final ConversionStats listener = new ConversionStats();
    private final Map<String, Mapping> mappings = new TreeMap<>();
    private final InheritableThreadLocal<LambdaLogger> lambdaLoggerTL = new InheritableThreadLocal<>();

    @Inject
    S3Client s3;

    Model namespaces = new TreeModel();

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
    // see LiteralConversionMode for allowed values
    @ConfigProperty(name = "mappings.literalConversionMode", defaultValue = "validateAndFallbackToPlaceholder")
    String mappingsLiteralConversionMode;
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

        // load namespace declarations to be used for pretty printing
        try (InputStream namespaceStream = getClass().getResourceAsStream("/namespaces.ttl")) {
            namespaces = Rio.parse(namespaceStream, RDFFormat.TURTLE);
        } catch (Exception e) {
            logger.warn("failed to load namespaces from {}: {}", e.getMessage());
            logger.debug("Details: ", e);
        }

        prepareMappers();
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        final int reportInterval = 100;
        final LambdaLogger lambdaLogger = context.getLogger();

        // read batch request from input stream
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, CHARSET_UTF8))) {
            try (PrintWriter writer = new PrintWriter(
                    new BufferedWriter(new OutputStreamWriter(outputStream, CHARSET_UTF8)))) {
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
            Optional<Mapping> mappingHolder = getMappingFor(taskFileName);
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
                    lambdaLoggerTL.set(context.getLogger());
                    long statementCount = processFile(context, mapping, sourceFile);
                    result.withResultCode(ResultCode.Succeeded).withResultString("successfully processed file "
                            + task.getS3Key() + " with " + statementCount + " RDF statements");
                } finally {
                    if (sourceFile.startsWith(resolvedDownloadDir) && downloadDelete) {
                        // file was downloaded, so we delete it after processing
                        deleteFile(sourceFile);
                    }
                    lambdaLoggerTL.remove();
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
        try (BufferedReader sourceReader = openInputFile(sourceFile)) {
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
            try (PrintWriter outDelete = new PrintWriter(openOutputFile(outputPathDelete))) {  
                // open RDF file for output
                try (OutputStream out = openOutputFile(outputPath)) {
                    // create RDF writer and write pre-amble with namespace declarations
                    RDFWriter writer = openRDFFile(tctx, mapping, out, targetContext, namespaces);

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
        sourceFile = stripExtension(sourceFile, EXTENSION_GZ);
        // strip .jsonl ending
        sourceFile = stripExtension(sourceFile, EXTENSION_JSONL);

        // append RDF file format extension
        String outputFile = sourceFile + "." + outputFormat.getDefaultFileExtension();

        if (rdfOutputCompressed) {
            outputFile = outputFile + EXTENSION_GZ;
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

    private BufferedReader openInputFile(URI uri) throws IOException {
        InputStream sourceStream = null;
        if ("s3".equalsIgnoreCase(uri.getScheme())) {
            // s3: URL
            S3Utilities s3Utilities = s3.utilities();
            S3Uri s3Uri = s3Utilities.parseUri(uri);
            if (!s3Uri.bucket().isPresent() || !s3Uri.key().isPresent()) {
                throw new IOException("S3 url does not contain bucket");
            }
            
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                                                    .bucket(s3Uri.bucket().get())
                                                    .key(s3Uri.key().get())
                                                    .build();

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
        BufferedReader sourceReader = new BufferedReader(new InputStreamReader(sourceStream, CHARSET_UTF8));
        return sourceReader;
    }

    private BufferedReader openInputFile(Path sourceFile) throws IOException {
        InputStream sourceStream = new FileInputStream(sourceFile.toFile());
        if (hasExtension(sourceFile, EXTENSION_GZ)) {
            sourceStream = new GZIPInputStream(sourceStream);
        }
        BufferedReader sourceReader = new BufferedReader(new InputStreamReader(sourceStream, CHARSET_UTF8));
        return sourceReader;
    }

    private OutputStream openOutputFile(Path outputPath) throws IOException {
        OutputStream out = new FileOutputStream(outputPath.toFile());

        if (hasExtension(outputPath, EXTENSION_GZ)) {
            out = new GZIPOutputStream(out);
        }
        
        return out;
    }

    private boolean hasExtension(Path path, String extension) {
        String name = path.toString();
        return hasExtension(name, extension);
    }

    private boolean hasExtension(String fileName, String extension) {
        return fileName.toLowerCase().endsWith(extension);
    }

    private String stripExtension(String fileName, String extension) {
        if (hasExtension(fileName, extension)) {
            // strip extension
            return fileName.substring(0, fileName.length() - extension.length());
        }
        // return unchanged
        return fileName;
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

    /**
     * Load {@link Model} from provided url.
     *
     * @param uri the {@link URL} from which to load RDF data
     * @return the {@link Model}.
     */
    private Model loadModel(URI uri) {
        return parseRDFToStream(uri).collect(new ModelCollector());
    }

    private Stream<Statement> parseRDFToStream(URI uri) {
        return parseRDFToModel(uri)
                    .map(m -> m.stream())
                    .orElse(Stream.empty());
    }

    private Optional<Model> parseRDFToModel(URI uri) {
        var fileName = uri.getPath();

        Optional<RDFFormat> rdfFormat = Rio.getParserFormatForFileName(fileName);
        if (!rdfFormat.isPresent()) {
            logger.debug("Skipping file {}: not recognized as RDF file", fileName);
            return Optional.empty();
        }
        try (Reader is = openInputFile(uri)) {
            logger.debug("Loading file {}", uri);
            return Optional.of(Rio.parse(is, BASE_URI, rdfFormat.get()));
        } catch (IOException | RDFParseException exception) {
            throw new RuntimeException(String.format("Exception occurred while parsing %s", uri), exception);
        }
    }

    /**
     * Find all file {@link Path}s in the file tree starting from given
     * {@link Path}.
     *
     * @param paths the {@link List} of {@link Path}s to search through.
     * @return the {@link List} of file {@link Path}s.
     */
    protected static List<Path> resolveFilePaths(List<Path> paths) {
        return paths.stream().flatMap(path -> {
            try (Stream<Path> walk = Files.walk(path)) {
                return walk.filter(Files::isRegularFile).collect(Collectors.toList()).stream();
            } catch (IOException exception) {
                throw new RuntimeException(String.format("Exception occurred while reading path %s", path), exception);
            }
        }).collect(Collectors.toList());
    }

    private void prepareMappers() {
        Optional<URI> mappingConfigURIHolder = getMappingConfigURI();
        if (!mappingConfigURIHolder.isPresent()) {
            // TODO lambdaLogger?
            logger.warn("No or no valid mapping config file, using empty mapping config!");
            return;
        }

        URI mappingConfigURI = mappingConfigURIHolder.get();
        logger.debug("Loading mapping config from path {} ...", mappingConfigURI);
        Optional<MappingConfig> mappingConfigHolder = getMappingConfig(mappingConfigURI);
        if (!mappingConfigHolder.isPresent()) {
            // TODO lambdaLogger?
            logger.warn("Failed to load mapping config from path {}!", mappingConfigURI);
            return;
        }

        // iterate over all mapping specs
        MappingConfig mappingConfig = mappingConfigHolder.get();
        for (MappingSpec spec : mappingConfig.getMappings()) {
            try {
                Model mappingModel = null;
                for (String mappingFile : spec.getMappingFiles()) {
                    URI mappingFileURI = mappingConfigURI.resolve(mappingFile);
                    logger.debug("loading mappings for {} from {}", spec.getId(), mappingFileURI);
                    try {
                        Model model = loadModel(mappingFileURI);
                        if (mappingModel == null) {
                            // first mapping file
                            mappingModel = model;
                        } else {
                            // additional files, merge into aggregated mappings model
                            mappingModel.addAll(model);
                        }
                    } catch (Exception e) {
                        logger.warn("failed to load mappings from {}: {}", mappingFileURI, e.getMessage());
                        logger.debug("Details: ", e);
                        throw e;
                    }
                }

                createMapping(mappingConfigURI, spec, mappingModel);
            } catch (Exception e) {
                logger.warn("failed to load mappings for {}: {}", spec.getId(), e.getMessage());
                logger.debug("Details: ", e);
            }
        }
    }

    private void createMapping(URI mappingConfigURI, MappingSpec spec, Model mappingModel) {
        RdfRmlMapper mapper = prepareMapper(mappingModel);
        Mapping mapping = new Mapping(spec, mappingModel, mapper);
        mappings.put(spec.getId().toLowerCase(), mapping);
    }

    private Optional<Mapping> getMappingFor(String fileName) {
        // find matching mapping
        for (Mapping mapping : mappings.values()) {
            if (mapping.matches(fileName)) {
                return Optional.of(mapping);
            }
        }
        return Optional.empty();
    }

    private Optional<URI> getMappingConfigURI() {
        if ((mappingsDir != null) && !mappingsDir.isBlank()) {
            String fileName = mappingsDir.trim();
            if (fileName.endsWith("/")) {
                // URI seems to point to a folder
                // try well-known file name in specified mappings directory
                fileName = fileName + DEFAULT_MAPPINGS_FILE;
            }
            Optional<URI> mappingFileURI = resolveFileOrURI(fileName);
            if (mappingFileURI.isPresent()) {
                return mappingFileURI;
            }
        }

        return Optional.empty();
    }

    private Optional<URI> resolveFileOrURI(String fileName) {
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

    private Optional<MappingConfig> getMappingConfig(URI mappingConfigURI) {
        logger.info("Loading mappings from {}", mappingConfigURI);
        try {
            try (Reader reader = openInputFile(mappingConfigURI)) {
                MappingConfig mappingsConfig = gson.fromJson(reader, MappingConfig.class);
                return Optional.of(mappingsConfig);
            }
        } catch (Exception e) {
            logger.warn("Failed to load mappings from {}: {}!", mappingConfigURI, e.getMessage());
            logger.debug("Details: ", e);
        }

        return Optional.empty();
    }

    private RdfRmlMapper prepareMapper(Model mappingModel) {
        var mapping = loadMapping(mappingModel);

        if (logger.isDebugEnabled()) {
            logger.debug("The following mapping constructs were detected:");
            logger.debug("{}{}", System.lineSeparator(), ModelSerializer.serializeAsRdf(mappingModel, RDFFormat.TURTLE,
                    ModelSerializer.SIMPLE_WRITER_CONFIG, n -> n));
        }

        var mapperBuilder = RdfRmlMapper.builder()
                                .baseIri(BASE_IRI)
                                // add mappings
                                .triplesMaps(mapping)
                                .valueFactorySupplier(getValueFactory())
                                .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
                                .setLogicalSourceResolver(Rdf.Ql.XPath, XPathResolver::getInstance)
                                .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance);

        var relativeSourceLocation = Optional.of(resolvedInputDir);
        relativeSourceLocation.ifPresent(location -> {
            logger.debug("Setting relative source location {} ...", location);
            mapperBuilder.fileResolver(location);
        });
        mapperBuilder.addFunctions(new RmlFunctions());
        return mapperBuilder.build();
    }

    private Supplier<ValueFactory> getValueFactory() {
        LiteralConversionMode mode = LiteralConversionMode.validateAndFallbackToPlaceholder;
        try {
            mode = LiteralConversionMode.valueOf(mappingsLiteralConversionMode);
        } catch (Exception e) {
            logger.warn("invalid value for mappings.literalConversionMode: {}! Allowed values are {}",
                    mappingsLiteralConversionMode, LiteralConversionMode.values());
        }
        switch (mode) {
        default:
        case noValidation:
            return () -> SimpleValueFactory.getInstance();
        case validateAndFail:
            return () -> new LoggingValidatingValueFactory() {
                protected void warn(String message) {
                    super.warn(message);
                    ConvertToRDFLambda.this.handleValueFactoryWarning(message);
                }
            };
        case validateAndFallbackToString:
            return () -> new ValidatingValueFactoryWithFallback() {
                protected void warn(String message) {
                    super.warn(message);
                    ConvertToRDFLambda.this.handleValueFactoryWarning(message);
                }
            };
        case validateAndFallbackToPlaceholder:
            return () -> new ValidatingValueFactoryWithPlaceholder() {
                protected void warn(String message) {
                    super.warn(message);
                    ConvertToRDFLambda.this.handleValueFactoryWarning(message);
                }
            };
        }
    }

    protected void handleValueFactoryWarning(String message) {
        try {
            LambdaLogger lambdaLogger = lambdaLoggerTL.get();
            lambdaLogger.log(message);
        } catch (Exception e) {
            // ignore
            logger.warn("failed to get Lambda Logger: {}", e.getMessage());
            logger.debug("Details: ", e);
        }
    }

    private Set<TriplesMap> loadMapping(Model mappingModel) {
        namespaces.getNamespaces().forEach(mappingModel::setNamespace);
        RmlNamespaces.applyRmlNameSpaces(mappingModel);

        // replace RML source spec with CARML stream source
        // as we will inject the actual source input using a stream
        replaceRMLSourceFileReferencesWithInputStream(mappingModel);

        return RmlMappingLoader.build().load(mappingModel);
    }

    /**
     * Replace concrete file name with a CARML stream input source.
     * 
     * <p>
     * This method searches for this pattern:
     * <p>
     * <pre>
     * rml:logicalSource [ 
     *   rml:source "/path/to/sample_publication.json";
     *   rml:referenceFormulation ql:JSONPath;
     *   rml:iterator "$."
     * ];
     * </pre>
     * 
     * <p>
     * and replaces it with a CARML stream input source:
     * </p>
     * <pre>
     * rml:logicalSource [ 
     *   rml:source [ a carml:Stream ];
     *   rml:referenceFormulation ql:JSONPath;
     *   rml:iterator "$."
     * ];
     * </pre>
     */
    private void replaceRMLSourceFileReferencesWithInputStream(Model mappings) {
        Model sources = mappings.filter(null, RML.SOURCE, null);
        Model sourcesReplacements = new TreeModel();
        sources.forEach(stmt -> {
            Resource source = Values.bnode();
            sourcesReplacements.add(Statements.statement(stmt.getSubject(), RML.SOURCE, source, stmt.getContext()));
            sourcesReplacements.add(Statements.statement(source, RDF.TYPE, CARML.STREAM, stmt.getContext()));
        });
        mappings.remove(null, RML.SOURCE, null);
        mappings.addAll(sourcesReplacements);
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

    static class RML {
        public final static IRI SOURCE = Values.iri(Rml.source);
    }

    static class CARML {
        public final static IRI STREAM = Values.iri(Carml.Stream);
    }

    static class StringInputStream extends ByteArrayInputStream {
        public StringInputStream(String s) {
            super(s.getBytes(StandardCharsets.UTF_8));
        }
    }
}
