package com.metaphacts.etl.lambda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
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
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
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

@Named("json2rdf")
public class ConvertToRDFLambda implements RequestStreamHandler {

    public enum LiteralConversionMode {
        noValidation, validateAndFail, validateAndFallbackToString, validateAndFallbackToPlaceholder;
    }

    private static final String DEFAULT_DATASET = "default";
    private static final String EXTENSION_GZ = ".gz";
    private static final String EXTENSION_JSONL = ".jsonl";
    private static final String SUFFIX_DELETE = "_delete.txt.gz";

    private static final Logger logger = LoggerFactory.getLogger(ConvertToRDFLambda.class);

    private static final String BASE_URI = "http://base.metaphacts.com/";
    private static final IRI BASE_IRI = Values.iri(BASE_URI);
    private static final IRI STATUS_IRI = Values.iri("urn:recordStatus");
    private static final IRI ID_IRI = Values.iri("urn:recordId");
    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final ConversionStats listener = new ConversionStats();
    private final Map<String, Mapping> mappings = new HashMap<>();
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
    @ConfigProperty(name = "preprocessing.list.enabled", defaultValue = "true")
    Boolean listPreprocessingEnabled;
    @ConfigProperty(name = "preprocessing.parent.enabled", defaultValue = "true")
    Boolean parentPreprocessingEnabled;
    @ConfigProperty(name = "preprocessing.index.enabled", defaultValue = "true")
    Boolean indexPreprocessingEnabled;
    @ConfigProperty(name = "preprocessing.log.enabled", defaultValue = "false")
    Boolean logPreprocessedEnabled;
    @ConfigProperty(name = "preprocessing.skipRedirects.enabled", defaultValue = "true")
    Boolean skipRedirectsEnabled;
    @ConfigProperty(name = "preprocessing.skipRedirects.pattern", defaultValue = "")
    String skipRedirectsPattern;

    @ConfigProperty(name = "redis.server", defaultValue = "localhost")
    String redisServer;
    @ConfigProperty(name = "redis.port", defaultValue = "6379")
    Integer redisPort;
    @ConfigProperty(name = "redis.password", defaultValue = "password")
    String redisPassword;
    @ConfigProperty(name = "process.detect.lastupdate", defaultValue = "false")
    Boolean onlyDetectLastUpdate;
    @ConfigProperty(name = "process.coldstart", defaultValue = "true")
    Boolean isColdStart;

    Path resolvedInputDir;
    Path resolvedDownloadDir;
    Path resolvedMappingsDir;
    Path resolvedRdfOutputDir;
    RDFFormat resolvedRdfOutputFormat;

    Pattern redirectsPattern = null;
    JedisPool jedisPool;



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
        
        if (skipRedirectsEnabled) {
            if (skipRedirectsPattern.isBlank()) {
                logger.warn("Skipping redirects is enabled but no pattern provided! Disabling skipping of lines");
            }
            else {
                logger.debug("Skipping redirects for line matching the regular exporession {}", skipRedirectsPattern);
                redirectsPattern = Pattern.compile(skipRedirectsPattern);
            }
        }
        else {
            redirectsPattern = null;
        }

        // load namespace declarations to be used for pretty printing
        try (InputStream namespaceStream = getClass().getResourceAsStream("/namespaces.ttl")) {
            namespaces = Rio.parse(namespaceStream, RDFFormat.TURTLE);
        } catch (Exception e) {
            logger.warn("failed to load namespaces from {}: {}", e.getMessage());
            logger.debug("Details: ", e);
        }

        prepareMappers();
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(poolConfig, redisServer, redisPort, 1800 , redisPassword);
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
            Optional<Path> sourceFileHolder = downloadFile(context);
            if (sourceFileHolder.isPresent()) {
                Path sourceFile = sourceFileHolder.get();
                try {
                    lambdaLoggerTL.set(context.getLogger());
                    long statementCount = processFile(context, sourceFile);
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

    public long processFile(TaskContext tctx, Path sourceFile) throws IOException {
        logger.debug("Processing file {}", sourceFile.toString());

        Optional<String> typeHolder = getEntityTypeForSource(tctx, sourceFile);
        if (typeHolder.isEmpty()) {
            logger.info("skipping file {}: no recognized entity type", sourceFile);
            throw new IllegalArgumentException("no recognized entity type");
        }
        Optional<String> datasetHolder = getEntityDatasetForSource(tctx, sourceFile);
        if (datasetHolder.isEmpty()) {
            logger.info("skipping file {}: no recognized entity dataset", sourceFile);
            throw new IllegalArgumentException("no recognized entity dataset");
        }
        String dataset = datasetHolder.get();
        String type = typeHolder.get();
        Optional<Mapping> mappingHolder = getMappingFor(dataset, type);
        if (mappingHolder.isEmpty()) {
            logger.info("skipping file {}: no mapping found for type {}", sourceFile, type);
            throw new IllegalArgumentException("no mapping found for type " + type);
        }
        Mapping mapping = mappingHolder.get();
        RdfRmlMapper rmlMapper = mapping.getMapper();

        long aggregatedSize = 0;
        try (BufferedReader sourceReader = openInputFile(sourceFile)) {
            Path outputPath = resolveRDFFile(tctx, sourceFile, type);
            Path outputPathDelete = Paths.get(outputPath.toString() + SUFFIX_DELETE);
            // determine the target named graph for the source file
            Path inputFile = Path.of(tctx.getTask().getS3Key());
            String version = tctx.getTask().getS3Key();
            Resource targetContext = targetContextForSource(inputFile, type, Optional.empty());
            // Add dataset to Context for datasets different from default
            if (!dataset.equals(DEFAULT_DATASET)){
                targetContext = targetContextForSource(inputFile, type, Optional.of(dataset));
            }

            logger.debug("Saving statements from file {} to file {} with context {}", inputFile, outputPath,
                    targetContext.stringValue());
            // open CSV file for delete IRIs list
            try (PrintWriter outDelete = new PrintWriter(openOutputFile(outputPathDelete))) {  
                // open RDF file for output
                try (OutputStream out = openOutputFile(outputPath)) {
                    // create RDF writer and write pre-amble with namespace declarations
                    RDFWriter writer = openRDFFile(tctx, out, targetContext, namespaces);

                    // process file: iterate over each line in input file
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

                        if (skipRedirectsEnabled && (redirectsPattern != null)
                                && redirectsPattern.matcher(line).matches()) {
                            logger.trace("Skipping line because it contains redirects : {}", line);
                            // continue with next line
                            continue;
                        }
                        // process line
                        listener.startDocument();
                        boolean success = true;
                        Model model = null;
                        try {
                            model = processLine(tctx, sourceFile, rmlMapper, line);
                            var docid = model.getStatements(null, ID_IRI, null).iterator().next().getObject().stringValue();
                            String lookupKey = uploadBucket + dataset + type + docid;
                            String lookupKeyHash = DigestUtils.sha256Hex(lookupKey);
                            if (onlyDetectLastUpdate){
                                try (Jedis jedis = jedisPool.getResource()) {
                                    jedis.eval("local newValue = ARGV[2]; local currentValue=redis.call( 'GET' , ARGV[1] ); local result = '' ; if (not currentValue) then result=newValue else if currentValue>newValue then result=currentValue else result=newValue end  end ; redis.call( 'SET' , ARGV[1], result);", 0, lookupKeyHash, version);
                                }
                            } else {
                                var addTriplesToOutput = true;
                                if (isColdStart){
                                    //Choose only last version in cold starts
                                    try (Jedis jedis = jedisPool.getResource()) {
                                        addTriplesToOutput = jedis.get(lookupKeyHash).equals(version);
                                    }
                                }
                                if (addTriplesToOutput) {
                                    String entityIRI = model.filter(null, STATUS_IRI, null).subjects().iterator().next().stringValue();
                                    String status = model.filter(null, STATUS_IRI, null).objects().iterator().next().stringValue();
                                    if (!isColdStart){
                                        //Save list of all affected IRIs (only incremental updates)
                                        outDelete.println(entityIRI);
                                    }
                                    if (!status.equals("obsolete")){
                                        //Remove status triples because they are not needed anymore.
                                        model.remove(null, STATUS_IRI, null);
                                        model.remove(null, ID_IRI, null);
                                        aggregatedSize += writeRDF(writer, model); 
                                    }
                                }
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
            if (!onlyDetectLastUpdate){
                // upload to S3
                if (uploadFile(tctx, type, outputPath, outputPathDelete)) {
                    if (uploadDelete) {
                        deleteFile(outputPath);
                        deleteFile(outputPathDelete);
                    }
                }
            }

            return aggregatedSize;
        }
    }

    private Path resolveRDFFile(TaskContext tctx, Path sourceFile, String type) throws IOException {

        Path inputFile = Path.of(tctx.getTask().getS3Key());

        // define file name in output folder
        RDFFormat outputFormat = resolvedRdfOutputFormat;
        Path outputFile = outputFileForSource(inputFile, type, outputFormat);

        Path outputPath = resolvedRdfOutputDir.resolve(outputFile);
        outputPath = outputPath.toAbsolutePath();
        ensureFolderExists(outputPath.getParent());
        return outputPath;
    }

    private RDFWriter openRDFFile(TaskContext tctx, OutputStream outputStream, Resource targetContext,
            Model namespaces)
            throws IOException {
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

    private Resource targetContextForSource(Path sourceFile, String type, Optional<String> dataset) {
        IRI targetContext = Values.iri(contextBaseNamespace, type + "/");
        if (dataset.isPresent()){
            targetContext = Values.iri(contextBaseNamespace, dataset.get() + "/" + type + "/");
        }
        return targetContext;
    }

    private Optional<String> getEntityTypeForSource(TaskContext tctx, Path sourceFile) {
        Path inputFile = Path.of(tctx.getTask().getS3Key());
        // get 2nd path segment as entity type
        int n = 1;
        if (inputFile.getNameCount() > n) {
            return Optional.ofNullable(inputFile.getName(n).toString());
        }
        return Optional.empty();
    }

    private Optional<String> getEntityDatasetForSource(TaskContext tctx, Path sourceFile) {
        Path inputFile = Path.of(tctx.getTask().getS3Key());
        // get 1st path segment as entity dataset
        int n = 0;
        if (inputFile.getNameCount() > n) {
            return Optional.ofNullable(inputFile.getName(n).toString());
        }
        return Optional.empty();
    }

    private Optional<String> getEntityTypeForMapping(Path mappingFile) {
        if (mappingFile.getNameCount() > 0) {
            String fileName = mappingFile.getName(mappingFile.getNameCount() - 1).toString();
            return Optional.ofNullable(FilenameUtils.getBaseName(fileName));
        }
        return Optional.empty();
    }

    private Optional<String> getEntityDatasetForMapping(Path mappingFile) {
        if (mappingFile.getNameCount() > 0) {
            String fileName = mappingFile.getName(mappingFile.getNameCount() - 2).toString();
            return Optional.ofNullable(FilenameUtils.getBaseName(fileName));
        }
        return Optional.empty();
    }

    private Path outputFileForSource(Path sourcePath, String type, RDFFormat outputFormat) {
        String sourceFile = sourcePath.toString();
        if (sourceFile.toLowerCase().endsWith(EXTENSION_GZ)) {
            // strip .gz ending
            sourceFile = sourceFile.substring(0, sourceFile.length() - EXTENSION_GZ.length());
        }
        if (sourceFile.toLowerCase().endsWith(EXTENSION_JSONL)) {
            // strip .jsonl ending
            sourceFile = sourceFile.substring(0, sourceFile.length() - EXTENSION_JSONL.length());
        }

        // append RDF file format extension
        String outputFile = sourceFile + "." + outputFormat.getDefaultFileExtension();

        if (rdfOutputCompressed) {
            outputFile = outputFile + EXTENSION_GZ;
        }

        return Path.of(outputFile);
    }

    public void materializeContextInfo(JsonElement jsonNodeToProcess) {
        List<String> FIELDS_TO_MATERIALIZE = Arrays.asList("id","name","domain","ocid");
        String PARENT_PREFIX = "__parent_";
        String INDEX_FIELD = "__index";
        String PARENT_KEY = "__parentKey";
        
        Map<String, JsonElement> fieldValuesToMaterialize = new HashMap<>();
        for (Map.Entry<String, JsonElement> attributeValue : jsonNodeToProcess.getAsJsonObject().entrySet()) {
            if (attributeValue.getKey().startsWith("__") || FIELDS_TO_MATERIALIZE.contains(attributeValue.getKey())) {
                fieldValuesToMaterialize.put(PARENT_PREFIX + attributeValue.getKey(), jsonNodeToProcess.getAsJsonObject().get(attributeValue.getKey()));
            }
        }
        for (Map.Entry<String, JsonElement> attributeValue : jsonNodeToProcess.getAsJsonObject().entrySet()) {
            String parentKey = attributeValue.getKey();
            if (parentPreprocessingEnabled) {
                if (attributeValue.getValue().isJsonObject()) {
                    attributeValue.getValue().getAsJsonObject().addProperty(PARENT_KEY, parentKey);
                    for (Map.Entry<String, JsonElement> materializedFieldValue : fieldValuesToMaterialize.entrySet()) {
                        attributeValue.getValue().getAsJsonObject().add(materializedFieldValue.getKey(), materializedFieldValue.getValue());
                    }
                    materializeContextInfo(attributeValue.getValue());
                }
            }
            if (attributeValue.getValue().isJsonArray()) {
                List <JsonElement> arrayElements = Lists.newArrayList(attributeValue.getValue().getAsJsonArray().iterator());
                for (int index=0; index<arrayElements.size(); index++){
                    if (arrayElements.get(index).isJsonObject()){
                        if (parentPreprocessingEnabled) {
                            for (Map.Entry<String, JsonElement> materializedFieldValue : fieldValuesToMaterialize.entrySet()) {
                                arrayElements.get(index).getAsJsonObject().add(materializedFieldValue.getKey(), materializedFieldValue.getValue());
                            }
                            arrayElements.get(index).getAsJsonObject().addProperty(PARENT_KEY, parentKey);
                        }
                        if (indexPreprocessingEnabled) {
                            arrayElements.get(index).getAsJsonObject().addProperty(INDEX_FIELD, index);
                        }
                        materializeContextInfo(arrayElements.get(index));
                    }
                }
            }
        }
    }

    private Model processLine(TaskContext tctx, Path sourceFile, RdfRmlMapper rmlMapper, String line)
            throws IOException {
        Model out = new LinkedHashModel();

        if (indexPreprocessingEnabled || parentPreprocessingEnabled) {
            Gson gson = new Gson();
            JsonElement fromJson = gson.fromJson(line, JsonElement.class);
            materializeContextInfo(fromJson);
            var id = "";
            if (fromJson.getAsJsonObject().has("id")){
                id = fromJson.getAsJsonObject().get("id").getAsString();
            }
            if (fromJson.getAsJsonObject().has("ocid")){
                id = fromJson.getAsJsonObject().get("ocid").getAsString();
            }
            out.add(Values.bnode(), ID_IRI, Values.literal(id));
            // materialize again as string
            line = fromJson.toString();
        }

        if (listPreprocessingEnabled) {
            // wrap as object with a list element
            line = "{\"list\":[" + line + "]}";
        }

        if ((indexPreprocessingEnabled || parentPreprocessingEnabled || listPreprocessingEnabled)
                && logPreprocessedEnabled) {
            // log preprocessed line
            logger.debug("prepocessed line:\n{}", line);
        }

        if (!onlyDetectLastUpdate){
            // perform mapping for document
            try (StringInputStream input = new StringInputStream(line)) {
                Model model = rmlMapper.mapToModel(input);
                out.addAll(model);
            }
        }
        return out;
    }

    private BufferedReader openInputFile(Path sourceFile) throws IOException {
        InputStream sourceStream = new FileInputStream(sourceFile.toFile());
        String name = sourceFile.toString();
        if (name.toLowerCase().endsWith(EXTENSION_GZ)) {
            sourceStream = new GZIPInputStream(sourceStream);
        }
        BufferedReader sourceReader = new BufferedReader(new InputStreamReader(sourceStream, CHARSET_UTF8));
        return sourceReader;
    }

    private OutputStream openOutputFile(Path outputPath) throws IOException {
        OutputStream out = new FileOutputStream(outputPath.toFile());
            
        String name = outputPath.toString();
        if (name.toLowerCase().endsWith(EXTENSION_GZ)) {
            out = new GZIPOutputStream(out);
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
        S3Resource s3res = S3ArnConverter.create().convertArn(Arn.fromString(s3BucketArn));
        if (s3res instanceof S3BucketResource) {
            return ((S3BucketResource) s3res).bucketName();
        }
        // get last past part of ARN
        int pos = s3BucketArn.lastIndexOf(':');
        if (pos >= 0) {
            return s3BucketArn.substring(pos + 1);
        }
        return s3BucketArn;
    }

    private boolean uploadFile(TaskContext tctx, String type, Path localPath, Path localPathDelete) {
        if (!uploadEnabled) {
            // skip uploading
            logger.trace("Skipped uploading file {}: upload disabled", localPath);
            return false;
        }
        logger.debug("Uploading file {} to {}", localPath, uploadBucket);
        tctx.getLogger().log("Uploading file " + localPath + " to " + uploadBucket);

        Path inputFile = Path.of(tctx.getTask().getS3Key());

        // define file name in output folder
        RDFFormat outputFormat = resolvedRdfOutputFormat;
        Path outputFile = outputFileForSource(inputFile, type, outputFormat);
        String key = outputFile.toString();
        String keyDelete = key + SUFFIX_DELETE;
        
        // upload to S3
        try {
            PutObjectRequest putRequest = PutObjectRequest.builder().bucket(uploadBucket).key(key).build();
            PutObjectResponse putResponse = s3.putObject(putRequest, RequestBody.fromFile(localPath));
            logger.trace("Successfully uploaded file {}/{}: {}", uploadBucket, key, putResponse);

            if (!isColdStart){
                //Upload delete files only for incremental updates
                PutObjectRequest putRequestDelete = PutObjectRequest.builder().bucket(uploadBucket).key(keyDelete).build();
                PutObjectResponse putResponseDelete = s3.putObject(putRequestDelete, RequestBody.fromFile(localPathDelete));
                logger.trace("Successfully uploaded file {}/{}: {}", uploadBucket, keyDelete, putResponseDelete);
            }
            return true;
        } catch (Exception e) {
            logger.warn("Failed to upload file {}/{}/{}: {}", uploadBucket, key, keyDelete, e.getMessage());
            logger.debug("Details: ", e);
            return false;
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
     * Load {@link Model} from provided path.
     *
     * @param path the {@link Path} from which to load RDF data
     * @return the {@link Model}.
     */
    private Model loadModel(Path path) {
        return parseRDFToStream(path).collect(new ModelCollector());
    }

    private Stream<Statement> parseRDFToStream(Path path) {
        return parseRDFToModel(path)
                    .map(m -> m.stream())
                    .orElse(Stream.empty());
    }
    
    private Optional<Model> parseRDFToModel(Path path) {
        var fileName = path.getFileName().toString();

        Optional<RDFFormat> rdfFormat = Rio.getParserFormatForFileName(fileName);
        if (!rdfFormat.isPresent()) {
            logger.debug("Skipping file {}: not recognized as RDF file", fileName);
            return Optional.empty();
        }
        try (var is = Files.newInputStream(path)) {
            logger.debug("Loading file {}", fileName);
            return Optional.of(Rio.parse(is, BASE_URI, rdfFormat.get()));
        } catch (IOException | RDFParseException exception) {
            throw new RuntimeException(String.format("Exception occurred while parsing %s", path), exception);
        }
    }

    /**
     * Find all file {@link Path}s in the file tree starting from given
     * {@link Path}.
     *
     * @param paths the {@link List} of {@link Path}s to search through.
     * @return the {@link List} of file {@link Path}s.
     */
    private static List<Path> resolveFilePaths(List<Path> paths) {
        return paths.stream().flatMap(path -> {
            try (Stream<Path> walk = Files.walk(path)) {
                return walk.filter(Files::isRegularFile).collect(Collectors.toList()).stream();
            } catch (IOException exception) {
                throw new RuntimeException(String.format("Exception occurred while reading path %s", path), exception);
            }
        }).collect(Collectors.toList());
    }

    private void prepareMappers() {
        List<Path> paths = getMappingFiles();

        logger.debug("Loading mapping from paths {} ...", paths);

        for (Path mappingPath : paths) {
            Optional<RDFFormat> rdfFormat = Rio.getParserFormatForFileName(mappingPath.toString());
            if (!rdfFormat.isPresent()) {
                logger.debug("Skipping file {}: not recognized as RDF file", mappingPath);
                continue;
            }
            Optional<String> type = getEntityTypeForMapping(mappingPath);
            if (type.isEmpty()) {
                logger.warn("ignoring mapping file {}: no recognized entity type", mappingPath);
                continue;
            }
            String entityType = type.get();
            Optional<String> dataset = getEntityDatasetForMapping(mappingPath);
            if (dataset.isEmpty()) {
                logger.warn("ignoring mapping file {}: no recognized entity dataset", mappingPath);
                continue;
            }
            String entityDataset = dataset.get();
            try {
                Model mappingModel = loadModel(mappingPath);
                logger.debug("loading mapping for {}({}) from {}", entityType, entityDataset, mappingPath);
                createMapping(entityDataset, entityType, mappingModel);
            } catch (Exception e) {
                logger.warn("failed to load mapping from {}: {}", mappingPath, e.getMessage());
                logger.debug("Details: ", e);
            }

        }
    }

    private void createMapping(String dataset, String type, Model mappingModel) {
        RdfRmlMapper mapper = prepareMapper(mappingModel);
        Mapping mapping = new Mapping(type, mappingModel, mapper);
        mappings.put(dataset.toLowerCase() + '_' + type.toLowerCase(), mapping);
    }

    private Optional<Mapping> getMappingFor(String dataset, String type) {
        return Optional.ofNullable(mappings.get(dataset.toLowerCase() + '_' + type.toLowerCase()));
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

    /**
     * Recursively get all mappings within the configured mappings directory
     * 
     * @return list of mappings
     */
    private List<Path> getMappingFiles() {
        List<Path> paths = List.of(resolvedMappingsDir);
        return paths.stream()
                .flatMap(path -> resolveFilePaths(List.of(path)).stream())
                .collect(Collectors.toList());
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
