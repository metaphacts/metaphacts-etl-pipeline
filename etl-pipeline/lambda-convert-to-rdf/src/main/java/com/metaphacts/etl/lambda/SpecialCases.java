/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.util.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * This class implements some special cases for pre-processing, change
 * detection, etc. that are relevant for certain use cases. They are highly
 * domain-specific, so they can be enabled via
 * {@link MappingSpec#hasProcessingHint(String)}.
 */
@ApplicationScoped
public class SpecialCases {
    private static final Logger logger = LoggerFactory.getLogger(SpecialCases.class);
    
    static final IRI STATUS_IRI = Values.iri("urn:recordStatus");
    static final IRI ID_IRI = Values.iri("urn:recordId");

    static final String SUFFIX_DELETE = "_delete.txt.gz";

    @ConfigProperty(name = "upload.bucket", defaultValue = "output-bucket")
    String uploadBucket;
    @ConfigProperty(name = "upload.delete", defaultValue = "true")
    Boolean uploadDelete;

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
    @ConfigProperty(name = "process.coldstart", defaultValue = "false")
    Boolean isColdStart;

    Pattern redirectsPattern = null;
    JedisPool jedisPool;

    @Inject
    S3Client s3;
    @Inject
    FileHelper fileHelper;


    public SpecialCases() {
    }

    @PostConstruct
    protected void init() throws IOException {
        if (skipRedirectsEnabled) {
            if (skipRedirectsPattern.isBlank()) {
                logger.warn("Skipping redirects is enabled but no pattern provided! Disabling skipping of lines");
            } else {
                logger.debug("Skipping redirects for line matching the regular exporession {}", skipRedirectsPattern);
                redirectsPattern = Pattern.compile(skipRedirectsPattern);
            }
        } else {
            redirectsPattern = null;
        }

        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(poolConfig, redisServer, redisPort, 1800, redisPassword);
    }

    public String preprocessLine(String line, Model out) {
        // TODO refactor as processor
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

        // TODO refactor as processor root-to-list
        if (listPreprocessingEnabled) {
            // wrap as object with a list element
            line = "{\"list\":[" + line + "]}";
        }

        if ((indexPreprocessingEnabled || parentPreprocessingEnabled || listPreprocessingEnabled)
                && logPreprocessedEnabled) {
            // log preprocessed line
            logger.debug("prepocessed line:\n{}", line);
        }
        return line;
    }

    // TODO refactor as processor "json-hierarchy"
    private void materializeContextInfo(JsonElement jsonNodeToProcess) {
        List<String> FIELDS_TO_MATERIALIZE = Arrays.asList("id", "name", "domain", "ocid");
        String PARENT_PREFIX = "__parent_";
        String INDEX_FIELD = "__index";
        String PARENT_KEY = "__parentKey";

        Map<String, JsonElement> fieldValuesToMaterialize = new HashMap<>();
        for (Map.Entry<String, JsonElement> attributeValue : jsonNodeToProcess.getAsJsonObject().entrySet()) {
            if (attributeValue.getKey().startsWith("__") || FIELDS_TO_MATERIALIZE.contains(attributeValue.getKey())) {
                fieldValuesToMaterialize.put(PARENT_PREFIX + attributeValue.getKey(),
                        jsonNodeToProcess.getAsJsonObject().get(attributeValue.getKey()));
            }
        }
        for (Map.Entry<String, JsonElement> attributeValue : jsonNodeToProcess.getAsJsonObject().entrySet()) {
            String parentKey = attributeValue.getKey();
            if (parentPreprocessingEnabled) {
                if (attributeValue.getValue().isJsonObject()) {
                    attributeValue.getValue().getAsJsonObject().addProperty(PARENT_KEY, parentKey);
                    for (Map.Entry<String, JsonElement> materializedFieldValue : fieldValuesToMaterialize.entrySet()) {
                        attributeValue.getValue().getAsJsonObject().add(materializedFieldValue.getKey(),
                                materializedFieldValue.getValue());
                    }
                    materializeContextInfo(attributeValue.getValue());
                }
            }
            if (attributeValue.getValue().isJsonArray()) {
                List<JsonElement> arrayElements = Lists
                        .newArrayList(attributeValue.getValue().getAsJsonArray().iterator());
                for (int index = 0; index < arrayElements.size(); index++) {
                    if (arrayElements.get(index).isJsonObject()) {
                        if (parentPreprocessingEnabled) {
                            for (Map.Entry<String, JsonElement> materializedFieldValue : fieldValuesToMaterialize
                                    .entrySet()) {
                                arrayElements.get(index).getAsJsonObject().add(materializedFieldValue.getKey(),
                                        materializedFieldValue.getValue());
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

    public boolean processLine(TaskContext tctx, Mapping mapping, String line) {
        // TODO refactor as processor "deletion-detection"
        if (skipRedirectsEnabled && (redirectsPattern != null)
                && redirectsPattern.matcher(line).matches()) {
            logger.trace("Skipping line because it contains redirects : {}", line);
            return false;
        }
        
        return true;
    }

    public boolean saveProcessTriples(TaskContext tctx, Mapping mapping, Path sourceFile, Model model,
            PrintWriter outDelete) {
        // TODO refactor as processor: "last-update"
        if (!mapping.getMappingSpec().hasProcessingHint("last-update")) {
            return true;
        }

        return handleLastUpdate(tctx, mapping, sourceFile, model, outDelete);
    }

    protected boolean handleLastUpdate(TaskContext tctx, Mapping mapping, Path sourceFile, Model model,
            PrintWriter outDelete) {

        boolean addTriplesToOutput = true;
        var docid = Models.objectString(model.getStatements(null, ID_IRI, null)).orElse("-none-");

        String version = tctx.getTask().getS3Key();
        String lookupKey = uploadBucket + mapping.getMappingSpec().getDatasetIri() + mapping.getType() + docid;
        String lookupKeyHash = DigestUtils.sha256Hex(lookupKey);
        if (onlyDetectLastUpdate) {
            addTriplesToOutput = false;
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.eval(
                        "local newValue = ARGV[2]; local currentValue=redis.call( 'GET' , ARGV[1] ); local result = '' ; if (not currentValue) then result=newValue else if currentValue>newValue then result=currentValue else result=newValue end  end ; redis.call( 'SET' , ARGV[1], result);",
                        0, lookupKeyHash, version);
            }
        } else {
            if (isColdStart) {
                // Choose only last version in cold starts
                try (Jedis jedis = jedisPool.getResource()) {
                    addTriplesToOutput = jedis.get(lookupKeyHash).equals(version);
                }
            }
            // TODO refactor as processor "deletion-detection"
            if (addTriplesToOutput) {
                String status = Models.objectString(model.filter(null, STATUS_IRI, null)).orElse("");
                if (!isColdStart) {
                    Optional<IRI> entityIRI = Models.subjectIRI(model.filter(null, STATUS_IRI, null));
                    entityIRI.ifPresent(iri -> {
                        // Save list of all affected IRIs (only incremental updates)
                        outDelete.println(iri.stringValue());
                    });
                }

                addTriplesToOutput = !"obsolete".equalsIgnoreCase(status);
                // Remove status triples because they are not needed anymore.
                model.remove(null, STATUS_IRI, null);
                model.remove(null, ID_IRI, null);
            }
        }
        return addTriplesToOutput;
    }

    public boolean performMapping(String line) {
        // do not bother performing RML mapping when we only need to detect the
        // timestamp of the last update
        return !onlyDetectLastUpdate;
    }

    public boolean saveResults(TaskContext tctx, Mapping mapping) {
        // do not bother savong the results when we only need to detect the timestamp of
        // the last update
        return !onlyDetectLastUpdate;
    }

    public Optional<String> onUploadFile(TaskContext tctx, Mapping mapping, Path sourceFile, Path localPathDelete,
            Path outputPath) {
        if (!isColdStart) {
            String key = outputPath.toString();
            String keyDelete = key + SUFFIX_DELETE;
            // upload delete file to S3
            try {
                // Upload delete files only for incremental updates
                fileHelper.uploadToS3(uploadBucket, keyDelete, localPathDelete);
                return Optional.of(keyDelete);
            } catch (Exception e) {
                logger.warn("Failed to upload file {}/{}: {}", uploadBucket, keyDelete, e.getMessage());
                logger.debug("Details: ", e);
            }
        }
        return Optional.empty();
    }

}
