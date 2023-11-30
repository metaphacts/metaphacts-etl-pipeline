/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.util.Statements;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MappingManager {
    private static final Logger logger = LoggerFactory.getLogger(MappingManager.class);

    private static final String BASE_URI = "http://base.metaphacts.com/";
    private static final IRI BASE_IRI = Values.iri(BASE_URI);

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private final Map<String, Mapping> mappings = new TreeMap<>();

    private Model namespaces = new TreeModel();

    // see LiteralConversionMode for allowed values
    @ConfigProperty(name = "mappings.literalConversionMode", defaultValue = "validateAndFallbackToPlaceholder")
    String mappingsLiteralConversionMode;

    @Inject
    FileHelper fileHelper;
    @Inject
    LambdaLoggerManager lambdaLoggerManager;

    public MappingManager() {
    }

    @PostConstruct
    protected void init() throws IOException {
        // load namespace declarations to be used for pretty printing
        try (InputStream namespaceStream = getClass().getResourceAsStream("/namespaces.ttl")) {
            namespaces = Rio.parse(namespaceStream, RDFFormat.TURTLE);
        } catch (Exception e) {
            logger.warn("failed to load namespaces from {}: {}", e.getMessage());
            logger.debug("Details: ", e);
        }
    }

    /**
     * Get {@link Model} with all pre-defined namespaces.
     * 
     * @return {@link Model} with all pre-defined namespaces
     */
    public Model getNamespaces() {
        return namespaces;
    }

    public void prepareMappers(URI mappingConfigURI, Path inputDir) {
        logger.debug("Loading mapping config from path {} ...", mappingConfigURI);
        Optional<MappingConfig> mappingConfigHolder = getMappingConfig(mappingConfigURI);
        if (!mappingConfigHolder.isPresent()) {
            String message = String.format("Failed to load mapping config from path %s!", mappingConfigURI);
            logger.warn(message);
            lambdaLoggerManager.get().ifPresent(lambdaLogger -> lambdaLogger.log(message));
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

                createMapping(mappingConfigURI, spec, mappingModel, inputDir);
            } catch (Exception e) {
                logger.warn("failed to load mappings for {}: {}", spec.getId(), e.getMessage());
                logger.debug("Details: ", e);
            }
        }
    }

    private void createMapping(URI mappingConfigURI, MappingSpec spec, Model mappingModel, Path inputDir) {
        RdfRmlMapper mapper = prepareMapper(mappingModel, inputDir);
        Mapping mapping = new Mapping(spec, mappingModel, mapper);
        mappings.put(spec.getId().toLowerCase(), mapping);
    }

    public Optional<Mapping> getMappingFor(String fileName) {
        // find matching mapping
        for (Mapping mapping : mappings.values()) {
            if (mapping.matches(fileName)) {
                return Optional.of(mapping);
            }
        }
        return Optional.empty();
    }

    public Optional<MappingConfig> getMappingConfig(URI mappingConfigURI) {
        logger.info("Loading mappings from {}", mappingConfigURI);
        try {
            try (Reader reader = fileHelper.openInputReader(mappingConfigURI)) {
                MappingConfig mappingsConfig = gson.fromJson(reader, MappingConfig.class);
                return Optional.of(mappingsConfig);
            }
        } catch (Exception e) {
            logger.warn("Failed to load mappings from {}: {}!", mappingConfigURI, e.getMessage());
            logger.debug("Details: ", e);
        }

        return Optional.empty();
    }

    private RdfRmlMapper prepareMapper(Model mappingModel, Path inputDir) {
        var mapping = loadMapping(mappingModel);

        if (logger.isDebugEnabled()) {
            logger.debug("The following mapping constructs were detected:");
            logger.debug("{}{}", System.lineSeparator(), ModelSerializer.serializeAsRdf(mappingModel, RDFFormat.TURTLE,
                    ModelSerializer.SIMPLE_WRITER_CONFIG, n -> n));
        }

        var mapperBuilder = RdfRmlMapper.builder()
                .baseIri(BASE_IRI)
                // add mappings
                .triplesMaps(mapping).valueFactorySupplier(getValueFactory())
                .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
                .setLogicalSourceResolver(Rdf.Ql.XPath, XPathResolver::getInstance)
                .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance);

        Optional<Path> relativeSourceLocation = Optional.of(inputDir);
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
                    MappingManager.this.handleValueFactoryWarning(message);
                }
            };
        case validateAndFallbackToString:
            return () -> new ValidatingValueFactoryWithFallback() {
                protected void warn(String message) {
                    super.warn(message);
                    MappingManager.this.handleValueFactoryWarning(message);
                }
            };
        case validateAndFallbackToPlaceholder:
            return () -> new ValidatingValueFactoryWithPlaceholder() {
                protected void warn(String message) {
                    super.warn(message);
                    MappingManager.this.handleValueFactoryWarning(message);
                }
            };
        }
    }

    protected void handleValueFactoryWarning(String message) {
        try {
            lambdaLoggerManager.get().ifPresent(lambdaLogger -> lambdaLogger.log(message));
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
     * Load {@link Model} from provided url.
     *
     * @param uri the {@link URL} from which to load RDF data
     * @return the {@link Model}.
     */
    private Model loadModel(URI uri) {
        return parseRDFToStream(uri).collect(new ModelCollector());
    }

    private Stream<Statement> parseRDFToStream(URI uri) {
        return parseRDFToModel(uri).map(m -> m.stream()).orElse(Stream.empty());
    }

    private Optional<Model> parseRDFToModel(URI uri) {
        var fileName = uri.getPath();

        Optional<RDFFormat> rdfFormat = Rio.getParserFormatForFileName(fileName);
        if (!rdfFormat.isPresent()) {
            logger.debug("Skipping file {}: not recognized as RDF file", fileName);
            return Optional.empty();
        }
        try (Reader is = fileHelper.openInputReader(uri)) {
            logger.debug("Loading file {}", uri);
            return Optional.of(Rio.parse(is, BASE_URI, rdfFormat.get()));
        } catch (IOException | RDFParseException exception) {
            throw new RuntimeException(String.format("Exception occurred while parsing %s", uri), exception);
        }
    }

    /**
     * Replace concrete file name with a CARML stream input source.
     * 
     * <p>
     * This method searches for this pattern:
     * <p>
     * 
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
     * 
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

    public static class RML {
        public final static IRI SOURCE = Values.iri(Rml.source);
    }

    public static class CARML {
        public final static IRI STREAM = Values.iri(Carml.Stream);
    }

    public enum LiteralConversionMode {
        noValidation, validateAndFail, validateAndFallbackToString, validateAndFallbackToPlaceholder;
    }
}
