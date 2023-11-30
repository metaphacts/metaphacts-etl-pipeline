/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Configuration for one type of mappings.
 * 
 * <p>
 * As this configuration is supposed to be read from/written to JSON, all
 * attributes should use types that can easily be converted from/to JSON.
 * </p>
 * 
 * @author Wolfgang Schell <ws@metaphacts.com>
 */
public class MappingSpec {
    /**
     * id for this mapping config (required).
     */
    public String id;

    /**
     * description for this mapping config (optional).
     */
    public String description;

    /**
     * List of mapping files to load (required).
     * 
     * <p>
     * Mapping files are resolved relative to the path or URL of the mappings config
     * file.
     * </p>
     * 
     * <p>
     * When specifying more than one file, the results are merged into a single
     * model.
     * </p>
     */
    public List<String> mappingFiles;

    /**
     * IRI (without enclosing &lt;&gt;) of the dataset the result data belongs to
     * (optional).
     * 
     * <p>
     * If provided, a link will be added from the named graph to the dataset.
     * </p>
     */
    public String datasetIri;

    /**
     * IRI (without enclosing &lt;&gt;) of the default named graph to use for
     * statements which have no explicit named graph set (optional).
     */
    public String namedGraphIri;

    /**
     * Regular expression to match against files (required).
     * 
     * <p>
     * All files matching this expression will also be matched against
     * {@link #sourceFileExcludePattern} if provided.
     * </p>
     * 
     * <p>
     * Example pattern: {@code publications/.*-records/records_.*\.jsonl}
     * </p>
     * 
     * @see Pattern
     */
    public String sourceFileIncludePattern;

    /**
     * Regular expression to match against files to skip (optional).
     * 
     * <p>
     * If <code>null</code>, no files are excluded.
     * </p>
     * 
     * <p>
     * Example pattern: {@code .*-summary\.jsonl}
     * </p>
     * 
     * @see Pattern
     */
    public String sourceFileExcludePattern;
    
    /**
     * List of additional processing hints (optional).
     * 
     * <p>
     * Processing hints may be interpreted by the conversion process.
     * </p>
     */
    public List<String> processingHints;

    public MappingSpec() {
    }
    
    public MappingSpec(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public MappingSpec withId(String id) {
        setId(id);
        return this;
    }

    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public MappingSpec withDescription(String description) {
        setDescription(description);
        return this;
    }

    public List<String> getMappingFiles() {
        return mappingFiles;
    }

    public void setMappingFiles(List<String> mappingFiles) {
        this.mappingFiles = new ArrayList<>(mappingFiles);
    }

    public MappingSpec withMappingFiles(List<String> mappingFiles) {
        setMappingFiles(mappingFiles);
        return this;
    }

    public MappingSpec withMappingFiles(String... mappingFiles) {
        setMappingFiles(Arrays.asList(mappingFiles));
        return this;
    }

    public String getDatasetIri() {
        return datasetIri;
    }

    public void setDatasetIri(String datasetIri) {
        this.datasetIri = datasetIri;
    }
    
    public MappingSpec withDatasetIri(String datasetIri) {
        setDatasetIri(datasetIri);
        return this;
    }

    public String getSourceFileIncludePattern() {
        return sourceFileIncludePattern;
    }

    public void setSourceFileIncludePattern(String sourceFileIncludePattern) {
        this.sourceFileIncludePattern = sourceFileIncludePattern;
    }

    public MappingSpec withSourceFileIncludePattern(String sourceFileIncludePattern) {
        setSourceFileIncludePattern(sourceFileIncludePattern);
        return this;
    }

    public String getSourceFileExcludePattern() {
        return sourceFileExcludePattern;
    }

    public void setSourceFileExcludePattern(String sourceFileExcludePattern) {
        this.sourceFileExcludePattern = sourceFileExcludePattern;
    }

    public MappingSpec withSourceFileExcludePattern(String sourceFileExcludePattern) {
        setSourceFileExcludePattern(sourceFileExcludePattern);
        return this;
    }

    public List<String> getProcessingHints() {
        return processingHints;
    }

    public void setProcessingHints(List<String> processingHints) {
        this.processingHints = new ArrayList<>(processingHints);
    }

    public MappingSpec withProcessingHints(String... processingHints) {
        setProcessingHints(Arrays.asList(processingHints));
        return this;
    }

    public MappingSpec withProcessingHints(List<String> processingHints) {
        setProcessingHints(processingHints);
        return this;
    }

    public boolean hasProcessingHint(String processingHint) {
        return processingHints.contains(processingHint);
    }

    @Override
    public String toString() {
        return getId() + ": " + getSourceFileIncludePattern();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MappingSpec other = (MappingSpec) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    
}
