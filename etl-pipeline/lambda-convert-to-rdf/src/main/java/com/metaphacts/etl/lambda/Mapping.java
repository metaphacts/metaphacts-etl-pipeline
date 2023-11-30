/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.util.regex.Pattern;

import org.eclipse.rdf4j.model.Model;

import io.carml.engine.rdf.RdfRmlMapper;

/**
 * Holder for a RDF mapper for a certain type.
 * 
 * @author Wolfgang Schell <ws@metaphacts.com>
 *
 */
public class Mapping {

    private final String type;
    private final Model mappingRules;
    private final RdfRmlMapper mapper;
    private MappingSpec mappingSpec;
    private Pattern includePattern;
    private Pattern excludePattern;
    private boolean processLines = false;

    public Mapping(MappingSpec spec, Model mappingRules, RdfRmlMapper mapper) {
        this.type = spec.getId();
        this.mappingRules = mappingRules;
        this.mappingSpec = spec;
        this.mapper = mapper;

        includePattern = parsePattern("include", spec.getSourceFileIncludePattern());
        excludePattern = parsePattern("exclude", spec.getSourceFileExcludePattern());
    }

    private Pattern parsePattern(String type, String regexPattern) {
        if (regexPattern == null) {
            return null;
        }
        try {
            return Pattern.compile(regexPattern);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("invalid %s pattern /%s/: %s", type, regexPattern, e.getMessage()), e);
        }
    }

    public MappingSpec getMappingSpec() {
        return mappingSpec;
    }

    public String getType() {
        return type;
    }

    public Model getMappingRules() {
        return mappingRules;
    }

    public RdfRmlMapper getMapper() {
        return mapper;
    }

    public void setProcessLines(boolean processLines) {
        this.processLines = processLines;
    }

    public boolean isProcessLines() {
        return processLines;
    }

    public boolean matches(String fileName) {
        if (includePattern != null) {
            if (!includePattern.matcher(fileName).matches()) {
                // include pattern is mandatory if specified
                return false;
            }
        }
        if (excludePattern != null) {
            if (excludePattern.matcher(fileName).matches()) {
                // exclude pattern must not match if specified
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return getType();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
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
        Mapping other = (Mapping) obj;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return true;
    }
}
