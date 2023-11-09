package com.metaphacts.ds.tm.lambda;

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
    private boolean processLines = false;

    public Mapping(String type, Model mappingRules, RdfRmlMapper mapper) {
        this.type = type;
        this.mappingRules = mappingRules;
        this.mapper = mapper;
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
