/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Holder for a list of {@link MappingSpec}s.
 * 
 * @author Wolfgang Schell <ws@metaphacts.com>
 */
public class MappingConfig {
    private List<MappingSpec> mappings;

    public MappingConfig() {
        this.mappings = new ArrayList<>();
    }

    public MappingConfig(MappingSpec... mappings) {
        this(Arrays.asList(mappings));
    }

    public MappingConfig(List<MappingSpec> mappings) {
        this.mappings = new ArrayList<>(mappings);
    }

    public void setMappings(List<MappingSpec> mappings) {
        this.mappings = new ArrayList<>(mappings);
    }

    public List<MappingSpec> getMappings() {
        return mappings;
    }

    public void addMapping(MappingSpec mapping) {
        if (this.mappings == null) {
            this.mappings = new ArrayList<>();
        }
        this.mappings.add(mapping);
    }

    public void addMappings(List<MappingSpec> mappings) {
        if (this.mappings == null) {
            this.mappings = new ArrayList<>();
        }
        this.mappings.addAll(mappings);
    }

    public void addMappings(MappingSpec... mappings) {
        if (this.mappings == null) {
            this.mappings = new ArrayList<>();
        }
        this.mappings.addAll(Arrays.asList(mappings));
    }

    @Override
    public String toString() {
        return String.join(", ", mappings.stream().map(Object::toString).toList());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((mappings == null) ? 0 : mappings.hashCode());
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
        MappingConfig other = (MappingConfig) obj;
        if (mappings == null) {
            if (other.mappings != null)
                return false;
        } else if (!mappings.equals(other.mappings))
            return false;
        return true;
    }

}
