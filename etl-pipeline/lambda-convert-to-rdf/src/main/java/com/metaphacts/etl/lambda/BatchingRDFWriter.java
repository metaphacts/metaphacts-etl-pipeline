/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;

/**
 * RDFWriter which collects a number of {@link Statement}s and writes them in
 * one go.
 */
public class BatchingRDFWriter extends DelegatingRDFWriter {
    List<Statement> statements = new ArrayList<>();
    private int batchSize;

    public BatchingRDFWriter(RDFWriter delegate, int batchSize) {
        super(delegate);
        this.batchSize = batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        // store statement in buffer
        statements.add(statement);
        if (statements.size() >= batchSize) {
            // batch size has been reached or exceeded, flush all statements
            flushStatements();
        }
    }

    public void flushStatements() {
        // write all pending statements
        for (Statement statement : statements) {
            super.handleStatement(statement);
        }
        // clear buffer
        statements.clear();
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        statements.clear();
        super.startRDF();
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        // write remaining items
        flushStatements();

        super.endRDF();
    }

}
