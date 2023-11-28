package com.metaphacts.etl.lambda;

import java.util.Collection;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RioSetting;
import org.eclipse.rdf4j.rio.WriterConfig;

public class DelegatingRDFWriter implements RDFWriter {
    private final RDFWriter delegate;

    public DelegatingRDFWriter(RDFWriter delegate) {
        this.delegate = delegate;
    }

    public RDFWriter getDelegate() {
        return delegate;
    }

    public RDFFormat getRDFFormat() {
        return delegate.getRDFFormat();
    }

    public void startRDF() throws RDFHandlerException {
        delegate.startRDF();
    }

    public RDFWriter setWriterConfig(WriterConfig config) {
        return delegate.setWriterConfig(config);
    }

    public void endRDF() throws RDFHandlerException {
        delegate.endRDF();
    }

    public WriterConfig getWriterConfig() {
        return delegate.getWriterConfig();
    }

    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
        delegate.handleNamespace(prefix, uri);
    }

    public Collection<RioSetting<?>> getSupportedSettings() {
        return delegate.getSupportedSettings();
    }

    public <T> RDFWriter set(RioSetting<T> setting, T value) {
        return delegate.set(setting, value);
    }

    public void handleStatement(Statement st) throws RDFHandlerException {
        delegate.handleStatement(st);
    }

    public void handleComment(String comment) throws RDFHandlerException {
        delegate.handleComment(comment);
    }
}
