package com.metaphacts.ds.tm.lambda;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;

public class FixedContextRDFWriterWrapper extends DelegatingRDFWriter implements FixedContextRDFWriter {
    private FixedContextStatementWrapper statementWrapper;
    private boolean buffering = false;
    private final Resource fixedContext;

    public static FixedContextRDFWriterWrapper fixedContextFor(RDFWriter writer, Resource context) {
        return new FixedContextRDFWriterWrapper(writer, context);
    }

    public FixedContextRDFWriterWrapper(RDFWriter delegate, Resource context) {
        super(delegate);
        this.statementWrapper = new FixedContextStatementWrapper(context);
        this.fixedContext = context;
    }

    public void setFixedContext(Resource context) {
        if (context != null) {
            this.statementWrapper = new FixedContextStatementWrapper(context);
        } else {
            this.statementWrapper = null;
        }
    }

    public void startRDF() throws RDFHandlerException {
        super.startRDF();

        // some writers like the TurtleWriter buffer statements, so we cannot reuse the
        // statement object but need to wrap each individual object
        boolean prettyPrint = getWriterConfig().get(BasicWriterSettings.PRETTY_PRINT);
        boolean inlineBNodes = getWriterConfig().get(BasicWriterSettings.INLINE_BLANK_NODES);
        if (prettyPrint || inlineBNodes) {
            buffering = true;
        }
    }

    public void handleStatement(Statement st) throws RDFHandlerException {
        if (statementWrapper != null) {
            FixedContextStatementWrapper wrapped = statementWrapper;
            if (buffering) {
                // in most cases, we only ever handle a single statement at a time
                // so we can use a single object
                // when buffering this assumption does not hold, so we create one
                // wrapper for each statement
                wrapped = new FixedContextStatementWrapper(this.fixedContext);
            }
            wrapped.setDelegate(st);
            super.handleStatement(wrapped);
        } else {
            super.handleStatement(st);
        }
    }

    static class FixedContextStatementWrapper implements Statement {
        private static final long serialVersionUID = 1L;
        private final Resource fixedContext;
        private Statement delegate;

        public FixedContextStatementWrapper(Resource context) {
            this.fixedContext = context;
        }

        public Statement getDelegate() {
            return delegate;
        }

        public void setDelegate(Statement delegate) {
            this.delegate = delegate;
        }

        public Resource getSubject() {
            return delegate.getSubject();
        }

        public IRI getPredicate() {
            return delegate.getPredicate();
        }

        public Value getObject() {
            return delegate.getObject();
        }

        public Resource getContext() {
            Resource context = delegate.getContext();
            if (context != null) {
                // use original named graph
                return context;
            }
            // context is overridden!
            return this.fixedContext;
        }

        public boolean equals(Object other) {
            return delegate.equals(other);
        }

        public int hashCode() {
            return delegate.hashCode();
        }
    }

}
