package com.metaphacts.etl.lambda;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.rio.RDFWriter;

public interface FixedContextRDFWriter extends RDFWriter {
    /**
     * Context to use for all statements.
     * 
     * @param context
     */
    void setFixedContext(Resource context);
}
