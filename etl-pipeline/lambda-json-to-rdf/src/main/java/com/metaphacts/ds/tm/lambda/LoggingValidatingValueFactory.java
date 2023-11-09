package com.metaphacts.ds.tm.lambda;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ValidatingValueFactory} which logs issues.
 * 
 * @author Wolfgang Schell <ws@metaphacts.com>
 */
public class LoggingValidatingValueFactory extends ValidatingValueFactory {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public Literal createLiteral(String label, IRI datatype) {
        try {
            return super.createLiteral(label, datatype);
        } catch (RuntimeException e) {
            warn("Invalid value for datatype " + datatype + ": '" + label + "', error: " + e.getMessage());
            throw e;
        }
    }

    public Literal createLiteral(String label, CoreDatatype datatype) {
        try {
            return super.createLiteral(label, datatype);
        } catch (RuntimeException e) {
            warn("Invalid value for datatype " + datatype + ": '" + label + "', error: " + e.getMessage());
            throw e;
        }
    }

    public Literal createLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
        try {
            return super.createLiteral(label, datatype, coreDatatype);
        } catch (RuntimeException e) {
            warn("Invalid value for datatype " + datatype + ": '" + label + "', error: " + e.getMessage());
            throw e;
        }
    }

    protected void warn(String message) {
        logger.warn(message);
    }
}
