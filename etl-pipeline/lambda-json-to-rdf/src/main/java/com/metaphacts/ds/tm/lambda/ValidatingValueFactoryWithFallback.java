package com.metaphacts.ds.tm.lambda;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;

/**
 * A validating ValueFactory which returns a simple string literal when a typed
 * literal has an invalid value for that type.
 * 
 * <p>
 * Only literals with a datatype are validated and possibly changed. Other
 * values (e.g. IRIs or BNodes) will not be modified.
 * </p>
 * 
 * <p>
 * Example: a literal of {@code "123abc"^^xsd:integer} will be changed to
 * {@code "123abc"^^xsd:string}.
 * </p>
 * 
 * @author Wolfgang Schell <ws@metaphacts.com>
 */
public class ValidatingValueFactoryWithFallback extends LoggingValidatingValueFactory {
    public ValidatingValueFactoryWithFallback() {
    }

    public Literal createLiteral(String label, IRI datatype) {
        try {
            return super.createLiteral(label, datatype);
        } catch (RuntimeException e) {
            warn("Invalid value for datatype " + datatype + ": '" + label + "', returning plain string instead");
            // ignore datatype, return plain string
            return createLiteral(label);
        }
    }

    public Literal createLiteral(String label, CoreDatatype datatype) {
        try {
            return super.createLiteral(label, datatype);
        } catch (RuntimeException e) {
            warn("Invalid value for datatype " + datatype + ": '" + label + "', returning plain string instead");
            // ignore datatype, return plain string
            return createLiteral(label);
        }
    }

    public Literal createLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
        try {
            return super.createLiteral(label, datatype, coreDatatype);
        } catch (RuntimeException e) {
            warn("Invalid value for datatype " + datatype + ": '" + label + "', returning plain string instead");
            // ignore datatype, return plain string
            return createLiteral(label);
        }
    }
}

