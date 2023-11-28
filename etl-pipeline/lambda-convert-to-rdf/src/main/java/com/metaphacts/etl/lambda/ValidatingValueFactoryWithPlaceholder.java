package com.metaphacts.etl.lambda;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.XSD;

/**
 * {@link ValidatingValueFactory} which returns placeholder values for invalid
 * values.
 * 
 * <p>
 * Only literals with a datatype are validated and possibly changed. Other
 * values (e.g. IRIs or BNodes) will not be modified.
 * </p>
 * 
 * <p>
 * Example: a literal of {@code "123abc"^^xsd:integer} will be changed to
 * {@code "-1"^^xsd:integer}.
 * </p>
 * 
 * @author Wolfgang Schell <ws@metaphacts.com>
 *
 */
public class ValidatingValueFactoryWithPlaceholder extends LoggingValidatingValueFactory {

    public ValidatingValueFactoryWithPlaceholder() {
    }

    public Literal createLiteral(String label, IRI datatype) {
        try {
            return super.createLiteral(label, datatype);
        } catch (RuntimeException e) {
            warn("Invalid value for datatype " + datatype + ": '" + label + "', returning placeholder instead");
            // create placeholder value for the specified datatype
            return createPlaceholder(label, datatype, null, e);
        }
    }

    public Literal createLiteral(String label, CoreDatatype datatype) {
        try {
            return super.createLiteral(label, datatype);
        } catch (RuntimeException e) {
            warn("Invalid value for datatype " + datatype + ": '" + label + "', returning placeholder instead");
            // create placeholder value for the specified datatype
            return createPlaceholder(label, null, datatype, e);
        }
    }

    public Literal createLiteral(String label, IRI datatype, CoreDatatype coreDatatype) {
        try {
            return super.createLiteral(label, datatype, coreDatatype);
        } catch (RuntimeException e) {
            warn("Invalid value for datatype " + datatype + ": '" + label + "', returning placeholder instead");
            // create placeholder value for the specified datatype
            return createPlaceholder(label, datatype, coreDatatype, e);
        }
    }

    protected Literal createPlaceholder(String label, IRI datatype, CoreDatatype coreDatatype, Exception originalError) {
        if ((datatype == null) && (coreDatatype != null)) {
            datatype = coreDatatype.getIri();
        }
        if (datatype == null) {
            throw new IllegalArgumentException("neither a datatype IRI nor CoreDatatype provided!");
        }
        if (XSD.STRING.equals(datatype)) {
            // should never reach here, as any string
            // would be accepted by the parent implementation
            return Values.literal(label);
        } else if (XSD.INT.equals(datatype)
            || XSD.INTEGER.equals(datatype)
            || XSD.SHORT.equals(datatype)
            || XSD.BYTE.equals(datatype)
            || XSD.LONG.equals(datatype)
            || XSD.POSITIVE_INTEGER.equals(datatype)
            || XSD.NON_POSITIVE_INTEGER.equals(datatype)
            || XSD.NEGATIVE_INTEGER.equals(datatype)
            || XSD.NON_NEGATIVE_INTEGER.equals(datatype)
            || XSD.UNSIGNED_SHORT.equals(datatype)
            || XSD.UNSIGNED_BYTE.equals(datatype)
            || XSD.UNSIGNED_INT.equals(datatype)
            || XSD.UNSIGNED_LONG.equals(datatype)
            || XSD.DECIMAL.equals(datatype)) {
            return Values.literal("0", datatype);
        }
        else if (XSD.FLOAT.equals(datatype)
                || XSD.DOUBLE.equals(datatype)) {
            return Values.literal("NaN", datatype);
        } else if (XSD.BOOLEAN.equals(datatype)) {
            return Values.literal(false);
        } else if (XSD.DATETIME.equals(datatype)
                || XSD.DATETIMESTAMP.equals(datatype)) {
            return Values.literal("1970-01-01T00:00:00Z", datatype);
        } else if (XSD.DATE.equals(datatype)) {
            return Values.literal("1970-01-01", datatype);
        } else if (XSD.DURATION.equals(datatype)) {
            return Values.literal("P0Y", datatype);
        }
        throw new IllegalArgumentException(
                "Invalid value '" + label + "' for datatype '" + datatype.stringValue()
                        + "': no replacement value available. Original error: " + originalError.getMessage(),
                originalError);
    }
}

