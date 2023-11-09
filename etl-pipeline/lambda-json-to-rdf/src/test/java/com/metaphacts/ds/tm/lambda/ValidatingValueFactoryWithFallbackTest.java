package com.metaphacts.ds.tm.lambda;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Test;

class ValidatingValueFactoryWithFallbackTest {

    @Test
    void testValidationAndFallback() {
        ValidatingValueFactoryWithFallback vf = new ValidatingValueFactoryWithFallback();

        Value l;

        // string value
        l = vf.createLiteral("1234");
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals(XSD.STRING, ((Literal) l).getDatatype(), "value should be a string literal");

        // int value
        l = vf.createLiteral("1234", XSD.INT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals(XSD.INT, ((Literal) l).getDatatype(), "value should be an int literal");

        // invalid int value
        l = vf.createLiteral("1234abcd", XSD.INT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals(XSD.STRING, ((Literal) l).getDatatype(), "value should be a string literal");

        // invalid int value
        l = vf.createLiteral("1234abcd", CoreDatatype.XSD.INT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals(XSD.STRING, ((Literal) l).getDatatype(), "value should be a string literal");

        // invalid int value
        l = vf.createLiteral("1234abcd", XSD.INT, CoreDatatype.XSD.INT);
        assertTrue(l.isLiteral(), "value should be a string literal");
        assertEquals(XSD.STRING, ((Literal) l).getDatatype(), "value should be a string literal");

        // dateTime value
        l = vf.createLiteral("2023-05-15T08:09:10", XSD.DATETIME);
        assertTrue(l.isLiteral(), "value should be a dateTime literal");
        assertEquals(XSD.DATETIME, ((Literal) l).getDatatype(), "value should be a dateTime literal");

        // invalid int value
        l = vf.createLiteral("May 15th 2023 08:09:10", XSD.DATETIME);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals(XSD.STRING, ((Literal) l).getDatatype(), "value should be a string literal");

        // invalid int value
        l = vf.createLiteral("May 15th 2023 08:09:10", CoreDatatype.XSD.DATETIME);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals(XSD.STRING, ((Literal) l).getDatatype(), "value should be a string literal");

        // invalid int value
        l = vf.createLiteral("May 15th 2023 08:09:10", XSD.DATETIME, CoreDatatype.XSD.DATETIME);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals(XSD.STRING, ((Literal) l).getDatatype(), "value should be a string literal");

    }
}
