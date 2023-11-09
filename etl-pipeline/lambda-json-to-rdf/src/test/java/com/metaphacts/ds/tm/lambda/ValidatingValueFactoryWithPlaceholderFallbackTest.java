package com.metaphacts.ds.tm.lambda;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Test;

class ValidatingValueFactoryWithPlaceholderFallbackTest {

    @Test
    void testValidationAndFallback() {
        ValidatingValueFactoryWithPlaceholder vf = new ValidatingValueFactoryWithPlaceholder();

        Value l;

        // string value
        l = vf.createLiteral("1234");
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals(XSD.STRING, ((Literal) l).getDatatype(), "value should be a string literal");

        // int value
        l = vf.createLiteral("1234", XSD.INT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1234", l.stringValue());
        assertEquals(1234, ((Literal) l).intValue());
        assertEquals(XSD.INT, ((Literal) l).getDatatype(), "value should be an int literal");

        // invalid int value
        l = vf.createLiteral("1234abcd", XSD.INT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(0, ((Literal) l).intValue(), "the replacement value should be 0");
        assertEquals(XSD.INT, ((Literal) l).getDatatype(), "value should be a int literal");

        // invalid int value
        l = vf.createLiteral("1234abcd", CoreDatatype.XSD.INT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(0, ((Literal) l).intValue(), "the replacement value should be 0");
        assertEquals(XSD.INT, ((Literal) l).getDatatype(), "value should be a int literal");

        // invalid int value
        l = vf.createLiteral("1234abcd", XSD.INT, CoreDatatype.XSD.INT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(0, ((Literal) l).intValue(), "the replacement value should be 0");
        assertEquals(XSD.INT, ((Literal) l).getDatatype(), "value should be a int literal");

        // unsigned byte value
        l = vf.createLiteral("123", XSD.UNSIGNED_BYTE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("123", l.stringValue());
        assertEquals(123, ((Literal) l).byteValue());
        assertEquals(XSD.UNSIGNED_BYTE, ((Literal) l).getDatatype(), "value should be an unsigned byte literal");

        // invalid unsigned byte value
        l = vf.createLiteral("123abcd", XSD.UNSIGNED_BYTE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(0, ((Literal) l).byteValue(), "the replacement value should be 0");
        assertEquals(XSD.UNSIGNED_BYTE, ((Literal) l).getDatatype(), "value should be a unsigned byte literal");

        // invalid unsigned byte value
        l = vf.createLiteral("123abcd", CoreDatatype.XSD.UNSIGNED_BYTE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(0, ((Literal) l).byteValue(), "the replacement value should be 0");
        assertEquals(XSD.UNSIGNED_BYTE, ((Literal) l).getDatatype(), "value should be a unsigned byte literal");

        // invalid unsigned byte value
        l = vf.createLiteral("123abcd", XSD.UNSIGNED_BYTE, CoreDatatype.XSD.UNSIGNED_BYTE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(0, ((Literal) l).byteValue(), "the replacement value should be 0");
        assertEquals(XSD.UNSIGNED_BYTE, ((Literal) l).getDatatype(), "value should be a unsigned byte literal");
        
        // long value
        l = vf.createLiteral("1234", XSD.LONG);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1234", l.stringValue());
        assertEquals(1234, ((Literal) l).longValue());
        assertEquals(XSD.LONG, ((Literal) l).getDatatype(), "value should be an long literal");

        // invalid long value
        l = vf.createLiteral("1234abcd", XSD.LONG);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(0, ((Literal) l).longValue(), "the replacement value should be 0");
        assertEquals(XSD.LONG, ((Literal) l).getDatatype(), "value should be a long literal");

        // invalid long value
        l = vf.createLiteral("1234abcd", CoreDatatype.XSD.LONG);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(0, ((Literal) l).longValue(), "the replacement value should be 0");
        assertEquals(XSD.LONG, ((Literal) l).getDatatype(), "value should be a long literal");

        // invalid long value
        l = vf.createLiteral("1234abcd", XSD.LONG, CoreDatatype.XSD.LONG);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(0, ((Literal) l).longValue(), "the replacement value should be 0");
        assertEquals(XSD.LONG, ((Literal) l).getDatatype(), "value should be a long literal");

        // decimal value
        l = vf.createLiteral("1234.5", XSD.DECIMAL);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1234.5", l.stringValue());
        assertEquals(new BigDecimal(1234.5), ((Literal) l).decimalValue());
        assertEquals(XSD.DECIMAL, ((Literal) l).getDatatype(), "value should be an decimal literal");

        // invalid decimal value
        l = vf.createLiteral("1234.5abcd", XSD.DECIMAL);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(new BigDecimal(0), ((Literal) l).decimalValue(), "the replacement value should be 0");
        assertEquals(XSD.DECIMAL, ((Literal) l).getDatatype(), "value should be a decimal literal");

        // invalid decimal value
        l = vf.createLiteral("1234.5abcd", CoreDatatype.XSD.DECIMAL);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(new BigDecimal(0), ((Literal) l).decimalValue(), "the replacement value should be 0");
        assertEquals(XSD.DECIMAL, ((Literal) l).getDatatype(), "value should be a decimal literal");

        // invalid decimal value
        l = vf.createLiteral("1234.5abcd", XSD.DECIMAL, CoreDatatype.XSD.DECIMAL);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("0", l.stringValue(), "the replacement value should be '0'");
        assertEquals(new BigDecimal(0), ((Literal) l).decimalValue(), "the replacement value should be 0");
        assertEquals(XSD.DECIMAL, ((Literal) l).getDatatype(), "value should be a decimal literal");

        // float value
        l = vf.createLiteral("1234.5", XSD.FLOAT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1234.5", l.stringValue());
        assertEquals(1234.5f, ((Literal) l).floatValue());
        assertEquals(XSD.FLOAT, ((Literal) l).getDatatype(), "value should be an float literal");

        // invalid float value
        l = vf.createLiteral("1234.5abcd", XSD.FLOAT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("NaN", l.stringValue(), "the replacement value should be 'NaN'");
        assertEquals(Float.NaN, ((Literal) l).floatValue(), "the replacement value should be NaN");
        assertEquals(XSD.FLOAT, ((Literal) l).getDatatype(), "value should be a float literal");

        // invalid float value
        l = vf.createLiteral("1234.5abcd", CoreDatatype.XSD.FLOAT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("NaN", l.stringValue(), "the replacement value should be 'NaN'");
        assertEquals(Float.NaN, ((Literal) l).floatValue(), "the replacement value should be NaN");
        assertEquals(XSD.FLOAT, ((Literal) l).getDatatype(), "value should be a float literal");

        // invalid float value
        l = vf.createLiteral("1234.5abcd", XSD.FLOAT, CoreDatatype.XSD.FLOAT);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("NaN", l.stringValue(), "the replacement value should be 'NaN'");
        assertEquals(Float.NaN, ((Literal) l).floatValue(), "the replacement value should be NaN");
        assertEquals(XSD.FLOAT, ((Literal) l).getDatatype(), "value should be a float literal");

        // double value
        l = vf.createLiteral("1234.5", XSD.DOUBLE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1234.5", l.stringValue());
        assertEquals(1234.5d, ((Literal) l).doubleValue());
        assertEquals(XSD.DOUBLE, ((Literal) l).getDatatype(), "value should be an double literal");

        // invalid double value
        l = vf.createLiteral("1234.5abcd", XSD.DOUBLE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("NaN", l.stringValue(), "the replacement value should be 'NaN'");
        assertEquals(Double.NaN, ((Literal) l).doubleValue(), "the replacement value should be NaN");
        assertEquals(XSD.DOUBLE, ((Literal) l).getDatatype(), "value should be a double literal");

        // invalid double value
        l = vf.createLiteral("1234.5abcd", CoreDatatype.XSD.DOUBLE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("NaN", l.stringValue(), "the replacement value should be 'NaN'");
        assertEquals(Double.NaN, ((Literal) l).doubleValue(), "the replacement value should be NaN");
        assertEquals(XSD.DOUBLE, ((Literal) l).getDatatype(), "value should be a double literal");

        // invalid double value
        l = vf.createLiteral("1234.5abcd", XSD.DOUBLE, CoreDatatype.XSD.DOUBLE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("NaN", l.stringValue(), "the replacement value should be 'NaN'");
        assertEquals(Double.NaN, ((Literal) l).doubleValue(), "the replacement value should be NaN");
        assertEquals(XSD.DOUBLE, ((Literal) l).getDatatype(), "value should be a double literal");

        // dateTime value
        l = vf.createLiteral("2023-05-15T08:09:10", XSD.DATETIME);
        assertTrue(l.isLiteral(), "value should be a dateTime literal");
        assertEquals("2023-05-15T08:09:10", l.stringValue());
        assertEquals(XSD.DATETIME, ((Literal) l).getDatatype(), "value should be a dateTime literal");

        // invalid dateTime value
        l = vf.createLiteral("May 15th 2023 08:09:10", XSD.DATETIME);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1970-01-01T00:00:00Z", l.stringValue());
        assertEquals(XSD.DATETIME, ((Literal) l).getDatatype(), "value should be a dateTime literal");

        // invalid dateTime value
        l = vf.createLiteral("May 15th 2023 08:09:10", CoreDatatype.XSD.DATETIME);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1970-01-01T00:00:00Z", l.stringValue());
        assertEquals(XSD.DATETIME, ((Literal) l).getDatatype(), "value should be a dateTime literal");

        // invalid dateTime value
        l = vf.createLiteral("May 15th 2023 08:09:10", XSD.DATETIME, CoreDatatype.XSD.DATETIME);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1970-01-01T00:00:00Z", l.stringValue());
        assertEquals(XSD.DATETIME, ((Literal) l).getDatatype(), "value should be a dateTime literal");

        // date value
        l = vf.createLiteral("2023-05-15", XSD.DATE);
        assertTrue(l.isLiteral(), "value should be a date literal");
        assertEquals("2023-05-15", l.stringValue());
        assertEquals(XSD.DATE, ((Literal) l).getDatatype(), "value should be a date literal");

        // invalid date value
        l = vf.createLiteral("May 15th 2023 08:09:10", XSD.DATE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1970-01-01", l.stringValue());
        assertEquals(XSD.DATE, ((Literal) l).getDatatype(), "value should be a date literal");

        // invalid date value
        l = vf.createLiteral("May 15th 2023 08:09:10", CoreDatatype.XSD.DATE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1970-01-01", l.stringValue());
        assertEquals(XSD.DATE, ((Literal) l).getDatatype(), "value should be a date literal");

        // invalid date value
        l = vf.createLiteral("May 15th 2023 08:09:10", XSD.DATE, CoreDatatype.XSD.DATE);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("1970-01-01", l.stringValue());
        assertEquals(XSD.DATE, ((Literal) l).getDatatype(), "value should be a date literal");

        // duration value
        // 2 years, 6 months, 5 days, 12 hours, 35 minutes, 30 seconds
        l = vf.createLiteral("P2Y6M5DT12H35M30S", XSD.DURATION);
        assertTrue(l.isLiteral(), "value should be a duration literal");
        assertEquals("P2Y6M5DT12H35M30S", l.stringValue());
        assertEquals(XSD.DURATION, ((Literal) l).getDatatype(), "value should be a duration literal");

        // invalid duration value
        l = vf.createLiteral("abcPXYZqwertz1234", XSD.DURATION);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("P0Y", l.stringValue());
        assertEquals(XSD.DURATION, ((Literal) l).getDatatype(), "value should be a duration literal");

        // invalid duration value
        l = vf.createLiteral("abcPXYZqwertz1234", CoreDatatype.XSD.DURATION);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("P0Y", l.stringValue());
        assertEquals(XSD.DURATION, ((Literal) l).getDatatype(), "value should be a duration literal");

        // invalid duration value
        l = vf.createLiteral("abcPXYZqwertz1234", XSD.DURATION, CoreDatatype.XSD.DURATION);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("P0Y", l.stringValue());
        assertEquals(XSD.DURATION, ((Literal) l).getDatatype(), "value should be a duration literal");

        // boolean value
        l = vf.createLiteral("true", XSD.BOOLEAN);
        assertTrue(l.isLiteral(), "value should be a boolean literal");
        assertEquals("true", l.stringValue());
        assertEquals(true, ((Literal) l).booleanValue());
        assertEquals(XSD.BOOLEAN, ((Literal) l).getDatatype(), "value should be a boolean literal");

        // invalid boolean value
        l = vf.createLiteral("xxx-falz-yyy", XSD.BOOLEAN);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("false", l.stringValue());
        assertEquals(false, ((Literal) l).booleanValue());
        assertEquals(XSD.BOOLEAN, ((Literal) l).getDatatype(), "value should be a boolean literal");

        // invalid boolean value
        l = vf.createLiteral("xxx-falz-yyy", CoreDatatype.XSD.BOOLEAN);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("false", l.stringValue());
        assertEquals(false, ((Literal) l).booleanValue());
        assertEquals(XSD.BOOLEAN, ((Literal) l).getDatatype(), "value should be a boolean literal");

        // invalid boolean value
        l = vf.createLiteral("xxx-falz-yyy", XSD.BOOLEAN, CoreDatatype.XSD.BOOLEAN);
        assertTrue(l.isLiteral(), "value should be a literal");
        assertEquals("false", l.stringValue());
        assertEquals(false, ((Literal) l).booleanValue());
        assertEquals(XSD.BOOLEAN, ((Literal) l).getDatatype(), "value should be a boolean literal");

    }

}
