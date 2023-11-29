/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

public interface ConversionListener {
    void startInputFile(String fileName);

    void endInputFile(boolean success);

    void startDocument();

    void endDocument(boolean success, long statements);
}
