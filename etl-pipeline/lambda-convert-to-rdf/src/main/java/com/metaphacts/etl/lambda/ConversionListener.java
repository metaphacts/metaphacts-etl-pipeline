package com.metaphacts.etl.lambda;

public interface ConversionListener {
    void startInputFile(String fileName);

    void endInputFile(boolean success);

    void startDocument();

    void endDocument(boolean success, long statements);
}
