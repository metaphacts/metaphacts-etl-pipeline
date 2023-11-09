package com.metaphacts.ds.tm.lambda;

public interface ConversionListener {
    void startInputFile(String fileName);

    void endInputFile(boolean success);

    void startDocument();

    void endDocument(boolean success, long statements);
}
