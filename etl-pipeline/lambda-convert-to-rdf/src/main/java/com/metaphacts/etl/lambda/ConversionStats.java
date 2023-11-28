package com.metaphacts.etl.lambda;

public class ConversionStats implements ConversionListener {
    private long inputFileCount = 0;
    private long documentCount = 0;
    private long rdfStatementCount = 0;
    private long successfulConversionCount = 0;
    private long failedConversionCount = 0;
    private long startTimeInputFile = 0;
    private long startTimeRDFConversion = 0;
    private long aggregatedTimeInputFiles = 0;
    private long aggregatedTimeRDFConversions = 0;
    private float averageTimeInputFiles = 0;
    private float averageTimeRDFConversions = 0;

    @Override
    public void startInputFile(String fileName) {
        inputFileCount++;
        startTimeInputFile = now();
    }

    private long now() {
        return System.currentTimeMillis();
    }

    @Override
    public void endInputFile(boolean success) {
        long duration = now() - startTimeInputFile;
        aggregatedTimeInputFiles += duration;
        startTimeInputFile = 0;
        averageTimeInputFiles = ((float) aggregatedTimeInputFiles) / ((float) inputFileCount);
    }

    @Override
    public void startDocument() {
        documentCount++;
        startTimeRDFConversion = now();
    }

    @Override
    public void endDocument(boolean success, long statements) {
        rdfStatementCount += statements;
        if (success) {
            successfulConversionCount++;
        } else {
            failedConversionCount++;
        }

        long duration = now() - startTimeRDFConversion;
        aggregatedTimeRDFConversions += duration;
        startTimeRDFConversion = 0;
        averageTimeRDFConversions = ((float) aggregatedTimeRDFConversions) / ((float) documentCount);
    }

    public float getAverageTimeInputFiles() {
        return averageTimeInputFiles;
    }

    public float getAverageTimeRDFConversions() {
        return averageTimeRDFConversions;
    }

    public long getInputFileCount() {
        return inputFileCount;
    }

    public long getJsonDocumentCount() {
        return documentCount;
    }

    public long getRdfStatementCount() {
        return rdfStatementCount;
    }

    public long getSuccessfulConversionCount() {
        return successfulConversionCount;
    }

    public long getFailedConversionCount() {
        return failedConversionCount;
    }
    
    public String getSummary() {
        StringBuilder b = new StringBuilder();
        
        b.append("processed ")
            .append(inputFileCount).append(" input files (avg ")
            .append(averageTimeInputFiles).append("ms per input file), ")
            .append(documentCount).append(" JSON docs (avg ")
            .append(averageTimeRDFConversions).append("ms per JSON doc), ")
            .append(rdfStatementCount).append(" RDF statements, ")
            .append(successfulConversionCount).append(" successful, ")
            .append(failedConversionCount).append(" failed");
        
        return b.toString();
    }

    @Override
    public String toString() {
        return getSummary();
    }
}
