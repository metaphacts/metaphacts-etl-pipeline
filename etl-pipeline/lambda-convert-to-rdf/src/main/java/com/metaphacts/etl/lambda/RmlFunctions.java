/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import io.carml.engine.function.FnoFunction;
import io.carml.engine.function.FnoParam;

/**
 * This class provides a set of utility functions that can be invoked from RML
 * mappings.
 */
public class RmlFunctions {
    private static final Logger logger = LoggerFactory.getLogger(RmlFunctions.class);

    /**
     * Return the current date.
     * 
     * @return current date in <code>xsd:date</code> format.
     */
    @FnoFunction("urn:today")
    public String dateToday() {
        String response = null;
        try {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
            response = fmt.format(new Date());
        } catch (Exception e) {
            logger.warn("Failed to convert timestamp {} to dateTime: {}", e.getMessage());
        }
        return response;
    }

    /**
     * Converts a unix timestamp to date-time string.
     * 
     * @param epoch unix timestamp (seconds since epoc) as string
     * 
     * @return current date in <code>xsd:dateTime</code> format.
     */
    @FnoFunction("urn:epochTimeToDateTime")
    public String epochTimeToDateTime(@FnoParam("urn:epochTime") String epoch) {
      if (epoch == null) {
         return null;
      }

      Date timestamp = null;
      SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      try {
          // interpret as numeric timestamp in milli seconds since Jan 1st 1970 midnight
          timestamp = new Date(Long.parseLong(epoch) / 1000);
      } catch (Exception e) {
          try {
              // interpret as formatted date+time
              timestamp = dateTimeFormat.parse(epoch);
          } catch (ParseException e2) {
              try {
                  // interpret as formatted date
                  timestamp = dateFormat.parse(epoch);
              } catch (ParseException e3) {
                  timestamp = null;
              }
          }
      }

      if (timestamp != null) {
          return dateTimeFormat.format(timestamp);
      }
      logger.debug("Failed to convert timestamp {} to dateTime: no suitable format found", epoch);
      return null;
    }

    /**
     * Generate a hashed IRI from a prefix and some parameters
     * 
     * @param prefix IRI prefix
     * @param param1 first parameter
     * @param param2 second parameter
     * @param param3 third parameter
     * @return generated IRI
     */
    @FnoFunction("urn:generateHashedIRI")
    public String generateHashedIRI(@FnoParam("urn:prefix") String prefix, @FnoParam("urn:param1") String param1, @FnoParam("urn:param2") String param2, @FnoParam("urn:param3") String param3) {
      final String DELIMITER = "_";
      String[] params = {param1, param2, param3};
      String response = null;
      try {
        boolean canGenerateIRI = true;
        for (String param:params){
          if (param == null){
            logger.debug("Null values found in the generation of the IRI: {} {}", prefix, params);
            canGenerateIRI = false;
            break;
          }
        }
        if (canGenerateIRI) {
          HashFunction hashFunction = Hashing.sha256();
          String joinParams = String.join(DELIMITER, params);
          HashCode hash = hashFunction.hashString(joinParams, StandardCharsets.UTF_8);
          response = prefix + hash;
        }
      } catch (Exception e) {
        logger.warn("Failed to generate hashed IRI : {} {}, exception", prefix, params, e.getMessage());
      }
      return response;
    }

    /**
     * Normalize a date.
     * 
     * <p>
     * This fixes partial dates, e.g. just a year (e.g. <code>2023</code>, adds
     * Januar 1st) or year and month (e.g. <code>2023-04</code>, adds 1st day of the
     * month) or different date-time formats without dashes.
     * 
     * @param date date as input string
     * @return date in <code>xsd:date</code> format
     */
    @FnoFunction("urn:normalizeDate")
    public String normalizeDate(@FnoParam("urn:date") String date) {
      if (date == null) {
         return null;
      }
      String response = null;
      try {
        if (date.matches("^\\d{4}$")) {
          response = date + "-01-01";
        } else if (date.matches("^\\d{4}-\\d{2}$")) {
          response = date + "-01";
        } else if (date.matches("^\\d{4}\\d{2}\\d{2}$")) {
          SimpleDateFormat sdfInput = new SimpleDateFormat("yyyyMMdd");
          Date dateObj = sdfInput.parse(date);
          SimpleDateFormat sdfOutput = new SimpleDateFormat("yyyy-MM-dd");
          response = sdfOutput.format(dateObj);
        } else {
          response = date;
        }
      } catch (Exception e) {
        logger.warn("Failed to normalize date {}, exception : {}", date, e.getMessage());
      }
      return response;
    }

    /**
     * Normalize (format) a unix timestamp
     * 
     * @param date unix timestamp (seconds since epoc) as string
     * @return date in <code>xsd:date</code> format
     */
    @FnoFunction("urn:normalizeUNIXDate")
    public String normalizeUNIXDate(@FnoParam("urn:date") String date) {
      if (date == null) {
         return null;
      }
      String response = null;
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
      try {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.add(Calendar.DATE, Integer.parseInt(date));
        Date dateRepresentation = cal.getTime();
        response = formatter.format(dateRepresentation);
      } catch (Exception e) {
        logger.warn("Failed to normalize date {}, exception : {}", date, e.getMessage());
      }
      return response;
    }

    /**
     * Generate a list of IRIs for the list of provided values based on a prefix and
     * optional replacement of values
     * 
     * @param prefix  IRI prefix
     * @param inputs  list of values for which to generate an IRI
     * @param search  search pattern (regular expression) within an input value to
     *                be replaced
     * @param replace replacement (may include references to groups in search
     *                pattern) for the search value
     * 
     * @return list of generated IRIs
     */
    @FnoFunction("urn:generateIRIWithReplace")
    public List<String> generateIRIWithReplace(@FnoParam("urn:prefix") String prefix, @FnoParam("urn:input") List<String> inputs, @FnoParam("urn:search") String search, @FnoParam("urn:replace") String replace) {
      if (inputs == null || inputs.isEmpty()) {
         return List.of();
      }
      List<String> response = Lists.newArrayList();
      for (String inputValue : inputs){
        try {
          String output = inputValue.replaceAll(search, replace);
          response.add(prefix+output);
        } catch (Exception e) {
          logger.warn("Failed to generate IRI : {} {} {} {}, exception {}", prefix, inputs, search, replace, e.getMessage());
        }
      }
      return response;
    }

    /**
     * Generate a list of IRIs for the list of provided values. If provided, each
     * value is split by the separator and trimmed before appending it to the
     * prefix.
     * 
     * @param prefix    IRI prefix
     * @param inputs    list of values for which to generate an IRI
     * @param separator separator used to split each input value
     * 
     * @return list of generated IRIs
     */
    @FnoFunction("urn:generateIRIWithSplit")
    public List<String> generateIRIWithSplit(@FnoParam("urn:prefix") String prefix,
            @FnoParam("urn:input") List<String> inputs, @FnoParam("urn:separator") String separator) {
        if (inputs == null || inputs.isEmpty()) {
            return List.of();
        }
        // iterate over provided values
        return inputs.stream()
                // filter null values
                .filter(Objects::nonNull)
                // split them into parts when a separator is provided
                .flatMap(input -> splitValues(input, separator).stream())
                // generate IRI
                .map(input -> generateIRIWithReplace(prefix, input))
                // filter null values
                .filter(Objects::nonNull)
                // return as list
                .collect(Collectors.toList());
    }

    /**
     * Split value.
     * 
     * @param inputValue value to split
     * @param separator  separator. When <code>null</code>, there original value is
     *                   returned unchanged
     * @return list of values or single value when no separator is provided
     */
    private Collection<? extends String> splitValues(String inputValue, String separator) {
        if (inputValue == null) {
            return List.of();
        }
        if (separator == null) {
            return List.of(inputValue);
        }
        String[] parts = inputValue.split(separator);
        if (parts == null) {
            return List.of();
        }
        return Arrays.asList(parts);
    }

    /**
     * Generate an IRI for the provided value.
     * 
     * The input value is trimmed to remove leading and trailing whitespace.
     * 
     * @param prefix     IRI prefix
     * @param inputValue value for which to generate an IRI.
     * 
     * @return generated IRI or <code>null</code> if no value is provided
     */
    private String generateIRIWithReplace(String prefix, String inputValue) {
        if (inputValue == null) {
            return null;
        }
        return prefix + inputValue.trim();
    }
}
