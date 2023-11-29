/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import io.carml.engine.function.FnoFunction;
import io.carml.engine.function.FnoParam;

public class RmlFunctions {
    private static final Logger logger = LoggerFactory.getLogger(RmlFunctions.class);

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
}
