package com.newstracker.controller;

import com.google.cloud.bigquery.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.MediaType;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class TestController {

  @Autowired
  private BigQuery bigQuery;

  @GetMapping("/")
  public Map<String, Object> hello() {
    Map<String, Object> response = new HashMap<>();
    response.put("message", "Hello World!");
    response.put("status", "running");
    response.put("timestamp", LocalDateTime.now());
    response.put("port", 8083);
    return response;
  }

  @GetMapping(value = "/bigquery-test", produces = MediaType.APPLICATION_JSON_VALUE + ";charset=UTF-8")
  public Map<String, Object> testBigQuery() {
    Map<String, Object> response = new HashMap<>();
    try {
      String query = "SELECT word_text FROM `news-trending-tracker.scraper_data.words` LIMIT 10";
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
      TableResult result = bigQuery.query(queryConfig);

      if (result.getTotalRows() > 0) {
        List<String> words = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
          words.add(row.get("word_text").getStringValue());
        }
        response.put("queryResult", words);
        response.put("totalRows", result.getTotalRows());
      } else {
        response.put("queryResult", "No data found in words table");
        response.put("totalRows", 0);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      response.put("status", "error");
      response.put("message", "Query was interrupted: " + e.getMessage());
    } catch (Exception e) {
      response.put("status", "error");
      response.put("message", "BigQuery connection failed: " + e.getMessage());
      e.printStackTrace();
    }
    return response;
  }
}