package com.newstracker.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Configuration
public class BigQueryConfig {

  @Value("${google.cloud.project-id}")
  private String projectId;

  @Value("${bigquery.credentials.path}")
  private String credentialsPath;

  @Bean
  public BigQuery bigQuery() throws IOException {
    System.out.println("Attempting to create BigQuery client...");
    System.out.println("Project ID: " + projectId);
    System.out.println("Credentials path: " + credentialsPath);

    if (!Files.exists(Paths.get(credentialsPath))) {
      throw new RuntimeException("Credentials file not found at: " + credentialsPath);
    }

    GoogleCredentials credentials = GoogleCredentials
        .fromStream(new FileInputStream(credentialsPath));

    BigQuery bigQuery = BigQueryOptions.newBuilder()
        .setProjectId(projectId)
        .setCredentials(credentials)
        .build()
        .getService();

    System.out.println("BigQuery client created successfully!");
    return bigQuery;
  }
}
