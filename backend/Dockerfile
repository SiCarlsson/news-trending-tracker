FROM eclipse-temurin:21-jdk-alpine AS build

WORKDIR /app

COPY gradlew gradlew.bat ./
COPY gradle gradle
COPY build.gradle settings.gradle ./

RUN chmod +x gradlew

COPY src src

RUN ./gradlew build -x test

FROM eclipse-temurin:21-jre-alpine

RUN apk add --no-cache wget

RUN addgroup -g 1001 -S newsgroup && \
  adduser -u 1001 -S newsuser -G newsgroup

WORKDIR /app

COPY --from=build /app/build/libs/*.jar app.jar

RUN chown newsuser:newsgroup app.jar

USER newsuser

ENV GOOGLE_CLOUD_PROJECT=news-trending-tracker
ENV BIGQUERY_DATASET=scraper_data
ENV SPRING_PROFILES_ACTIVE=docker
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/backend-bigquery-service-account.json

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
