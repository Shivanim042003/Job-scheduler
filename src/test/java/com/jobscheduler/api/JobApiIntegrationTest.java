package com.jobscheduler.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jobscheduler.domain.JobStatus;
import com.jobscheduler.support.IntegrationTest;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@IntegrationTest
class JobApiIntegrationTest {

    @Autowired
    TestRestTemplate rest;

    @Test
    void submittedJobIsAcceptedTrackedAndCompleted() {
        ResponseEntity<JobResponse> accepted = rest.postForEntity(
                "/api/jobs", Map.of("type", "echo", "payload", Map.of("msg", "hi")), JobResponse.class);

        assertThat(accepted.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(accepted.getHeaders().getLocation()).isNotNull();
        UUID id = accepted.getBody().id();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            JobResponse job = rest.getForObject("/api/jobs/" + id, JobResponse.class);
            assertThat(job.status()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(job.attempts()).isEqualTo(1);
        });
    }

    @Test
    void unknownJobReturns404() {
        ResponseEntity<String> response = rest.getForEntity("/api/jobs/" + UUID.randomUUID(), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void blankTypeIsRejected() {
        ResponseEntity<String> response = rest.postForEntity("/api/jobs", Map.of("type", ""), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void maxAttemptsAboveTheConfiguredLimitIsRejected() {
        ResponseEntity<String> response =
                rest.postForEntity("/api/jobs", Map.of("type", "echo", "maxAttempts", 99), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
