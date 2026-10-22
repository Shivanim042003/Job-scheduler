package com.jobscheduler.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.jobscheduler.api.JobResponse;
import com.jobscheduler.support.IntegrationTest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

@IntegrationTest
class IdempotencyKeyIntegrationTest {

    @Autowired JobService jobService;
    @Autowired JdbcTemplate jdbc;
    @Autowired TestRestTemplate rest;

    @Test
    void repeatedSubmissionWithTheSameKeyReturnsTheOriginalJob() {
        String key = newKey();

        JobService.SubmitResult first = jobService.submit("echo", Map.of(), null, key);
        JobService.SubmitResult second = jobService.submit("echo", Map.of("ignored", "payload"), null, key);

        assertThat(first.created()).isTrue();
        assertThat(second.created()).isFalse();
        assertThat(second.job().getId()).isEqualTo(first.job().getId());
        assertThat(jobsWithKey(key)).isEqualTo(1);
    }

    @Test
    void differentKeysCreateDifferentJobs() {
        JobService.SubmitResult a = jobService.submit("echo", Map.of(), null, newKey());
        JobService.SubmitResult b = jobService.submit("echo", Map.of(), null, newKey());

        assertThat(a.job().getId()).isNotEqualTo(b.job().getId());
    }

    @Test
    void submissionsWithoutAKeyAreNeverDeduplicated() {
        JobService.SubmitResult a = jobService.submit("echo", Map.of(), null, null);
        JobService.SubmitResult b = jobService.submit("echo", Map.of(), null, null);

        assertThat(a.job().getId()).isNotEqualTo(b.job().getId());
    }

    @Test
    void manyConcurrentSubmissionsWithOneKeyCreateExactlyOneJob() throws Exception {
        String key = newKey();
        int callers = 8;

        ExecutorService pool = Executors.newFixedThreadPool(callers);
        try {
            CountDownLatch ready = new CountDownLatch(callers);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<JobService.SubmitResult>> futures = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return jobService.submit("echo", Map.of(), null, key);
                }));
            }
            ready.await();
            go.countDown();

            Set<UUID> ids = new HashSet<>();
            int created = 0;
            for (Future<JobService.SubmitResult> future : futures) {
                JobService.SubmitResult result = future.get(30, TimeUnit.SECONDS);
                ids.add(result.job().getId());
                if (result.created()) {
                    created++;
                }
            }
            assertThat(ids).hasSize(1);
            assertThat(created).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
        assertThat(jobsWithKey(key)).isEqualTo(1);
    }

    @Test
    void httpReplayReturns200WithTheSameJobInsteadOf202() {
        String key = newKey();
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", key);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(Map.of("type", "echo"), headers);

        ResponseEntity<JobResponse> first = rest.exchange("/api/jobs", HttpMethod.POST, request, JobResponse.class);
        ResponseEntity<JobResponse> second = rest.exchange("/api/jobs", HttpMethod.POST, request, JobResponse.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getHeaders().getFirst("Idempotent-Replay")).isEqualTo("true");
        assertThat(second.getBody().id()).isEqualTo(first.getBody().id());
    }

    @Test
    void oversizedKeyIsRejected() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", "x".repeat(201));

        ResponseEntity<String> response = rest.exchange("/api/jobs", HttpMethod.POST,
                new HttpEntity<>(Map.of("type", "echo"), headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private int jobsWithKey(String key) {
        Integer count = jdbc.queryForObject("select count(*) from jobs where idempotency_key = ?", Integer.class, key);
        return count == null ? 0 : count;
    }

    private static String newKey() {
        return "test-key-" + UUID.randomUUID();
    }
}
