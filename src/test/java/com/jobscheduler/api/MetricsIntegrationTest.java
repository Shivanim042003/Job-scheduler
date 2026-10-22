package com.jobscheduler.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jobscheduler.domain.Job;
import com.jobscheduler.domain.JobRepository;
import com.jobscheduler.domain.JobStatus;
import com.jobscheduler.service.JobService;
import com.jobscheduler.support.IntegrationTest;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;

@IntegrationTest
class MetricsIntegrationTest {

    @Autowired TestRestTemplate rest;
    @Autowired JobService jobService;
    @Autowired JobRepository jobRepository;

    @Test
    void prometheusEndpointExposesJobMetrics() {
        Job job = jobService.submit("echo", Map.of(), null);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                        .isEqualTo(JobStatus.SUCCEEDED));

        String metrics = rest.getForObject("/actuator/prometheus", String.class);

        assertThat(metrics)
                .contains("jobs_execution_seconds_count")
                .contains("jobs_by_status")
                .contains("jobs_queue_depth");
    }
}
