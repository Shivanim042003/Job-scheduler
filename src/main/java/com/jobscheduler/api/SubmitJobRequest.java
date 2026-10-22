package com.jobscheduler.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.Map;

public record SubmitJobRequest(
        @NotBlank @Size(max = 100) String type,
        Map<String, Object> payload,
        Integer maxAttempts) {
}
