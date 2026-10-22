package com.jobscheduler.worker;

import com.jobscheduler.domain.Job;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** Dispatches a job to the handler registered for its type. */
@Component
@Profile("worker")
public class JobExecutor {

    private final Map<String, JobHandler> handlers = new HashMap<>();

    public JobExecutor(List<JobHandler> handlerBeans) {
        for (JobHandler handler : handlerBeans) {
            if (handlers.put(handler.type(), handler) != null) {
                throw new IllegalStateException("Duplicate handler for job type: " + handler.type());
            }
        }
    }

    public void execute(Job job) throws Exception {
        JobHandler handler = handlers.get(job.getType());
        if (handler == null) {
            throw new NonRetryableJobException("No handler registered for job type '" + job.getType() + "'");
        }
        handler.handle(job);
    }
}
