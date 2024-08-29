
package de.gedoplan.demo.sbatch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@EnableScheduling
public class BatchScheduler {

    final JobLauncher jobLauncher;

    final Job simpleAsPossibleJob;

    final Job products;

    @Scheduled(cron = "30 * * * * *")
    public void simpleJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        Map<String, JobParameter<?>> parameter = new HashMap<>();
        parameter.put("message", new JobParameter<>("Hello", String.class));

        jobLauncher.run(simpleAsPossibleJob, new JobParameters(parameter));
    }

    @Scheduled(cron = "45 * * * * *")
    public void simpleUniqueJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        Map<String, JobParameter<?>> parameter = new HashMap<>();
        parameter.put("message", new JobParameter<>("Hello", String.class));
        parameter.put("date", new JobParameter<>(Instant.now().toString(), String.class));

        jobLauncher.run(simpleAsPossibleJob, new JobParameters(parameter));
    }

    @Scheduled(cron = "0 * * * * *")
    public void productsJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        jobLauncher.run(products, new JobParameters());
    }
}
