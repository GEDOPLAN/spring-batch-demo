package de.gedoplan.demo.sbatch.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class SimpleAsPossible {

    private static int counter = 0;

    @Bean
    public Job simpleAsPossibleJob(JobRepository jobRepository, PlatformTransactionManager ptm) {
        return new JobBuilder("SimpleAsPossible", jobRepository)
                .start(singleStep(jobRepository, ptm))
                .build();
    }

    public Step singleStep(JobRepository jobRepository, PlatformTransactionManager ptm) {
        return new StepBuilder("singleStep", jobRepository)
                .tasklet(singleStepTasklet(), ptm)
                .allowStartIfComplete(true)
                .build();
    }

    private Tasklet singleStepTasklet() {
        return (contribution, chunkContext) -> {
            System.out.println("Hello World");
            chunkContext.getStepContext().getStepExecution().getExecutionContext().put("counter", ++counter);
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public ExecutionContextSerializer jacksonSerializer() {
        return new Jackson2ExecutionContextStringSerializer();
    }
}
