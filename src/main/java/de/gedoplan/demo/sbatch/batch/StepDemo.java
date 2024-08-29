package de.gedoplan.demo.sbatch.batch;

import de.gedoplan.demo.sbatch.repository.ProductRepository;
import de.gedoplan.demo.sbatch.repository.model.Product;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.AbstractPagingItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Configuration
@RequiredArgsConstructor
@Log
public class StepDemo {

    private final ProductRepository productRepository;
    private final DummyJsonItemReader dummyJsonItemReader;

    @Bean
    public Job products(JobRepository jobRepository, PlatformTransactionManager ptm) {
        return new JobBuilder("ImportProducts", jobRepository)
                .start(cleanDbStep(jobRepository, ptm, null))
                .next(importProductsStep(jobRepository, ptm))
                .build();
    }

    @Bean
    @JobScope
    public Step cleanDbStep(JobRepository jobRepository, PlatformTransactionManager ptm, @Value("#{jobExecution}") JobExecution jobExecution) {
        return new StepBuilder("cleanDB", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    JobExecution last = jobRepository.getLastJobExecution(jobExecution.getJobInstance().getJobName(), jobExecution.getJobParameters());
                    if (last != null && !last.getExitStatus().equals(ExitStatus.COMPLETED)) {
                        log.info("Last run was NOT successfully, we keep our data");
                    } else {
                        log.info("Last run was successfully, lets clean the database for a fresh copy");
                        productRepository.deleteAll();
                    }
                    return RepeatStatus.FINISHED;
                }, ptm)
                .allowStartIfComplete(true)
                .build();
    }

    private Step importProductsStep(JobRepository jobRepository, PlatformTransactionManager ptm) {
        return new StepBuilder("importProducts", jobRepository)
                .<Product, Product>chunk(10, ptm)
                .reader(dummyJsonItemReader)
//                .reader(readProduct(null))
                .writer(writeProduct())
                .faultTolerant()
                .retry(IOException.class)
                .retry(RestClientException.class)
                .retryLimit(5)
                .allowStartIfComplete(true)
                .listener(idToContext(null))
                .build();
    }


    @Bean
    @StepScope
    public ItemWriteListener<Product> idToContext(@Value("#{stepExecution}") StepExecution stepExecution) {
        return new ItemWriteListener<>() {
            @Override
            public void afterWrite(Chunk<? extends Product> items) {
                stepExecution.getJobExecution().getExecutionContext().put("lastId", items.getItems().get(items.size() - 1).getId());
            }
        };
    }


    private ItemWriter<Product> writeProduct() {
        return (products) -> {
                productRepository.saveAll(products);
// let's fail for some reason
//            for (Product p : products) {
//                if (p.getId().equals(new BigDecimal(50))){
//                if (p.getId().compareTo(new BigDecimal(125)) > 0) {
//                    throw new IOException("IDs bigger then 125 are unfriendly");
//                }
//            }
            productRepository.flush();
        };
    }

    @Bean
    @StepScope
    public ItemReader<Product> readProduct(@Value("#{stepExecution}") StepExecution stepExecution) {
        return () -> {
            BigDecimal newId = Optional.ofNullable(stepExecution.getExecutionContext().get("lastId")).map(v -> (BigDecimal) v).orElse(BigDecimal.ZERO).add(BigDecimal.ONE);
            if(newId.compareTo(new BigDecimal(250))>0) {
                return null;
            }
            stepExecution.getExecutionContext().put("lastId", newId);
            return Product.builder().title("Dummy").id(newId).build();
        };
    }

    @Data
    private static class ProductWrapper {
        private List<Product> products;
    }

    @Component
    @StepScope
    public static class DummyJsonItemReader extends AbstractPagingItemReader<Product> {

        private final RestTemplate dummyJson;

        public DummyJsonItemReader(RestTemplate dummyJson) {
            this.dummyJson = dummyJson;
            this.setPageSize(100);
        }

        protected void doReadPage() {
            if (this.results == null) {
                this.results = new ArrayList<>();
            } else {
                this.results.clear();
            }

            String requestURI = UriComponentsBuilder.fromUriString("/product")
                    .queryParam("limit", 100)
                    .queryParam("skip", getPage() * 100)
                    .build()
                    .toUriString();

            logger.info("Process URL " + requestURI);
            ProductWrapper products = dummyJson.getForObject(requestURI, ProductWrapper.class);

            if (products != null && !products.getProducts().isEmpty()) {
                this.results.addAll(products.getProducts());
            }
        }
    }

}
