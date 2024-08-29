package de.gedoplan.demo.sbatch.batch;

import de.gedoplan.demo.sbatch.repository.ProductRepository;
import de.gedoplan.demo.sbatch.repository.model.Product;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Configuration
@RequiredArgsConstructor
@Log
public class StepDemo {

    private final RestTemplate dummyJson;
    private final ProductRepository productRepository;

    @Bean
    public Job products(JobRepository jobRepository, PlatformTransactionManager ptm) {
        return new JobBuilder("ImportProducts", jobRepository)
                .start(cleanDbStep(jobRepository, ptm))
                .next(importProductsStep(jobRepository, ptm))
                .build();
    }

    private Step cleanDbStep(JobRepository jobRepository, PlatformTransactionManager ptm) {
        return new StepBuilder("cleanDB", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    productRepository.deleteAll();
                    return RepeatStatus.FINISHED;
                }, ptm)
                .build();
    }

    private Step importProductsStep(JobRepository jobRepository, PlatformTransactionManager ptm) {
        return new StepBuilder("importProducts", jobRepository)
                .<Product, Product>chunk(10, ptm)
                .reader(dummyJSONItemReader(null))
                .writer(writeProduct())
                .faultTolerant()
                .retry(IOException.class)
                .retry(RestClientException.class)
                .retryLimit(5)
                .skip(RuntimeException.class)
                .skipLimit(5)
                .allowStartIfComplete(true)
                .listener(idToContext( null))
                .build();
    }


    @Bean
    @StepScope
    public ItemWriteListener<Product> idToContext(@Value("#{stepExecution}") StepExecution stepExecution){
        return new ItemWriteListener<>() {
            @Override
            public void afterWrite(Chunk<? extends Product> items) {
                stepExecution.getJobExecution().getExecutionContext().put("lastId", items.getItems().get(items.size()-1).getId());
            }
        };
    }

    @Bean
    @StepScope
    public ItemReader<Product> dummyJSONItemReader(@Value("#{stepExecution}") StepExecution stepExecution) {
        return new AbstractPagingItemReader<>() {

            @Override
            protected void doReadPage() {
                if (this.results == null) {
                    this.results = new ArrayList<>();
                    this.setPageSize(100);
                    logger.info("Item id from last Run: " + stepExecution.getJobExecution().getExecutionContext().get("lastId"));
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
        };
    }

    private ItemWriter<Product> writeProduct() {
        return (products) -> {
            for (Product p : products) {
// let's fail for some reason
//                if (p.getId().equals(new BigDecimal(50))){
//                if (p.getId().compareTo(new BigDecimal(125)) > 0) {
//                    throw new IOException("IDs bigger then 125 are unfriendly");
//                }
                productRepository.save(p);
            }
            productRepository.flush();
        };

    }

    @Data
    private static class ProductWrapper {
        private List<Product> products;
    }

}
