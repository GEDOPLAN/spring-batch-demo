package de.gedoplan.demo.sbatch.config;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class RestClientConfiguration {

    @Bean
    public RestTemplate dummyJson(RestTemplateBuilder restTemplateBuilder){
        return restTemplateBuilder.rootUri("https://dummyjson.com").build();
    }
}
