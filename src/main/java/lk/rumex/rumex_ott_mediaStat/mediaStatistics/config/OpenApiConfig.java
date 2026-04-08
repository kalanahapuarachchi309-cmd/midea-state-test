package lk.rumex.rumex_ott_mediaStat.mediaStatistics.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI apiDocumentation() {
        return new OpenAPI()
                .info(new Info()
                        .title("Media Statistics API")
                        .description("API Documentation for Media Statistics endpoints")
                        .version("v1.0.0")
                );
    }
}
