package lk.rumex.rumex_ott_mediaStat;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EntityScan({"lk.rumex.ott_domain_models","lk.rumex.rumex_ott_mediaStat"})
public class RumexOttMediaStatApplication {
	public static void main(String[] args) {
		SpringApplication.run(RumexOttMediaStatApplication.class, args);
	}
}
