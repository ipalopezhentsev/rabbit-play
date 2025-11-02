package ru.ipal.rabbit.publisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import ru.ipal.rabbit.RabbitMqProperties;

@SpringBootApplication
@EnableConfigurationProperties(RabbitMqProperties.class)
public class PublisherApp {

	public static void main(String[] args) {
		SpringApplication.run(PublisherApp.class, args);
	}
}
