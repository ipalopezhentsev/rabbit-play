package ru.ipal.rabbit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.NotBlank;

@ConfigurationProperties(prefix = "rabbitmq")
@ConfigurationPropertiesBinding
@Validated
public record RabbitMqProperties(
    @NotBlank
    String server,
    boolean isDurable,
    @NotBlank
    String helloRqQueue,
    @NotBlank
    String helloRespQueue,
    @NotBlank
    String logRqFanoutExchange,
    @NotBlank
    String logRqRoutingExchange,
    @NotBlank
    String logRqRoutingConsumerBindingKey,
    @NotBlank
    String logRqTopicExchange,
    //comma-separated patterns
    @NotBlank
    String logRqTopicConsumerBindingKeys
    ) {
    
}
