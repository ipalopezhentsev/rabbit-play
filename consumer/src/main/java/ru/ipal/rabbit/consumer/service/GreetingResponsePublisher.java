package ru.ipal.rabbit.consumer.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import ru.ipal.rabbit.consumer.model.GreetingResponse;

@Service
public class GreetingResponsePublisher {
    private final String queueName;
    private final boolean isDurable;
    private final ConnectionFactory connFactory;
    @Autowired
    private ObjectMapper objectMapper;

    public GreetingResponsePublisher(
        @Value("${RABBIT_MQ_SERVER:}") String rabbitHost,
        @Value("${HELLO_RESP_QUEUE_NAME:}") String queueName,
        @Value("${IS_TOPIC_DURABLE:false}") boolean isDurable
    ) {
        this.queueName = queueName;
        this.isDurable = isDurable;
        connFactory = new ConnectionFactory();
        connFactory.setHost(rabbitHost);
    }

    public void publishResponse(GreetingResponse resp) throws IOException, TimeoutException {
        var respSer = objectMapper.writeValueAsString(resp);
        try (var conn = connFactory.newConnection();
                var channel = conn.createChannel()) {
            channel.queueDeclare(queueName, isDurable, false, false, null);
            var props = isDurable ? MessageProperties.PERSISTENT_TEXT_PLAIN : null;
            channel.basicPublish("", queueName, props, respSer.getBytes(StandardCharsets.UTF_8));
        }
    }
}
