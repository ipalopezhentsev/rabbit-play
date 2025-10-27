package ru.ipal.rabbit.consumer.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;

import ru.ipal.rabbit.consumer.model.GreetingResponse;

@Service
public class GreetingResponsePublisher {
    private final String queueName;
    private final ConnectionFactory connFactory;
    @Autowired
    private ObjectMapper objectMapper;

    public GreetingResponsePublisher(
        @Value("${RABBIT_MQ_SERVER:}") String rabbitHost,
        @Value("${HELLO_RESP_QUEUE_NAME:}") String queueName
    ) {
        this.queueName = queueName;
        connFactory = new ConnectionFactory();
        connFactory.setHost(rabbitHost);
    }

    public void publishResponse(GreetingResponse resp) throws IOException, TimeoutException {
        var respSer = objectMapper.writeValueAsString(resp);
        try (var conn = connFactory.newConnection();
                var channel = conn.createChannel()) {
            channel.queueDeclare(queueName, false, false, false, null);
            channel.basicPublish("", queueName, null, respSer.getBytes(StandardCharsets.UTF_8));
        }
    }
}
