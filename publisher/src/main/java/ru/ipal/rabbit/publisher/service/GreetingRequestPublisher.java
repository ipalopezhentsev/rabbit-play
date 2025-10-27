package ru.ipal.rabbit.publisher.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;

import lombok.extern.slf4j.Slf4j;
import ru.ipal.rabbit.publisher.model.GreetingRequest;

@Service
@Slf4j
public class GreetingRequestPublisher {
    private final String queueName;
    private final ConnectionFactory connFactory;
    @Autowired
    private ObjectMapper objectMapper;

    public GreetingRequestPublisher(
            @Value("${RABBIT_MQ_SERVER:}") String rabbitHost,
            @Value("${HELLO_RQ_QUEUE_NAME:}") String queueName) {
        this.queueName = queueName;
        connFactory = new ConnectionFactory();
        connFactory.setHost(rabbitHost);
    }


    public void publishGreet(GreetingRequest rq) throws IOException, TimeoutException {
        var rqSer = objectMapper.writeValueAsString(rq);
        try (var conn = connFactory.newConnection();
                var channel = conn.createChannel()) {
            channel.queueDeclare(queueName, false, false, false, null);
            channel.basicPublish("", queueName, null, rqSer.getBytes(StandardCharsets.UTF_8));
        }
    }
}
