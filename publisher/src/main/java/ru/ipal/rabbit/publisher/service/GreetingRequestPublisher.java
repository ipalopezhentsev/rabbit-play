package ru.ipal.rabbit.publisher.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import lombok.extern.slf4j.Slf4j;
import ru.ipal.rabbit.publisher.model.GreetingRequest;

@Service
@Slf4j
public class GreetingRequestPublisher {
    private final String queueName;
    private final boolean isDurable;
    private final ConnectionFactory connFactory;
    @Autowired
    private ObjectMapper objectMapper;

    public GreetingRequestPublisher(
            @Value("${RABBIT_MQ_SERVER:}") String rabbitHost,
            @Value("${HELLO_RQ_QUEUE_NAME:}") String queueName,
            @Value("${IS_TOPIC_DURABLE:false}") boolean isDurable) {
        this.queueName = queueName;
        this.isDurable = isDurable;
        connFactory = new ConnectionFactory();
        connFactory.setHost(rabbitHost);
    }


    public void publishGreet(GreetingRequest rq) throws IOException, TimeoutException {
        var rqSer = objectMapper.writeValueAsString(rq);
        try (var conn = connFactory.newConnection();
                var channel = conn.createChannel()) {
            channel.queueDeclare(queueName, isDurable, false, false, null);
            var props = isDurable ? MessageProperties.PERSISTENT_TEXT_PLAIN : null;
            channel.basicPublish("", queueName, props, rqSer.getBytes(StandardCharsets.UTF_8));
        }
    }
}
