package ru.ipal.rabbit.consumer.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import ru.ipal.rabbit.consumer.model.GreetingRequest;
import ru.ipal.rabbit.consumer.model.GreetingResponse;

@Service
@Slf4j
public class GreetingProcessor {
    private final String queueName;
    private final boolean isDurable;
    private final ConnectionFactory connFactory;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private GreetingResponsePublisher responsePublisher;

    public GreetingProcessor(
            @Value("${RABBIT_MQ_SERVER:}") String rabbitHost,
            @Value("${HELLO_RQ_QUEUE_NAME:}") String queueName,
            @Value("${IS_TOPIC_DURABLE:false}") boolean isDurable) {
        this.queueName = queueName;
        this.isDurable = isDurable;
        connFactory = new ConnectionFactory();
        connFactory.setHost(rabbitHost);
    }

    @PostConstruct
    public void startListening() throws IOException, TimeoutException {
        log.info("Listening for messages on queue {}", queueName);
        Connection connection = connFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(queueName, isDurable, false, false, null);
        channel.basicQos(1);
        DeliverCallback deliverCallback = new DeliverCallback() {
            @Override
            public void handle(String consumerTag, Delivery message) throws IOException {
                try {
                    var strRq = new String(message.getBody(), StandardCharsets.UTF_8);
                    var rq = objectMapper.readValue(strRq, GreetingRequest.class);
                    var resp = new GreetingResponse(rq.corrId(), "Hello " + rq.name());
                    responsePublisher.publishResponse(resp);
                    channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                } catch (IOException | TimeoutException e) {
                    log.error("Error while publishing response", e);
                    // channel.basicNack(0, false, false);
                    throw new RuntimeException("Error while publishing response", e);
                }
            }
        };
        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
        });
    }

}
