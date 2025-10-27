package ru.ipal.rabbit.publisher.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

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
import ru.ipal.rabbit.publisher.model.GreetingResponse;

@Service
@Slf4j
public class GreetingResponseConsumer {
    private final String queueName;
    private final ConnectionFactory connFactory;
    @Autowired
    private ObjectMapper objectMapper;
    private Map<UUID, Consumer<GreetingResponse>> listeners = new ConcurrentHashMap<>();

    public GreetingResponseConsumer(
            @Value("${RABBIT_MQ_SERVER:}") String rabbitHost,
            @Value("${HELLO_RESP_QUEUE_NAME:}") String queueName) {
        this.queueName = queueName;
        connFactory = new ConnectionFactory();
        connFactory.setHost(rabbitHost);
    }

    public void registerListener(UUID correlationId, Consumer<GreetingResponse> consumer) {
        listeners.put(correlationId, consumer);
    }

    @PostConstruct
    public void consume() throws IOException, TimeoutException {
        Connection connection = connFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(queueName, false, false, false, null);
        DeliverCallback deliverCallback = new DeliverCallback() {
            @Override
            public void handle(String consumerTag, Delivery message) throws IOException {
                try {
                    var strResp = new String(message.getBody(), StandardCharsets.UTF_8);
                    var resp = objectMapper.readValue(strResp, GreetingResponse.class);
                    var listener = listeners.get(resp.correlationId());
                    if (listener != null) {
                        listener.accept(resp);
                    } else {
                        log.warn("Skipping resp {} because no listener", resp.correlationId());
                    }
                    // channel.basicAck(deliveryTag, false);
                } catch (IOException e) {
                    log.error("Error while getting response", e);
                    // channel.basicNack(0, false, false);
                    throw new RuntimeException("Error while getting response", e);
                }
            }
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });

    }
}
