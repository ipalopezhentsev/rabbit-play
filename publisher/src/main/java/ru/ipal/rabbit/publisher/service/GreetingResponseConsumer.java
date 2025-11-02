package ru.ipal.rabbit.publisher.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import ru.ipal.rabbit.RabbitMqProperties;
import ru.ipal.rabbit.publisher.model.GreetingResponse;

@Service
@Slf4j
public class GreetingResponseConsumer {
    private final RabbitMqProperties props;
    private final ConnectionFactory connFactory;
    private final Connection connection;
    private final Channel channel;
    @Autowired
    private ObjectMapper objectMapper;
    private Map<UUID, Consumer<GreetingResponse>> listeners = new ConcurrentHashMap<>();

    public GreetingResponseConsumer(
            RabbitMqProperties props) throws IOException, TimeoutException {
        this.props = props;
        connFactory = new ConnectionFactory();
        connFactory.setHost(props.server());
        connection = connFactory.newConnection();
        channel = connection.createChannel();
    }

    public void registerListener(UUID correlationId, Consumer<GreetingResponse> consumer) {
        listeners.put(correlationId, consumer);
    }

    @PostConstruct
    public void consume() throws IOException, TimeoutException {
        channel.queueDeclare(props.helloRespQueue(), props.isDurable(), false, false, null);
        channel.basicQos(1);
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
                    channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                } catch (IOException e) {
                    log.error("Error while getting response", e);
                    // channel.basicNack(0, false, false);
                    throw new RuntimeException("Error while getting response", e);
                }
            }
        };
        //this call exits immediately - that's why we cannot use try-with-resources in this method
        //on channel/connection
        channel.basicConsume(props.helloRespQueue(), false, deliverCallback, consumerTag -> {
        });
    }

    @PreDestroy
    public void destroy() throws IOException, TimeoutException {
        channel.close();
        connection.close();
    }
}
