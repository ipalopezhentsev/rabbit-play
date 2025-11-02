package ru.ipal.rabbit.consumer.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

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
import ru.ipal.rabbit.consumer.model.GreetingRequest;
import ru.ipal.rabbit.consumer.model.GreetingResponse;

@Service
@Slf4j
public class GreetingRequestConsumer {
    private final RabbitMqProperties props;
    private final ConnectionFactory connFactory;
    private final Connection connection;
    private final Channel channel;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private GreetingResponsePublisher responsePublisher;

    public GreetingRequestConsumer(
            RabbitMqProperties props) throws IOException, TimeoutException {
        this.props = props;
        connFactory = new ConnectionFactory();
        connFactory.setHost(props.server());
        connection = connFactory.newConnection();
        channel = connection.createChannel();
    }

    @PostConstruct
    public void startListening() throws IOException, TimeoutException {
        log.info("Listening for messages on queue {}", props.helloRqQueue());
        channel.queueDeclare(props.helloRqQueue(), props.isDurable(), false, false, null);
        channel.basicQos(1);
        DeliverCallback deliverCallback = new DeliverCallback() {
            @Override
            public void handle(String consumerTag, Delivery message) throws IOException {
                try {
                    var strRq = new String(message.getBody(), StandardCharsets.UTF_8);
                    var rq = objectMapper.readValue(strRq, GreetingRequest.class);
                    Thread.sleep(rq.delayMs());
                    var resp = new GreetingResponse(rq.corrId(), "Hello " + rq.name());
                    responsePublisher.publishResponse(resp);
                    channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                } catch (IOException | TimeoutException e) {
                    log.error("Error while publishing response", e);
                    // channel.basicNack(0, false, false);
                    throw new RuntimeException("Error while publishing response", e);
                } catch (InterruptedException e) {
                    log.error("interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }
        };
        channel.basicConsume(props.helloRqQueue(), false, deliverCallback, consumerTag -> {
        });
    }

    @PreDestroy
    public void destroy() throws IOException, TimeoutException {
        channel.close();
        connection.close();
    }
}
