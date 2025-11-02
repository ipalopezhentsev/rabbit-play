package ru.ipal.rabbit.consumer.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import ru.ipal.rabbit.RabbitMqProperties;
import ru.ipal.rabbit.consumer.model.LogRequestExtended;

@Service
@Slf4j
public class LogTopicRequestConsumer {
    private final RabbitMqProperties props;
    private final ConnectionFactory connFactory;
    private final Connection connection;
    private final Channel channel;
    @Autowired
    private ObjectMapper objectMapper;

    public LogTopicRequestConsumer(
            RabbitMqProperties props) throws IOException, TimeoutException {
        this.props = props;
        connFactory = new ConnectionFactory();
        connFactory.setHost(props.server());
        connection = connFactory.newConnection();
        channel = connection.createChannel();
    }

    @PostConstruct
    public void startListening() throws IOException {
        channel.exchangeDeclare(props.logRqTopicExchange(), BuiltinExchangeType.TOPIC);
        // creates random autodelete queue because we'll bind to exchange with binding key
        final String queueName = channel.queueDeclare().getQueue();
        final String[] bindingKeys = props.logRqTopicConsumerBindingKeys().split(",");
        //listen to exchange but only for messages where routingKey matchers our bindingKey
        for (String bindingKey : bindingKeys) {
            channel.queueBind(queueName, props.logRqTopicExchange(), bindingKey);
        }
        DeliverCallback deliverCallback = new DeliverCallback() {
            @Override
            public void handle(String consumerTag, Delivery message) throws IOException {
                var strRq = new String(message.getBody(), StandardCharsets.UTF_8);
                var rq = objectMapper.readValue(strRq, LogRequestExtended.class);
                log.info("{}", rq);
            }
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }

    @PreDestroy
    public void destroy() throws IOException, TimeoutException {
        channel.close();
        connection.close();
    }
}
