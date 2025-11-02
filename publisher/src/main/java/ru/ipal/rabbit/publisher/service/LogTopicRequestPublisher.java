package ru.ipal.rabbit.publisher.service;

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
import com.rabbitmq.client.MessageProperties;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import ru.ipal.rabbit.RabbitMqProperties;
import ru.ipal.rabbit.publisher.model.LogRequestExtended;

@Service
@Slf4j
public class LogTopicRequestPublisher {
    private final Connection conn;
    private final Channel channel;
    private final RabbitMqProperties props;
    @Autowired
    private ObjectMapper objectMapper;

    public LogTopicRequestPublisher(
            RabbitMqProperties props) throws IOException, TimeoutException {
        this.props = props;
        var connFactory = new ConnectionFactory();
        connFactory.setHost(props.server());
        this.conn = connFactory.newConnection();
        this.channel = conn.createChannel();
        channel.exchangeDeclare(props.logRqTopicExchange(), BuiltinExchangeType.TOPIC);
    }

    public void send(LogRequestExtended rq) throws IOException {
        final var rqSer = objectMapper.writeValueAsString(rq);
        var p = props.isDurable() ? MessageProperties.PERSISTENT_TEXT_PLAIN : null;
        // sends to all queues bound to exchange and having bindingKey to which routingKey in our msg matches
        var routingKey = rq.severity() + "." + rq.component();
        channel.basicPublish(props.logRqTopicExchange(), routingKey, p,
                rqSer.getBytes(StandardCharsets.UTF_8));
    }

    @PreDestroy
    public void destroy() throws IOException, TimeoutException {
        channel.close();
        conn.close();
    }
}
