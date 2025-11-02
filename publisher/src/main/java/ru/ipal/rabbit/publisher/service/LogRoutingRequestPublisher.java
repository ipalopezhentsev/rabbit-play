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
import ru.ipal.rabbit.publisher.model.LogRequest;

@Service
@Slf4j
public class LogRoutingRequestPublisher {
    private final Connection conn;
    private final Channel channel;
    private final RabbitMqProperties props;
    @Autowired
    private ObjectMapper objectMapper;

    public LogRoutingRequestPublisher(
            RabbitMqProperties props) throws IOException, TimeoutException {
        this.props = props;
        var connFactory = new ConnectionFactory();
        connFactory.setHost(props.server());
        this.conn = connFactory.newConnection();
        this.channel = conn.createChannel();
        channel.exchangeDeclare(props.logRqRoutingExchange(), BuiltinExchangeType.DIRECT);
    }

    public void send(LogRequest rq) throws IOException {
        final var rqSer = objectMapper.writeValueAsString(rq);
        var p = props.isDurable() ? MessageProperties.PERSISTENT_TEXT_PLAIN : null;
        // sends to all queues bound to exchange and having bindingKey = routingKey which we publish in message
        var routingKey = rq.severity();
        channel.basicPublish(props.logRqRoutingExchange(), routingKey, p,
                rqSer.getBytes(StandardCharsets.UTF_8));
    }

    @PreDestroy
    public void destroy() throws IOException, TimeoutException {
        channel.close();
        conn.close();
    }
}
