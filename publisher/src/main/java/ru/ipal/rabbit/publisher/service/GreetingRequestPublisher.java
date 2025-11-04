package ru.ipal.rabbit.publisher.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import ru.ipal.rabbit.RabbitMqProperties;
import ru.ipal.rabbit.publisher.model.GreetingRequest;

@Service
@Slf4j
public class GreetingRequestPublisher {
    private final RabbitMqProperties props;
    private final ConnectionFactory connFactory;
    private final Connection conn;
    @Autowired
    private ObjectMapper objectMapper;

    public GreetingRequestPublisher(
            RabbitMqProperties props) throws IOException, TimeoutException {
        this.props = props;
        connFactory = new ConnectionFactory();
        connFactory.setHost(props.server());

        this.conn = connFactory.newConnection();
        try (var channel = conn.createChannel()) {
            channel.queueDeclare(props.helloRqQueue(), props.isDurable(), false, false, null);
        }
    }

    public void publishGreet(GreetingRequest rq) throws IOException, TimeoutException {
        final var rqSer = objectMapper.writeValueAsString(rq);
        try (var channel = conn.createChannel()) {
            var msgProps = props.isDurable() ? MessageProperties.PERSISTENT_TEXT_PLAIN : null;
            channel.basicPublish("", props.helloRqQueue(), msgProps, rqSer.getBytes(StandardCharsets.UTF_8));
        }
    }

    @PreDestroy
    public void destroy() throws IOException, TimeoutException {
        conn.close();
    }
}
