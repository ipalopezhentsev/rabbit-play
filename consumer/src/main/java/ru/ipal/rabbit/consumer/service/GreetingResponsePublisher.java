package ru.ipal.rabbit.consumer.service;

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
import ru.ipal.rabbit.RabbitMqProperties;
import ru.ipal.rabbit.consumer.model.GreetingResponse;

@Service
public class GreetingResponsePublisher {
    private final RabbitMqProperties props;
    private final ConnectionFactory connFactory;
    @Autowired
    private ObjectMapper objectMapper;
    private final Connection conn;

    public GreetingResponsePublisher(
            RabbitMqProperties props) throws IOException, TimeoutException {
        this.props = props;
        connFactory = new ConnectionFactory();
        connFactory.setHost(props.server());
        this.conn = connFactory.newConnection();
    }

    public void publishResponse(GreetingResponse resp) throws IOException, TimeoutException {
        var respSer = objectMapper.writeValueAsString(resp);
        try (var channel = conn.createChannel()) {
            channel.queueDeclare(props.helloRespQueue(), props.isDurable(), false, false, null);
            var msgProps = props.isDurable() ? MessageProperties.PERSISTENT_TEXT_PLAIN : null;
            channel.basicPublish("", props.helloRespQueue(), msgProps, respSer.getBytes(StandardCharsets.UTF_8));
        }
    }

    @PreDestroy
    public void destroy() throws IOException {
        conn.close();
    }
}
