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
import com.rabbitmq.client.AMQP.BasicProperties;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import ru.ipal.rabbit.RabbitMqProperties;
import ru.ipal.rabbit.consumer.model.GreetingRpcRequest;
import ru.ipal.rabbit.consumer.model.GreetingRpcResponse;

@Service
@Slf4j
public class GreetingRpcRequestConsumer {
    private final RabbitMqProperties props;
    private final ConnectionFactory connFactory;
    private final Connection connection;
    private final Channel channel;
    @Autowired
    private ObjectMapper objectMapper;

    public GreetingRpcRequestConsumer(
            RabbitMqProperties props) throws IOException, TimeoutException {
        this.props = props;
        connFactory = new ConnectionFactory();
        connFactory.setHost(props.server());
        connection = connFactory.newConnection();
        channel = connection.createChannel();
    }

    @PostConstruct
    public void startListening() throws IOException, TimeoutException {
        log.info("Listening for messages on queue {}", props.helloRqRpcQueue());
        channel.queueDeclare(props.helloRqRpcQueue(), props.isDurable(), false, false, null);
        channel.basicQos(1);
        DeliverCallback deliverCallback = new DeliverCallback() {
            @Override
            public void handle(String consumerTag, Delivery message) throws IOException {
                try {
                    var strRq = new String(message.getBody(), StandardCharsets.UTF_8);
                    var rq = objectMapper.readValue(strRq, GreetingRpcRequest.class);
                    Thread.sleep(rq.delayMs());
                    var resp = new GreetingRpcResponse("Hello " + rq.name());
                    var msgProps = message.getProperties();
                    var replyTo = msgProps.getReplyTo();
                    var corrId = msgProps.getCorrelationId();
                    var respSer = objectMapper.writeValueAsString(resp);
                    var replyMsgProps = new BasicProperties.Builder()
                            .deliveryMode(props.isDurable() ? 2 : 1)
                            .correlationId(corrId.toString())
                            .build();
                    channel.basicPublish("", replyTo, replyMsgProps, respSer.getBytes(StandardCharsets.UTF_8));
                    channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                } catch (IOException e) {
                    log.error("Error while publishing response", e);
                    throw new RuntimeException("Error while publishing response", e);
                } catch (InterruptedException e) {
                    log.error("interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }
        };
        channel.basicConsume(props.helloRqRpcQueue(), false, deliverCallback, consumerTag -> {
        });
    }

    @PreDestroy
    public void destroy() throws IOException, TimeoutException {
        channel.close();
        connection.close();
    }
}
