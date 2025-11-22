package ru.ipal.rabbit.publisher.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
import com.rabbitmq.client.AMQP.BasicProperties;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import ru.ipal.rabbit.RabbitMqProperties;
import ru.ipal.rabbit.publisher.model.GreetingRpcRequest;
import ru.ipal.rabbit.publisher.model.GreetingRpcResponse;

@Service
@Slf4j
public class GreetingRpcRequestPublisher {
    private final RabbitMqProperties props;
    private final ConnectionFactory connFactory;
    private final Connection conn;
    private final Channel channel;
    @Autowired
    private ObjectMapper objectMapper;
    private final String replyQueue;
    private Map<String, Consumer<GreetingRpcResponse>> listeners = new ConcurrentHashMap<>();

    public GreetingRpcRequestPublisher(
            RabbitMqProperties props) throws IOException, TimeoutException {
        this.props = props;
        connFactory = new ConnectionFactory();
        connFactory.setHost(props.server());

        this.conn = connFactory.newConnection();
        this.channel = conn.createChannel();
        channel.queueDeclare(props.helloRqRpcQueue(), props.isDurable(), false, false, null);
        // create temp queue for this instance which we'll set to replyTo in msg.
        // allows:
        // 1. use several publishers
        // 2. if publisher A sent a rq 1, then reply to it will be delivered to A, not
        // some B, because A may have had e.g. completable future waiting for this.
        this.replyQueue = channel.queueDeclare().getQueue();
    }

    public CompletableFuture<String> publishGreet(GreetingRpcRequest rq)
            throws IOException, TimeoutException, InterruptedException {
        var corrId = UUID.randomUUID();
        var res = new CompletableFuture<String>();
        // it's important to register before publishing!
        registerListener(corrId, resp -> res.complete(resp.greeting()));

        final var rqSer = objectMapper.writeValueAsString(rq);
        //we create channel because I think it's not thread safe.
        //also, creating it via thread local would behave badly with spring virtual threads enabled...
        try (var channel = conn.createChannel()) {
            //enable publisher confirms (i.e. that broker took the msg)
            channel.confirmSelect();
            var msgProps = new BasicProperties.Builder()
                    .deliveryMode(props.isDurable() ? 2 : 1)
                    .replyTo(replyQueue)
                    .correlationId(corrId.toString())
                    .build();
            channel.basicPublish("", props.helloRqRpcQueue(), msgProps, rqSer.getBytes(StandardCharsets.UTF_8));
            //Note: I know it's slow, want to see how much
            //channel.waitForConfirmsOrDie(5_000);
        }
        return res;
    }

    private void registerListener(UUID correlationId, Consumer<GreetingRpcResponse> consumer) {
        listeners.put(correlationId.toString(), consumer);
    }

    @PostConstruct
    public void consume() throws IOException, TimeoutException {
        channel.basicQos(1);
        DeliverCallback deliverCallback = new DeliverCallback() {
            @Override
            public void handle(String consumerTag, Delivery message) throws IOException {
                try {
                    var strResp = new String(message.getBody(), StandardCharsets.UTF_8);
                    var resp = objectMapper.readValue(strResp, GreetingRpcResponse.class);
                    BasicProperties msgProps = message.getProperties();
                    var corrId = msgProps.getCorrelationId();
                    var listener = listeners.get(corrId);
                    if (listener != null) {
                        listener.accept(resp);
                    } else {
                        log.warn("Skipping resp {} because no listener", corrId);
                    }
                    channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                } catch (IOException e) {
                    log.error("Error while getting response", e);
                    throw new RuntimeException("Error while getting response", e);
                }
            }
        };
        // this call exits immediately - that's why we cannot use try-with-resources in
        // this method on channel/connection
        channel.basicConsume(replyQueue, false, deliverCallback, consumerTag -> {
        });
    }

    @PreDestroy
    public void destroy() throws IOException, TimeoutException {
        channel.close();
        conn.close();
    }
}
