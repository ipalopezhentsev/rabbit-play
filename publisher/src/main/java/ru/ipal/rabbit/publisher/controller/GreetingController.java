package ru.ipal.rabbit.publisher.controller;

import org.springframework.web.bind.annotation.RestController;

import ru.ipal.rabbit.publisher.model.GreetingRequest;
import ru.ipal.rabbit.publisher.model.GreetingRpcRequest;
import ru.ipal.rabbit.publisher.service.GreetingRequestPublisher;
import ru.ipal.rabbit.publisher.service.GreetingResponseConsumer;
import ru.ipal.rabbit.publisher.service.GreetingRpcRequestPublisher;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@RestController
public class GreetingController {
    @Autowired
    private GreetingRequestPublisher publishSvc;
    @Autowired
    private GreetingResponseConsumer respConsumer;
    @Autowired
    private GreetingRpcRequestPublisher publishRpcSvc;

    @PostMapping("/greet")
    public CompletableFuture<String> greet(@RequestParam String name) throws IOException, TimeoutException {
        var corrId1 = UUID.randomUUID();
        var corrId2 = UUID.randomUUID();

        var res1 = new CompletableFuture<String>();
        var res2 = new CompletableFuture<String>();
        // it's important to register before publishing!
        respConsumer.registerListener(corrId1, resp -> res1.complete(resp.greeting()));
        respConsumer.registerListener(corrId2, resp -> res2.complete(resp.greeting()));

        var rqShort = new GreetingRequest(corrId1, name, 0);
        var rqLong = new GreetingRequest(corrId2, name, 10);
        // purposefully create unsymmetric load on two workers to demonstrate effects of
        // QoS: by default rabbit assigns incoming msq to consumer at the moment it
        // enters the queue, and does it round robin among consumers, and in our
        // situation one one consumer would be much slower and will have larger queue.
        // but if we set prefetch=1, basically backpressure, it tells rabbit to assign
        // incoming message only to _FREE_ worker, i.e. which does not have unacked
        // messages. this evens out workers, if one consumer is busy with 1 long rq,
        // another consumer would process much more tasks.
        publishSvc.publishGreet(rqShort);
        publishSvc.publishGreet(rqLong);
        return res1.thenCombine(res2, (s1, s2) -> s1 + s2);
    }

    @PostMapping("/greetRpc")
    public CompletableFuture<String> greetRpc(@RequestParam String name) throws IOException, TimeoutException {
        var rqShort = new GreetingRpcRequest(name, 0);
        var rqLong = new GreetingRpcRequest(name, 10);
        var fut1 = publishRpcSvc.publishGreet(rqShort);
        var fut2 = publishRpcSvc.publishGreet(rqLong);
        return fut1.thenCombine(fut2, (s1, s2) -> s1 + s2);
    }
}
