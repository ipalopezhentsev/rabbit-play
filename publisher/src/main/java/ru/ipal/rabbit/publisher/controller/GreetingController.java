package ru.ipal.rabbit.publisher.controller;

import org.springframework.web.bind.annotation.RestController;

import ru.ipal.rabbit.publisher.model.GreetingRequest;
import ru.ipal.rabbit.publisher.service.GreetingRequestPublisher;
import ru.ipal.rabbit.publisher.service.GreetingResponseConsumer;

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

    @PostMapping("/greet")
    public CompletableFuture<String> greet(@RequestParam String name) throws IOException, TimeoutException {
        var corrId = UUID.randomUUID();

        var res = new CompletableFuture<String>();
        //it's important to register before publishing!
        respConsumer.registerListener(corrId, resp -> res.complete(resp.greeting()));

        var rqShort = new GreetingRequest(corrId, name, 0);
        var rqLong = new GreetingRequest(corrId, name, 10);
        //purposefully create unsymmetric load on two workers to demonstrate effects of QoS:
        //by default rabbit assigns incoming msq to consumer at the moment it enters the queue,
        //and does it round robin among consumers, and in our situation one one consumer would
        //be much slower and will have larger queue.
        //but if we set prefetch=1, basically backpressure, it tells rabbit to assign incoming
        //message only to _FREE_ worker, i.e. which does not have unacked messages. this evens
        //out workers, if one consumer is busy with 1 long rq, another consumer would process
        //much more tasks.
        publishSvc.publishGreet(rqShort);
        publishSvc.publishGreet(rqLong);
        return res;
    }
    
}
