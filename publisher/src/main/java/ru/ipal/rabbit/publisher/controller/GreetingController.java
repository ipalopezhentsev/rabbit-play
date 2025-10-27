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

        var rq = new GreetingRequest(corrId, name);
        publishSvc.publishGreet(rq);
        return res;
    }
    
}
