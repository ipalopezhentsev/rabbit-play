package ru.ipal.rabbit.publisher.controller;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

import ru.ipal.rabbit.publisher.model.LogRequest;
import ru.ipal.rabbit.publisher.model.LogRequestExtended;
import ru.ipal.rabbit.publisher.service.LogFanoutRequestPublisher;
import ru.ipal.rabbit.publisher.service.LogRoutingRequestPublisher;
import ru.ipal.rabbit.publisher.service.LogTopicRequestPublisher;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@RestController
public class LoggingController {
    @Autowired
    private LogFanoutRequestPublisher fanoutPublisher;
    @Autowired 
    private LogRoutingRequestPublisher routingPublisher;
    @Autowired 
    private LogTopicRequestPublisher topicPublisher;

    @PostMapping("logFanout")
    public void logFanout(
            @RequestParam String msg,
            @RequestParam String severity) throws IOException {
        var rq = new LogRequest(msg, severity);
        fanoutPublisher.send(rq);
    }

    @PostMapping("logRouting")
    public void logRouting(
            @RequestParam String msg,
            @RequestParam String severity) throws IOException {
        var rq = new LogRequest(msg, severity);
        routingPublisher.send(rq);
    }

    @PostMapping("logTopic")
    public void logTopic(
            @RequestParam String msg,
            @RequestParam String severity,
            @RequestParam String component) throws IOException {
        var rq = new LogRequestExtended(msg, severity, component);
        topicPublisher.send(rq);
    }
}
