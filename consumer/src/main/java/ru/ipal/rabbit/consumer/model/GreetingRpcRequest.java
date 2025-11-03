package ru.ipal.rabbit.consumer.model;

public record GreetingRpcRequest(String name, int delayMs) {

}
