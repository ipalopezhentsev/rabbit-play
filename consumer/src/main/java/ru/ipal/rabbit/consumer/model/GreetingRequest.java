package ru.ipal.rabbit.consumer.model;

import java.util.UUID;

public record GreetingRequest(UUID corrId, String name) { }
