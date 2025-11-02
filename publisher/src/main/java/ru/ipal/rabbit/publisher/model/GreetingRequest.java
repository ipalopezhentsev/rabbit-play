package ru.ipal.rabbit.publisher.model;

import java.util.UUID;

public record GreetingRequest(UUID corrId, String name, int delayMs) { }
