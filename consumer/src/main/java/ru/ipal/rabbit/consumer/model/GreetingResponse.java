package ru.ipal.rabbit.consumer.model;

import java.util.UUID;

public record GreetingResponse(UUID correlationId, String greeting) {
    
}
