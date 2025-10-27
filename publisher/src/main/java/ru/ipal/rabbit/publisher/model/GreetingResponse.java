package ru.ipal.rabbit.publisher.model;

import java.util.UUID;

public record GreetingResponse(UUID correlationId, String greeting) {
    
}
