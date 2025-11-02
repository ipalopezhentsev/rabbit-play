package ru.ipal.rabbit.publisher.model;

public record LogRequestExtended(String msg, String severity, String component) {
} 