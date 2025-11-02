package ru.ipal.rabbit.consumer.model;

public record LogRequestExtended(String msg, String severity, String component) {
} 