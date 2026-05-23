package com.multirkh.chimhahanotification.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PostCreatedPayload(
    String postId,
    String authorId,
    String categoryKey,
    String title,
    Instant occurredAt
) {}
