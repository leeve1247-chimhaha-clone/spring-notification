package com.multirkh.chimhahanotification.notification;

import java.time.Instant;

public record NotificationDto(
    Long id,
    String type,
    String postId,
    String title,
    String body,
    boolean isRead,
    Instant createdAt
) {
    public static NotificationDto from(Notification n) {
        return new NotificationDto(
            n.getId(), n.getType(), n.getPostId(),
            n.getTitle(), n.getBody(), n.isRead(), n.getCreatedAt()
        );
    }
}
