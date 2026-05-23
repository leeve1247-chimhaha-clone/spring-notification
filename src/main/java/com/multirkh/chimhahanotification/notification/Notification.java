package com.multirkh.chimhahanotification.notification;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter
@NoArgsConstructor
@Table(name = "notification")
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false, length = 128)
    private String userId;

    @Column(name = "type", nullable = false, length = 64)
    private String type;

    @Column(name = "post_id", length = 64)
    private String postId;

    @Column(name = "title", length = 255)
    private String title;

    @Column(name = "body", length = 512)
    private String body;

    @Setter
    @Column(name = "is_read", nullable = false)
    private boolean isRead;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    public Notification(String userId, String type, String postId, String title, String body) {
        this.userId = userId;
        this.type = type;
        this.postId = postId;
        this.title = title;
        this.body = body;
        this.isRead = false;
        this.createdAt = Instant.now();
    }
}
