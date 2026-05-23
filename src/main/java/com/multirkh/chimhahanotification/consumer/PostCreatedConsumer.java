package com.multirkh.chimhahanotification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.multirkh.chimhahanotification.notification.Notification;
import com.multirkh.chimhahanotification.notification.NotificationRepository;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consumes post.events.v1, creates a notification for the post author.
 * Idempotent via processed_event(event_id PK) + existsById guard.
 *
 * Single-instance assumption: existsById is a separate query from save, so
 * two consumer instances of this group could race between check and insert.
 * Phase 2 will switch to a true insert-or-skip mechanism (Persistable or
 * native INSERT IGNORE) when we run multi-instance services.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PostCreatedConsumer {

    public static final String CONSUMER_GROUP = "chimhaha-notification";
    public static final String HEADER_EVENT_ID = "event-id";
    public static final String HEADER_EVENT_TYPE = "event-type";

    private final NotificationRepository notificationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "post.events.v1", groupId = CONSUMER_GROUP)
    @Transactional
    public void onMessage(ConsumerRecord<String, String> record) throws Exception {
        String eventId = headerString(record, HEADER_EVENT_ID);
        String eventType = headerString(record, HEADER_EVENT_TYPE);

        if (eventId == null) {
            log.warn("[notif-consumer] missing event-id header, skipping. topic={} offset={}",
                record.topic(), record.offset());
            return;
        }
        if (!"PostCreated".equals(eventType)) {
            log.debug("[notif-consumer] ignoring eventType={}", eventType);
            return;
        }

        if (processedEventRepository.existsById(eventId)) {
            log.info("[notif-consumer] duplicate event-id={}, skipping", eventId);
            return;
        }

        PostCreatedPayload payload = objectMapper.readValue(record.value(), PostCreatedPayload.class);
        processedEventRepository.save(new ProcessedEvent(eventId, CONSUMER_GROUP));
        Notification n = new Notification(
            payload.authorId(),
            "PostCreated",
            payload.postId(),
            payload.title(),
            "Your post '" + payload.title() + "' was published."
        );
        notificationRepository.save(n);
        log.info("[notif-consumer] created notification id={} for user={} event-id={}",
            n.getId(), payload.authorId(), eventId);
    }

    private static String headerString(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
