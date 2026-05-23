package com.multirkh.chimhahanotification.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.multirkh.chimhahanotification.notification.Notification;
import com.multirkh.chimhahanotification.notification.NotificationRepository;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PostCreatedConsumerTest {

    @Mock
    private NotificationRepository notificationRepository;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @InjectMocks
    private PostCreatedConsumer consumer;

    private ConsumerRecord<String, String> record(String eventId, String eventType, String value) {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("post.events.v1", 0, 0L, "42", value);
        if (eventId != null) {
            r.headers().add(PostCreatedConsumer.HEADER_EVENT_ID, eventId.getBytes(StandardCharsets.UTF_8));
        }
        if (eventType != null) {
            r.headers().add(PostCreatedConsumer.HEADER_EVENT_TYPE, eventType.getBytes(StandardCharsets.UTF_8));
        }
        return r;
    }

    private static String payloadJson() {
        return """
            {"postId":"42","authorId":"user-uuid","categoryKey":"BEST","title":"hello","occurredAt":"2026-05-24T00:00:00Z"}
            """;
    }

    @Test
    @DisplayName("정상 흐름 → ProcessedEvent saved + Notification created for authorId")
    void happyPath() throws Exception {
        ConsumerRecord<String, String> r = record("123", "PostCreated", payloadJson());
        when(processedEventRepository.existsById("123")).thenReturn(false);

        consumer.onMessage(r);

        verify(processedEventRepository).save(any(ProcessedEvent.class));
        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        verify(notificationRepository).save(captor.capture());
        Notification saved = captor.getValue();
        assertThat(saved.getUserId()).isEqualTo("user-uuid");
        assertThat(saved.getType()).isEqualTo("PostCreated");
        assertThat(saved.getPostId()).isEqualTo("42");
        assertThat(saved.getTitle()).isEqualTo("hello");
    }

    @Test
    @DisplayName("중복 event-id → existsById true → Notification 생성 안 됨 + save도 호출 안 됨")
    void duplicateEventIdSkipsNotification() throws Exception {
        ConsumerRecord<String, String> r = record("123", "PostCreated", payloadJson());
        when(processedEventRepository.existsById("123")).thenReturn(true);

        consumer.onMessage(r);

        verify(processedEventRepository, never()).save(any());
        verifyNoInteractions(notificationRepository);
    }

    @Test
    @DisplayName("event-id header 없음 → skip, DB 쓰기 0")
    void missingEventIdHeader() throws Exception {
        ConsumerRecord<String, String> r = record(null, "PostCreated", payloadJson());

        consumer.onMessage(r);

        verifyNoInteractions(processedEventRepository);
        verifyNoInteractions(notificationRepository);
    }

    @Test
    @DisplayName("event-type 이 PostCreated 가 아니면 → skip, Notification 생성 안 함")
    void wrongEventTypeSkipped() throws Exception {
        ConsumerRecord<String, String> r = record("123", "PostDeleted", payloadJson());

        consumer.onMessage(r);

        verify(notificationRepository, never()).save(any());
    }
}
