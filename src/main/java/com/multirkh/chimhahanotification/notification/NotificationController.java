package com.multirkh.chimhahanotification.notification;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/notif")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationRepository notificationRepository;

    @GetMapping("/me")
    public List<NotificationDto> myNotifications(@AuthenticationPrincipal Jwt jwt) {
        String userId = jwt.getSubject();
        return notificationRepository
            .findByUserIdOrderByCreatedAtDesc(userId, PageRequest.of(0, 50))
            .stream().map(NotificationDto::from).toList();
    }

    @PatchMapping("/{id}/read")
    public ResponseEntity<Void> markRead(@PathVariable Long id, @AuthenticationPrincipal Jwt jwt) {
        return notificationRepository.findById(id)
            .filter(n -> n.getUserId().equals(jwt.getSubject()))
            .map(n -> {
                n.setRead(true);
                notificationRepository.save(n);
                return ResponseEntity.noContent().<Void>build();
            })
            .orElse(ResponseEntity.notFound().build());
    }
}
