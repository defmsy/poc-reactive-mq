package net.defmsy.reactivemqpoc.unit.mq;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import net.defmsy.reactivemqpoc.mq.ReactivePublisher;
import net.defmsy.reactivemqpoc.mq.ReactivePublisherImpl;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class ReactiveMQTest {

  private static final String KEY_A = "listener.a";
  private static final String KEY_B = "listener.b";
  private static final String KEY_C = "listener.c";

  private static final List<String> MESSAGES_A = List.of(
      "message.a.1",
      "message.a.2",
      "message.a.3"
  );
  private static final List<String> MESSAGES_B = List.of(
      "message.b.1",
      "message.b.2",
      "message.b.3"
  );
  private static final List<String> MESSAGES_C = List.of(
      "message.c.1",
      "message.c.2",
      "message.c.3"
  );

  private final ReactivePublisher reactivePublisher = new ReactivePublisherImpl();

  @Test
  void shouldReceiveMessages() {
    final var listenerReadyA = new AtomicBoolean();
    final var listenerReadyB = new AtomicBoolean();
    final var listenerReadyC = new AtomicBoolean();
    var d1 = createListenerAndAssertValuesMatchesExpectation(listenerReadyA, KEY_A, MESSAGES_A);
    var d2 = createListenerAndAssertValuesMatchesExpectation(listenerReadyB, KEY_B, MESSAGES_B);
    var d3 = createListenerAndAssertValuesMatchesExpectation(listenerReadyC, KEY_C, MESSAGES_C);
    while (!listenerReadyA.get() || !listenerReadyB.get() || !listenerReadyC.get()) {
      // Waiting for listeners to be ready
    }
    publishMessages();
    while (!d1.isDone() || !d2.isDone() || !d3.isDone()) {
      // Waiting for all messages to be published
    }
    assertThat(d1.isCompletedExceptionally()).isFalse();
    assertThat(d2.isCompletedExceptionally()).isFalse();
    assertThat(d3.isCompletedExceptionally()).isFalse();
  }

  @Test
  void shouldTimeout() {
    final var listenerReadyA = new AtomicBoolean();
    var d1 = CompletableFuture.supplyAsync(
        () -> reactivePublisher
            .listen(KEY_A, 3)
            .log()
            .doOnSubscribe(subscription -> listenerReadyA.set(true))
            .timeout(Duration.ofSeconds(3))
            .doOnError(TimeoutException.class, e -> reactivePublisher.removePublisher(KEY_A))
            .as(StepVerifier::create)
            .expectError(TimeoutException.class)
            .verify());
    while (!listenerReadyA.get()) {
      // Waiting for listeners to be ready
    }
    while (!d1.isDone()) {
      // Waiting for all messages to be published
    }
    assertThat(d1.isCompletedExceptionally()).isFalse();
  }

  private CompletableFuture<Duration> createListenerAndAssertValuesMatchesExpectation(
      final AtomicBoolean ready, String key,
      List<String> messages) {
    return CompletableFuture.supplyAsync(
        () -> reactivePublisher
            .listen(key, 3)
            .log()
            .doOnSubscribe(subscription -> ready.set(true))
            .timeout(Duration.ofSeconds(3))
            .doOnError(TimeoutException.class, e -> reactivePublisher.removePublisher(key))
            .as(StepVerifier::create)
            .expectNextSequence(messages)
            .verifyComplete());
  }

  private void publishMessages() {
    reactivePublisher.publish(KEY_C, MESSAGES_C.get(0));
    reactivePublisher.publish(KEY_A, MESSAGES_A.get(0));
    reactivePublisher.publish(KEY_A, MESSAGES_A.get(1));
    reactivePublisher.publish(KEY_B, MESSAGES_B.get(0));
    reactivePublisher.publish(KEY_A, MESSAGES_A.get(2));
    reactivePublisher.publish(KEY_C, MESSAGES_C.get(1));
    reactivePublisher.publish(KEY_B, MESSAGES_B.get(1));
    reactivePublisher.publish(KEY_B, MESSAGES_B.get(2));
    reactivePublisher.publish(KEY_C, MESSAGES_C.get(2));
  }
}
