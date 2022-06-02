package net.defmsy.reactivemqpoc.mq;

import reactor.core.publisher.Flux;

public interface ReactivePublisher {

  Flux<String> listen(String key, int size);

  void publish(String key, String message);

  void removePublisher(String key);
}
