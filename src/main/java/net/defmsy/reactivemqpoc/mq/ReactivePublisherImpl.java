package net.defmsy.reactivemqpoc.mq;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.SimpleJmsListenerContainerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Component
public class ReactivePublisherImpl implements ReactivePublisher {

  public static final Logger log = LoggerFactory.getLogger(ReactivePublisherImpl.class);

  private final Map<String, Publisher> publishers = new ConcurrentHashMap<>();
  private final SimpleJmsListenerContainerFactory listenerContainerFactory;

  public ReactivePublisherImpl() {
    this.listenerContainerFactory = new SimpleJmsListenerContainerFactory();
    this
  }

  @JmsListener(destination = "destination", containerFactory = "ibmFactory")
  public void receive(String message) {

  }

  @Override
  public Flux<String> listen(String key, int size) {
    log.info("Listener requested: key='{}', size='{}'", key, size);
    if (!publishers.containsKey(key)) {
      Publisher publisher = new Publisher(size);
      publishers.put(key, publisher);
    }
    return publishers.get(key).getListener();
  }

  @Override
  public void publish(String key, String message) {
    log.info("Publish message: key='{}', message='{}'", key, message);
    if (this.publishers.get(key).publish(message)) {
      this.publishers.remove(key);
    }
  }

  @Override
  public void removePublisher(String key) {
    this.publishers.get(key).close();
    this.publishers.remove(key);
  }

  public static class Publisher {

    private final int size;
    private final Sinks.Many<String> sinks = Sinks.many().unicast().onBackpressureBuffer();

    private int counter = 0;

    public Publisher(int size) {
      this.size = size;
    }

    public Flux<String> getListener() {
      return this.sinks.asFlux();
    }

    public void close() {
      this.sinks.tryEmitComplete();
    }

    public boolean publish(String message) {
      this.counter++;
      this.sinks.tryEmitNext(message);
      if (this.counter == this.size) {
        this.sinks.tryEmitComplete();
        return true;
      }
      return false;
    }
  }
}
