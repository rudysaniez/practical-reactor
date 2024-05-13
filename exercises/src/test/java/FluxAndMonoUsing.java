import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.*;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class FluxAndMonoUsing {

    @Test
    void fromIterable() {

        List<Integer> data = List.of(1,2,3);
        Flux.fromIterable(data)
            .repeat(5)
            .subscribe(System.out::println, t -> {}, System.out::println);
    }

    @Test
    void fromStream() {

        var disposable = Flux.fromStream(Stream.of(1, 2, 3))
            .repeat(1)
            .doOnError(t -> System.out.println(t.getMessage()))
            .onErrorResume(t -> t instanceof IllegalStateException, t -> Mono.empty())
            .subscribe(System.out::println, t -> {}, System.out::println);
        Awaitility.await().until(disposable::isDisposed);

        //Lazy
        disposable = Flux.fromStream(() -> Stream.of(1,2,3))
            .repeat(1)
            .subscribe(System.out::println);
        Awaitility.await().until(disposable::isDisposed);
    }

    @DisplayName("From future, eager and lazy")
    @Test
    void fromFuture() {

        //Eager
        Mono.fromFuture(CompletableFuture.supplyAsync(() -> {
            System.out.println(" > Eager");
            return "Eager";
        }))
        .repeat(2)
        .subscribe(System.out::println, t -> {}, System.out::println);

        //Lazy
        Mono.fromFuture(() -> CompletableFuture.supplyAsync(() -> {
            System.out.println(" > Lazy");
            return "Lazy";
        }))
        .repeat(2)
        .subscribe(System.out::println, t -> {}, System.out::println);
    }

    @Test
    void defer() {

        //Lazy
        Mono.defer(() -> Mono.just(getValue()))
            .repeat(4)
            .subscribe(System.out::println, t -> {}, System.out::println);

        //Eager
        Mono.just(getValue())
            .repeat(4)
            .subscribe(System.out::println, t -> {}, System.out::println);

        Flux.defer(() -> Flux.just(1,2,3))
            .subscribe(System.out::println);
    }

    private Integer getValue() {
        System.out.println("Get value");
        return 1;
    }

    @Test
    void generate() {

        AtomicInteger counter = new AtomicInteger(1);

        //Stateless
        Flux.generate(sink -> {
            sink.next(counter.getAndIncrement());

            if(counter.get() > 5)
                sink.complete();
        })
        .repeat(2)
        .doOnTerminate(() -> counter.set(1))
        .subscribe(System.out::println, t -> {}, System.out::println);

        //Stateful
        Flux.generate(() -> 1, (state, sink) -> {
            sink.next(state);
            if(state == 5)
                sink.complete();
            return state + 1;
        })
        .subscribe(System.out::println, t -> {}, System.out::println);
    }

    @Test
    void create() {

        AtomicInteger counter = new AtomicInteger(1);
        Mono.create(sink -> sink.success(counter.getAndIncrement()))
            .repeat(4)
            .doOnTerminate(() -> counter.set(1))
            .subscribe(System.out::println, t -> {}, System.out::println);

        Flux.create(sink -> {
            IntStream.range(1, 6)
                .forEach(sink::next);
            sink.complete();
        }, FluxSink.OverflowStrategy.BUFFER)
        .subscribe(System.out::println, t -> {}, System.out::println);
    }

    @Test
    void subscribe() throws InterruptedException {

         Flux.just(1,2,3)
            .delaySubscription(Duration.ofSeconds(1))
            .subscribe(new BaseSubscriber<>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    request(1);
                }

                @Override
                protected void hookOnNext(Integer value) {
                    System.out.println(value);
                    request(1);
                }

                @Override
                protected void hookFinally(SignalType type) {
                    System.out.println(type.name());
                }
            });

         Thread.sleep(2000);
    }
}
