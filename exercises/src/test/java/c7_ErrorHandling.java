import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * It's time introduce some resiliency by recovering from unexpected events!
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.errors
 * https://projectreactor.io/docs/core/release/reference/#error.handling
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
class c7_ErrorHandling extends ErrorHandlingBase {

    /**
     * You are monitoring hearth beat signal from space probe. Heart beat is sent every 1 second.
     * Raise error if probe does not any emit heart beat signal longer then 3 seconds.
     * If error happens, save it in `errorRef`.
     */
    @Test
    void houston_we_have_a_problem() {
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        Flux<String> heartBeat = probeHeartBeatSignal()
                .timeout(Duration.ofSeconds(3))
                .doOnError(errorRef::set);

        StepVerifier.create(heartBeat)
                    .expectNextCount(3)
                    .expectError(TimeoutException.class)
                    .verify();

        Assertions.assertTrue(errorRef.get() instanceof TimeoutException);
    }

    /**
     * Retrieve currently logged user.
     * If any error occurs, exception should be further propagated as `SecurityException`.
     * Keep original cause.
     */
    @Test
    void potato_potato() {
        Mono<String> currentUser = getCurrentUser()
                .onErrorMap(SecurityException::new);

        StepVerifier.create(currentUser)
                    .expectErrorMatches(e -> e instanceof SecurityException &&
                            e.getCause().getMessage().equals("No active session, user not found!"))
                    .verify();
    }

    /**
     * Consume all the messages `messageNode()`.
     * Ignore any failures, and if error happens finish consuming silently without propagating any error.
     */
    @Test
    void under_the_rug() {
        Flux<String> messages = messageNode()
                .onErrorResume(t -> Mono.empty());

        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2")
                    .verifyComplete();

        /*
        Other example with the onErrorContinue operator.
        In the middle of the flow, an exception is thrown.
        I use the onErrorContinue operator to continue and retrieve the end of the flow.
        If i use the onErrorResume operator, i will not receive the end of the flow, when the error is thrown, the flow is stopped.
         */
        messages = messageNode01()
                .onErrorContinue((t,o) -> System.out.println(" > Error detected " + t.getMessage()));
        System.out.println(messages.collectList().block());
    }

    /**
     * Retrieve all the messages `messageNode()`,and if node suddenly fails
     * use `backupMessageNode()` to consume the rest of the messages.
     */
    @Test
    void have_a_backup() {

        Flux<String> messages = messageNode()
                .onErrorResume(t -> backupMessageNode());

        //don't change below this line
        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2", "0x3", "0x4")
                    .verifyComplete();
    }

    /**
     * Consume all the messages `messageNode()`, if node suddenly fails report error to `errorReportService` then
     * propagate error downstream.
     */
    @Test
    void error_reporter() {

        Flux<String> messages = messageNode()
                .onErrorResume(t -> {
                    return errorReportService(t)
                            .then(Mono.error(t));
                });

        StepVerifier.create(messages)
                    .expectNext("0x1", "0x2")
                    .expectError(RuntimeException.class)
                    .verify();
        Assertions.assertTrue(errorReported.get());
    }

    /**
     * Execute all tasks from `taskQueue()`. If task executes
     * without any error, commit task changes, otherwise rollback task changes.
     * Do don't propagate any error downstream.
     */
    @Test
    void unit_of_work() {
        Flux<Task> taskFlux = taskQueue()
                .concatMap(task -> task.execute()
                                    .then(task.commit())
                                    .onErrorResume(task::rollback)
                                    .thenReturn(task)
                );

        StepVerifier.create(taskFlux)
                    .expectNextMatches(task -> task.executedExceptionally.get() && !task.executedSuccessfully.get())
                    .expectNextMatches(task -> !task.executedExceptionally.get() && task.executedSuccessfully.get())
                    .verifyComplete();
    }

    /**
     * `getFilesContent()` should return files content from three different files. But one of the files may be
     * corrupted and will throw an exception if opened.
     * Using `onErrorContinue()` skip corrupted file and get the content of the other files.
     */
    @Test
    void billion_dollar_mistake() {
        Flux<String> content = getFilesContent()
                .flatMap(Function.identity())
                .onErrorContinue((t,o) -> System.out.println(" > Error occurred while reading file: " + t.getMessage()));

        StepVerifier.create(content)
                    .expectNext("file1.txt content", "file3.txt content")
                    .verifyComplete();
    }

    @Test
    void billion_dollar_mistakePlusOne() {
        Flux<String> content = getFilesContent()
                .flatMap(file -> file.onErrorResume(t -> Mono.empty()))
                .onErrorContinue((t,o) -> System.out.println("Error continue"))
                ;

        StepVerifier.create(content)
                .expectNext("file1.txt content", "file3.txt content")
                .verifyComplete();
    }

    /**
     * Quote from one of creators of Reactor: onErrorContinue is my billion-dollar mistake. `onErrorContinue` is
     * considered as a bad practice, its unsafe and should be avoided.
     *
     * {@see <a href="https://nurkiewicz.com/2021/08/onerrorcontinue-reactor.html">onErrorContinue</a>} {@see <a
     * href="https://devdojo.com/ketonemaniac/reactor-onerrorcontinue-vs-onerrorresume">onErrorContinue vs
     * onErrorResume</a>} {@see <a href="https://bsideup.github.io/posts/daily_reactive/where_is_my_exception/">Where is
     * my exception?</a>}
     *
     * Your task is to implement `onErrorContinue()` behaviour using `onErrorResume()` operator,
     * by using knowledge gained from previous lessons.
     */
    @Test
    void resilience() {

        Flux<String> content = getFilesContent()
                .flatMap(file -> file.doOnError(t -> System.out.println("Error occurred while reading file: " + t.getMessage()))
                        .onErrorResume(t -> Mono.empty()));

        StepVerifier.create(content)
                .expectNext("file1.txt content", "file3.txt content")
                .verifyComplete();
    }

    @Test
    void resiliencePlusOne() {

        Flux<String> content = getFilesContentWithDownstreamOnErrorResume()
                .onErrorContinue((t,o) -> System.out.println("On error continue"))
                .flatMap(file -> file);

        StepVerifier.create(content)
                .expectNext("file1.txt content", "file3.txt content")
                .verifyComplete();
    }

    @Test
    void resiliencePlusTwo() {

        var disposable = Flux.range(1, 5)
                .doOnNext(i -> System.out.println("input=" + i))
                .flatMap(i -> Mono.just(i)
                        .<Integer>handle((n, sink) -> {
                            if(n == 2) sink.next(n / 0);
                            else sink.next(n * 2);
                        })
                        .onErrorResume(t -> {
                            System.out.println("On error resume");
                            return Mono.empty();
                        })
                        .onErrorStop()
                )
                .onErrorContinue((t,o) -> System.out.println("On error continue"))
                .reduce(Integer::sum)
                .doOnNext(System.out::println)
                .subscribe();

        Awaitility.await().until(disposable::isDisposed);
    }

    /**
     * You are trying to read temperature from your recently installed DIY IoT temperature sensor. Unfortunately, sensor
     * is cheaply made and may not return value on each read. Keep retrying until you get a valid value.
     */
    @Test
    void its_hot_in_here() {
        Mono<Integer> temperature = temperatureSensor()
                .retry();

        StepVerifier.create(temperature)
                    .expectNext(34)
                    .verifyComplete();
    }

    /**
     * In following example you are trying to establish connection to database, which is very expensive operation.
     * You may retry to establish connection maximum of three times, so do it wisely!
     * FIY: database is temporarily down, and it will be up in few seconds (5).
     */
    @Test
    void back_off() {
        Mono<String> connection_result = establishConnection()
                .retryWhen(RetrySpec.backoff(3, Duration.ofSeconds(2)));

        StepVerifier.create(connection_result)
                    .expectNext("connection_established")
                    .verifyComplete();
    }

    /**
     * You are working with legacy system in which you need to read alerts by pooling SQL table. Implement polling
     * mechanism by invoking `nodeAlerts()` repeatedly until you get all (2) alerts. If you get empty result, delay next
     * polling invocation by 1 second.
     */
    @Test
    void good_old_polling() {

        Flux<String> alerts = nodeAlerts()
                .repeatWhenEmpty(i -> i.delayElements(Duration.ofMillis(1000)))
                .repeat();

        StepVerifier.create(alerts.take(2))
                    .expectNext("node1:low_disk_space", "node1:down")
                    .verifyComplete();
    }

    @Test
    void good_old_pollingV2() {

        Flux<String> alerts;

        AtomicInteger c1 = new AtomicInteger(0);

        alerts = Flux.interval(Duration.ofMillis(1000))
                .flatMap(i -> nodeAlerts()
                        .<String>handle((s, sink) -> {
                            c1.incrementAndGet();
                            sink.next(s);
                        })
                )
                .takeUntil(s -> c1.get() == 2);

        StepVerifier.create(alerts.take(2))
                .expectNext("node1:low_disk_space", "node1:down")
                .verifyComplete();
    }

    public static class SecurityException extends Exception {

        public SecurityException(Throwable cause) {
            super(cause);
        }
    }
}
