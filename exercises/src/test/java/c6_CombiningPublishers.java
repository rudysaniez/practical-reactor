import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * In this important chapter we are going to cover different ways of combining publishers.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.values
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
class c6_CombiningPublishers extends CombiningPublishersBase {

    /**
     * Goal of this exercise is to retrieve e-mail of currently logged-in user.
     * `getCurrentUser()` method retrieves currently logged-in user
     * and `getUserEmail()` will return e-mail for given user.
     *
     * No blocking operators, no subscribe operator!
     * You may only use `flatMap()` operator.
     */
    @Test
    void behold_flatmap() {
        Hooks.enableContextLossTracking(); //used for testing - detects if you are cheating!

        Mono<String> currentUserEmail = getCurrentUser()
            .flatMap(this::getUserEmail);

        //don't change below this line
        StepVerifier.create(currentUserEmail)
            .expectNext("user123@gmail.com")
            .verifyComplete();
    }

    /**
     * `taskExecutor()` returns tasks that should execute important work.
     * Get all the tasks and execute them.
     *
     * Answer:
     * - Is there a difference between Mono.flatMap() and Flux.flatMap()?
     */
    @Test
    void task_executor() {
        Flux<Void> tasks = taskExecutor()
            .flatMap(Function.identity());

        //don't change below this line
        StepVerifier.create(tasks)
            .verifyComplete();

        Assertions.assertEquals(taskCounter.get(), 10);
    }

    /**
     * `streamingService()` opens a connection to the data provider.
     * Once connection is established you will be able to collect messages from stream.
     *
     * Establish connection and get all messages from data provider stream!
     */
    @Test
    void streaming_service() {
        Flux<Message> messageFlux = streamingService()
            .flatMapMany(Function.identity());

        //don't change below this line
        StepVerifier.create(messageFlux)
            .expectNextCount(10)
            .verifyComplete();
    }

    /**
     * Join results from services `numberService1()` and `numberService2()` end-to-end.
     * First `numberService1` emits elements and then `numberService2`. (no interleaving)
     *
     * Bonus: There are two ways to do this, check out both!
     */
    @Test
    void i_am_rubber_you_are_glue() {
        Flux<Integer> numbers = Flux.concat(numberService1(), numberService2());

        //don't change below this line
        StepVerifier.create(numbers)
            .expectNext(1, 2, 3, 4, 5, 6, 7)
            .verifyComplete();
    }

    /**
     * Similar to previous task:
     *
     * `taskExecutor()` returns tasks that should execute important work.
     * Get all the tasks and execute each of them.
     *
     * Instead of flatMap() use concatMap() operator.
     *
     * Answer:
     * - What is difference between concatMap() and flatMap()?
     * - What is difference between concatMap() and flatMapSequential()?
     * - Why doesn't Mono have concatMap() operator?
     */
    @Test
    void task_executor_again() {
        //The order is ensured with concatMap
        Flux<Void> tasks = taskExecutor().concatMap(m -> m);

        //don't change below this line
        StepVerifier.create(tasks)
            .verifyComplete();

        Assertions.assertEquals(taskCounter.get(), 10);
    }

    /**
     * You are writing software for broker house. You can retrieve current stock prices by calling either
     * `getStocksGrpc()` or `getStocksRest()`.
     * Since goal is the best response time, invoke both services but use result only from the one that responds first.
     */
    @Test
    void need_for_speed() {
        Flux<String> stocks = Flux.firstWithValue(List.of(getStocksGrpc(), getStocksRest()));

        //don't change below this line
        StepVerifier.create(stocks)
            .expectNextCount(5)
            .verifyComplete();
    }

    /**
     * As part of your job as software engineer for broker house, you have also introduced quick local cache to retrieve
     * stocks from. But cache may not be formed yet or is empty. If cache is empty, switch to a live source:
     * `getStocksRest()`.
     */
    @Test
    void plan_b() {
        Flux<String> stocks = getStocksLocalCache()
            .switchIfEmpty(getStocksRest());

        //don't change below this line
        StepVerifier.create(stocks)
            .expectNextCount(6)
            .verifyComplete();

        Assertions.assertTrue(localCacheCalled.get());
    }

    /**
     * You are checking mail in your mailboxes. Check first mailbox, and if first message contains spam immediately
     * switch to a second mailbox. Otherwise, read all messages from first mailbox.
     */
    @Test
    void mail_box_switcher() {
        Flux<Message> myMail = mailBoxPrimary()
            .switchOnFirst((signal, flux) -> {
                if(signal.isOnNext() && !signal.get().metaData.contains("spam")) {
                    return flux;
                }
                return mailBoxSecondary();
            });

        //don't change below this line
        StepVerifier.create(myMail)
            .expectNextMatches(m -> !m.metaData.equals("spam"))
            .expectNextMatches(m -> !m.metaData.equals("spam"))
            .verifyComplete();

        Assertions.assertEquals(1, consumedSpamCounter.get());
    }

    /**
     * You are implementing instant search for software company.
     * When user types in a text box results should appear in near real-time with each keystroke.
     *
     * Call `autoComplete()` function for each user input
     * but if newer input arrives, cancel previous `autoComplete()` call and call it for latest input.
     */
    @Test
    void instant_search() {
        Flux<String> suggestions = userSearchInput()
            .switchMap(this::autoComplete);

        //don't change below this line
        StepVerifier.create(suggestions)
            .expectNext("reactor project", "reactive project", "RX programming project")
            .verifyComplete();
    }


    /**
     * Code should work, but it should also be easy to read and understand.
     * Orchestrate file writing operations in a self-explanatory way using operators like `when`,`and`,`then`...
     * If all operations have been executed successfully return boolean value `true`.
     */
    @Test
    void prettify() {
        Mono<Boolean> successful = Mono.when(checkSystem(), openFile())
            .then(writeToFile("0x3522285912341"))
            .then(closeFile())
            .thenReturn(true);

        //don't change below this line
        StepVerifier.create(successful)
            .expectNext(true)
            .verifyComplete();

        Assertions.assertTrue(fileOpened.get());
        Assertions.assertTrue(writtenToFile.get());
        Assertions.assertTrue(fileClosed.get());
    }

    /**
     * Before reading from a file we need to open file first.
     */
    @Test
    void one_to_n() {
        Flux<String> fileLines = Mono.when(openFile(), checkSystem())
            .thenMany(readFile());

        StepVerifier.create(fileLines)
            .expectNext("0x1", "0x2", "0x3")
            .verifyComplete();
    }

    /**
     * Execute all tasks sequentially and after each task have been executed, commit task changes. Don't lose id's of
     * committed tasks, they are needed to further processing!
     */
    @Test
    void acid_durability() {
        Flux<String> committedTasksIds = tasksToExecute()
            .concatMap(mTask -> mTask
                .flatMap(id -> commitTask(id)
                    .thenReturn(id)
                )
            );

        //don't change below this line
        StepVerifier.create(committedTasksIds)
                    .expectNext("task#1", "task#2", "task#3")
                    .verifyComplete();

        Assertions.assertEquals(3, committedTasksCounter.get());
    }


    /**
     * News have come that Microsoft is buying Blizzard and there will be a major merger.
     * Merge two companies, so they may still produce titles in individual pace but as a single company.
     */
    @Test
    void major_merger() {
        Flux<String> microsoftBlizzardCorp = Flux.merge(5, microsoftTitles(), blizzardTitles());

        //don't change below this line
        StepVerifier.create(microsoftBlizzardCorp)
            .expectNext("windows12",
                        "wow2",
                        "bing2",
                        "overwatch3",
                        "office366",
                        "warcraft4")
            .verifyComplete();
    }


    /**
     * Your job is to produce cars. To produce car you need chassis and engine that are produced by a different
     * manufacturer. You need both parts before you can produce a car.
     * Also, engine factory is located further away and engines are more complicated to produce, therefore it will be
     * slower part producer.
     * After both parts arrive connect them to a car.
     */
    @Test
    void car_factory() {

        Flux<Car> producedCars = carChassisProducer()
            .zipWith(carEngineProducer())
            .map(tuple -> new Car(tuple.getT1(), tuple.getT2()));

        //don't change below this line
        StepVerifier.create(producedCars)
            .recordWith(ConcurrentLinkedDeque::new)
            .expectNextCount(3)
            .expectRecordedMatches(cars -> cars.stream()
                                               .allMatch(car -> Objects.equals(car.chassis.getSeqNum(),
                                                                               car.engine.getSeqNum())))
            .verifyComplete();
    }

    /**
     * When `chooseSource()` method is used, based on current value of sourceRef, decide which source should be used.
     */

    //only read from sourceRef
    AtomicReference<String> sourceRef = new AtomicReference<>("X");

    public Mono<String> chooseSource() {

        return Mono.defer(() -> {
           if(sourceRef.get().equals("A")) {
               return sourceA();
           }
           else if(sourceRef.get().equals("B")) {
                return sourceB();
           }
           return Mono.empty();
        });
    }

    @Test
    void deterministic() {
        //don't change below this line
        Mono<String> source = chooseSource();

        sourceRef.set("A");
        StepVerifier.create(source)
                    .expectNext("A")
                    .verifyComplete();

        sourceRef.set("B");
        StepVerifier.create(source)
                    .expectNext("B")
                    .verifyComplete();
    }

    /**
     * Sometimes you need to clean up after your self.
     * Open a connection to a streaming service and after all elements have been consumed,
     * close connection (invoke closeConnection()), without blocking.
     *
     * This may look easy...
     */
    @Test
    void cleanup() {

        Flux<String> stream = Flux.usingWhen(StreamingConnection.startStreaming(),
        f -> f,
        f -> StreamingConnection.closeConnection()
        );

        //don't change below this line
        StepVerifier.create(stream)
                    .then(()-> Assertions.assertTrue(StreamingConnection.isOpen.get()))
                    .expectNextCount(20)
                    .verifyComplete();
        Assertions.assertTrue(StreamingConnection.cleanedUp.get());
    }
}
