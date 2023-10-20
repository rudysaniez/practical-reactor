import org.junit.jupiter.api.*;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

        //todo: feel free to change code as you need
        Mono<String> currentUserEmail = null;
        currentUserEmail = getCurrentUser()
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
        //todo: feel free to change code as you need
        Flux<Void> tasks = taskExecutor()
                .flatMap(m -> m);

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
        //todo: feel free to change code as you need
        Flux<Message> messageFlux = streamingService()
                .flatMapMany(flow -> flow);

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
        //todo: feel free to change code as you need
        Flux<Integer> numbers = numberService1()
                .concatWith(numberService2());

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
     * - What is difference between concatMap() and flatMap() :
     * ConcatMap keep the order, sequentially and preserving order.
     * - What is difference between concatMap() and flatMapSequential()?
     * - Why doesn't Mono have concatMap() operator?
     */
    @Test
    void task_executor_again() {
        //todo: feel free to change code as you need
        Flux<Void> tasks = taskExecutor()
                .concatMap(Function.identity());

        //don't change below this line
        StepVerifier.create(tasks)
                    .verifyComplete();

        Assertions.assertEquals(taskCounter.get(), 10);

        System.out.println("---");

        //---
        Flux<Void> tasks01 = taskExecutor()
                .flatMap(m -> m);

        StepVerifier.create(tasks01)
                .verifyComplete();

        Assertions.assertEquals(taskCounter.get(), 20);
    }

    /**
     * You are writing software for broker house. You can retrieve current stock prices by calling either
     * `getStocksGrpc()` or `getStocksRest()`.
     * Since goal is the best response time, invoke both services but use result only from the one that responds first.
     */
    @Test
    void need_for_speed() {
        //todo: feel free to change code as you need
        Flux<String> stones = Flux.firstWithValue(getStocksGrpc(), getStocksRest(), getStocksRpm());

        //don't change below this line
        StepVerifier.create(stones)
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
        //todo: feel free to change code as you need
        Flux<String> stones = getStocksLocalCache()
                .switchIfEmpty(getStocksRest());

        //don't change below this line
        StepVerifier.create(stones)
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
        //todo: feel free to change code as you need
        Flux<Message> myMail = mailBoxPrimary()
                .switchOnFirst( ((signal, messageFlux) -> {

                    final Flux<Message> out;

                    if(signal.hasValue() && signal.get().metaData.contains("spam"))
                        out = mailBoxSecondary();
                    else
                        out = messageFlux;

                    return out;
                }));

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
        //todo: feel free to change code as you need
        //autoComplete(null);
        Flux<String> suggestions = userSearchInput()
                .switchMap(this::autoComplete)
                //todo: use one operator only
                ;

        //don't change below this line
        StepVerifier.create(suggestions)
                    .expectNext("reactor project", "reactive project")
                    .verifyComplete();

        Flux<String> simpleSuggestions = userSimpleSearchInput()
                .switchMap(this::autoComplete);

        StepVerifier.create(simpleSuggestions)
                .expectNext("reactor project")
                .verifyComplete();
    }


    /**
     * Code should work, but it should also be easy to read and understand.
     * Orchestrate file writing operations in a self-explanatory way using operators like `when`,`and`,`then`...
     * If all operations have been executed successfully return boolean value `true`.
     */
    @Test
    void prettify() {
        //todo: feel free to change code as you need
        //todo: use when,and,then...

        Mono<Boolean> successful = Mono.when(openFile())
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
        //todo: feel free to change code as you need

        Flux<String> fileLines = Mono.when(openFile())
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
        //todo: feel free to change code as you need
        //Flux<String> committedTasksIds = null;
        //tasksToExecute();
        //commitTask(null);

        Flux<String> committedTasksIds = tasksToExecute()
                .concatMap(m -> m.flatMap(taskId -> commitTask(taskId)
                                                    .thenReturn(taskId))
                );

        //don't change below this line
        StepVerifier.create(committedTasksIds)
                    .expectNext("task#1", "task#2", "task#3")
                    .verifyComplete();

        Assertions.assertEquals(3, committedTasksCounter.get());

        //---
        System.out.println(" > ----- ");

        //The publisher emits elements asynchronously. The order isn't ensured.
        Flux<String> result = taskToExecuteByCreateAsync(5)
                .concatMap(flow -> flow.flatMap(taskId -> Mono.when(commitTaskWithReturn(taskId))
                                                            .thenReturn(taskId)));

        StepVerifier.create(result.take(5))
                .expectNextCount(5)
                .verifyComplete();
    }


    /**
     * News have come that Microsoft is buying Blizzard and there will be a major merger.
     * Merge two companies, so they may still produce titles in individual pace but as a single company.
     */
    @Test
    void major_merger() {
        //todo: feel free to change code as you need
        //Flux<String> microsoftBlizzardCorp = microsoftTitles().concatWith(blizzardTitles());

        Flux<String> microsoftBlizzardCorp = Flux.merge(microsoftTitles(), blizzardTitles());
        microsoftBlizzardCorp = microsoftTitles().mergeWith(blizzardTitles());

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
        //todo: feel free to change code as you need
        Flux<Car> producedCars = carChassisProducer()
                .zipWith(carEngineProducer(), Car::new);

        //don't change below this line
        StepVerifier.create(producedCars)
                    .recordWith(ConcurrentLinkedDeque::new)
                    .expectNextCount(3)
                    .expectRecordedMatches(cars -> cars.stream()
                                                       .allMatch(car -> Objects.equals(car.chassis.getSeqNum(),
                                                                                       car.engine.getSeqNum())))
                    .verifyComplete();


        //Two
        StepVerifier.create(
                Flux.zip(carChassisProducer(), carEngineProducer())
                        .map(tuple2 -> new Car(tuple2.getT1(), tuple2.getT2()))
        )
        .recordWith(CopyOnWriteArrayList::new)
        .expectNextCount(3)
        .expectRecordedMatches(cars -> cars.stream().findAny().isPresent())
        .verifyComplete();

        //Three
        Flux<Car> carFlux = Flux.zip(values -> {
            return aggregate((Chassis) values[0], (Engine) values[1]);
        },
        carChassisProducer(),
        carEngineProducer());

        StepVerifier.create(carFlux)
                .recordWith(CopyOnWriteArrayList::new)
                .expectNextCount(3)
                .expectRecordedMatches(cars -> cars.stream().findAny().isPresent())
                .verifyComplete();

    }

    private Car aggregate(Chassis c, Engine e) {
        return new Car(c, e);
    }

    /**
     * When `chooseSource()` method is used, based on current value of sourceRef, decide which source should be used.
     */

    //only read from sourceRef
    AtomicReference<String> sourceRef = new AtomicReference<>("X");

    //todo: implement this method based on instructions
    public Mono<String> chooseSource() {

        return Mono.defer(() -> {

            final Mono<String> out;

            if (sourceRef.get().equals("A"))
                out = sourceA();
            else if (sourceRef.get().equals("B"))
                out = sourceB();
            else
                out = Mono.just("X");

            return out;
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
        //BlockHound.install(); //don't change this line, blocking = cheating!

        //todo: feel free to change code as you need
        Flux<String> stream = StreamingConnection.startStreaming()
                                                 .flatMapMany(Function.identity());
        StreamingConnection.closeConnection();

        stream = Flux.usingWhen(
                StreamingConnection.startStreaming(),
                stringFlux -> stringFlux,
                x -> StreamingConnection.closeConnection()
        );

        //don't change below this line
        StepVerifier.create(stream)
                    .then(()-> Assertions.assertTrue(StreamingConnection.isOpen.get()))
                    .expectNextCount(20)
                    .verifyComplete();
        Assertions.assertTrue(StreamingConnection.cleanedUp.get());
    }
}
