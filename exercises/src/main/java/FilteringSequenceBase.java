import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Stefan Dragisic
 * @author rudysaniez
 */
public class FilteringSequenceBase {

    public Flux<String> popular_girl_names_service() {
        return Flux.just("Olivia", "Emma", "Ava", "Charlotte", "Sophia",
                         "Amelia", "Isabella", "Mia", "Evelyn", "Harper",
                         "Camila", "Gianna", "Abigail", "Luna", "Ella");
    }


    public Flux<Object> mashed_data_service() {
        return Flux.just("1", new LinkedList<String>(), new AtomicReference<String>(), "String.class", String.class);
    }

    public Flux<String> duplicated_records_service() {
        return Flux.just("1","2","1","3","4","5","3","3");
    }

    public Flux<String> fragile_service() {
        return Flux.just("watch_out").concatWith(Flux.error(new RuntimeException("Spiders!")));
    }

    public Flux<Integer> number_service() {
        return Flux.range(0,300);
    }

    public Flux<Message> generateMessage() {

        return Flux.range(0,100)
            .doFirst(() -> System.out.println(" > Start generating messages"))
            .delaySubscription(Duration.ofSeconds(1))
            .map(i -> new Message("user#" + i, "payload#" + i))
            .delayElements(Duration.ofMillis(100));
    }

    public record Message(String user, String payload) {}
}
