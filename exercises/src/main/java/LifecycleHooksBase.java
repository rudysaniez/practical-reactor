import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * @author Stefan Dragisic
 */
public class LifecycleHooksBase {

    public Flux<Integer> room_temperature_service() {
        return Flux.interval(Duration.ofMillis(100), Duration.ofMillis(100))
                   .take(20)
                   .map(i -> ThreadLocalRandom.current().nextInt(10, 30));
    }
}
