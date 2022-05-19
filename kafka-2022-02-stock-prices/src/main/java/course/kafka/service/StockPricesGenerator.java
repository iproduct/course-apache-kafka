package course.kafka.service;

import course.kafka.model.StockPrice;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;


public class StockPricesGenerator {
    private static Random rand = new Random();

    public static final List<StockPrice> STOCKS = Arrays.asList(
            new StockPrice("VMW", "VMWare", 215.35),
            new StockPrice("GOOG", "Google", 309.17),
            new StockPrice("CTXS", "Citrix Systems, Inc.", 112.11),
            new StockPrice("DELL", "Dell Inc.", 92.93),
            new StockPrice("MSFT", "Microsoft", 255.19),
            new StockPrice("ORCL", "Oracle", 115.72),
            new StockPrice("RHT", "Red Hat", 111.27)
    );

    public static Flux<StockPrice> getQuotesStream(long number, Duration period) {
        return Flux.interval(period)
                .take(number)
                .map(index -> {
                    StockPrice quote = STOCKS.get(index.intValue() % STOCKS.size());
                    quote.setPrice(quote.getPrice() * (0.9 + 0.2 * rand.nextDouble()));
                    return new StockPrice(index, quote.getSymbol(), quote.getName(), quote.getPrice(), new Date());
                })
                .share()
                .log();
    }
}
