package course.kafka.model;

import lombok.*;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class StockPrice {
    private int id;
    @NonNull
    private String symbol;
    @NonNull
    private String name;
    @NonNull
    private Double price;
}
