package course.kafka.model;

import lombok.*;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TemperatureReading {
    private String id;
    private String sensorId;
    private double value;
    private LocalDateTime timestamp = LocalDateTime.now();

    public TemperatureReading(String id, String sensorId, double value) {
        this.id = id;
        this.sensorId = sensorId;
        this.value = value;
    }
}
