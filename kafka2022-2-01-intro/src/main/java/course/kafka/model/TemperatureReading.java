package course.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public class TemperatureReading {
    private final String id;
    private final String sensorId;
    private final double value;
    private LocalDateTime timestamp = LocalDateTime.now();
}
