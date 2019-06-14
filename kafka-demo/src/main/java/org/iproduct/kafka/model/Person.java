package org.iproduct.kafka.model;

import lombok.*;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class Person {
	private long id;
	@NonNull
	private String firstName;
	@NonNull
	private String lastName;
	@NonNull
	private int age;
}
