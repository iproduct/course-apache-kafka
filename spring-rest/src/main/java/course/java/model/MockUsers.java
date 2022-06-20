package course.java.model;

public class MockUsers {
    public static final User[] MOCK_USERS = {
            new User("Ivan", "Petrov", 25, "ivan", "Ivan123#", Role.ADMIN,
                    "+(359) 887 894356"),
            new User("Nadezda", "Todorova", 29, "nadia", "Nadia123#", Role.READER,
                    "+(359) 889 123456"),
            new User("Hristo", "Yanakiev", 23, "hristo", "Hris123#", Role.ADMIN, null),
            new User("Gorgi", "Petrov", 45, "georgi", "Gogo123#", Role.READER,
                    "+(1) 456778898"),
            new User("Petko", "Yanakiev", 23, "hristo2", "Hris123#", Role.AUTHOR,
                    "+(11) 56457567"),
            new User("Stoyan", "Petrov", 45, "georgi2", "Gogo123#", Role.ADMIN,
                    "+(91) 456456456"),
            new User("Maria", "Manolova", 22, "maria", "Mari123#", Role.AUTHOR, null)
    };
}
