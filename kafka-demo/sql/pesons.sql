DROP TABLE IF EXISTS persons;

CREATE TABLE persons
(
  id  int IDENTITY (1,1) PRIMARY KEY,
  lName  varchar(255) NOT NULL,
  fName varchar(255),
  age       int
);

