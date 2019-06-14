package org.iproduct.kafka.dao;

import org.iproduct.kafka.exception.NonexistingEntityException;
import org.iproduct.kafka.model.Person;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PersonDBController {
	public static final String DB_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	public static final String DB_URL = "jdbc:sqlserver://localhost:1433;databaseName=kafka_demo;user=sa;password=sa12X+;";
	public static final String DB_USER = "sa";
	public static final String DB_PASSWORD = "sa12X+";

	private List<Person> availablePersons = new ArrayList<Person>();

	public void init() throws ClassNotFoundException {
		Class.forName(DB_DRIVER); // load db driver
	}

	public void reload() {
		try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
				Statement statement = connection.createStatement()) {
			ResultSet rs = statement.executeQuery("SELECT * FROM persons ORDER BY lname");
			while (rs.next()) {
				availablePersons.add(new Person(rs.getInt(1), rs.getString(2), rs.getString(3),
						rs.getInt(4)));
			}
			System.out.println(availablePersons);
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public List<Person> getAllPersons() {
		return availablePersons;
	}

	public Person getPersonById(long id) throws NonexistingEntityException {
		try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
				Statement statement = connection.createStatement()) {
			ResultSet rs = statement.executeQuery("SELECT * FROM books WHERE id=" + id);
			if (rs.next()) {
				return new Person(rs.getInt(1), rs.getString(2), rs.getString(3),
						rs.getInt(4));
			}
		} catch (SQLException ex) {
			throw new NonexistingEntityException("Person with ID=" + id +" not found.", ex);
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		PersonDBController bdc = new PersonDBController();
		bdc.init();
		bdc.reload();
	}
}
