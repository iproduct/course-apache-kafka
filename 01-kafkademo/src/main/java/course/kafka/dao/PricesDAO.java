package course.kafka.dao;

import course.kafka.model.StockPrice;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class PricesDAO {
    public static final String DB_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String DB_URL =
            "jdbc:sqlserver://localhost:1433;databaseName=kafka_demo;user=sa;password=sa12X+;";
    public static final String DB_USER = "sa";
    public static final String DB_PASSWORD = "sa12X+";

    List<StockPrice> prices = new CopyOnWriteArrayList<>();

    public PricesDAO() {
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException ex) {
            log.error("MS SQLServer db driver not found.", ex);
        }
    }

    public void reload() {
        try(Connection con = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
           Statement statement = con.createStatement();
        ) {
            ResultSet rs = statement.executeQuery("SELECT * FROM Prices");
            while(rs.next()) {
                prices.add(new StockPrice(
                        rs.getInt("id"),
                        rs.getString("symbol"),
                        rs.getString("name"),
                        rs.getDouble("price"),
                        rs.getTimestamp("timestamp")
                ));
            }
        } catch (SQLException e) {
            log.error("Connection to MS SQLServer URL:{} can not be established.\n{}", DB_URL, e);
        }
    }

    public void printData(){
        prices.forEach(price -> {
            System.out.printf(
                "| %10d | %5.5s | %20.20s | %10.2f | %td.%<tm.%<ty %<tH:%<tM:%<tS |",
                price.getId(), price.getSymbol(), price.getName(), price.getPrice(),
                    price.getTimestamp());
        });
    }

    public static void main(String[] args) {
        PricesDAO dao = new PricesDAO();
        dao.reload();
        dao.printData();
    }
}
