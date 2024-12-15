package Java.OOP;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DBExporter {

    private static final String URL = "jdbc:postgresql://lt.uxcrowd.ru:54322/uxtest";
    private static final String USER = "student";
    private static final String PASSWORD = "f8rndk";

    public void exportToCSV(String filePath) {
        String query = "SELECT * FROM pg_stat_statements WHERE calls > 10 "
                + "ORDER BY mean_time DESC LIMIT 25";

        try {
            // Загрузка драйвера JDBC для PostgreSQL
            Class.forName("org.postgresql.Driver");

            try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query);
                 BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {

                // Запись заголовков в CSV
                writer.write("DateTime;Query;Mean Time (ms)\n");

                // Получение текущей даты и времени
                String currentDateTime = getCurrentDateTime();

                // Запись данных в CSV
                while (resultSet.next()) {
                    String sqlQuery = resultSet.getString("query");
                    double meanTime = resultSet.getDouble("mean_time");
                    writer.write(String.format("%s;\"%s\";%.2f\n", currentDateTime, sqlQuery, meanTime));
                }

                System.out.println("Data exported successfully to " + filePath);
            }
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("File error: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("JDBC Driver not found: " + e.getMessage());
        }
    }

    private String getCurrentDateTime() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return LocalDateTime.now().format(formatter);
    }

    public static void main(String[] args) {
        DBExporter exporter = new DBExporter();
        exporter.exportToCSV("pg_stat_statements_export.csv");
    }
}
