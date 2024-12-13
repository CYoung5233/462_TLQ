package lambda;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
// import java.nio.file.Files;
// import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Load {
    // public static void main(String[] args) {
    //     System.out.println("main");
    //     try {
    //         //loadJSONtoDB();
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    // }

    public static void loadJSONtoDB(String jsonData) throws Exception {
        //Class.forName("com.mysql.cj.jdbc.Driver");
        // Step 1: Load environment variables
        //Map<String, String> properties = loadEnv("./.env");
        // String dbHost = properties.get("DB_HOST");
        // String dbUser = properties.get("DB_USER");
        // String dbPassword = properties.get("DB_PASSWORD");
        // String dbName = properties.get("DB_NAME");
        // String dbPort = properties.get("DB_PORT");
        String dbHost = System.getenv("DB_HOST");
        String dbUser = System.getenv("DB_USER");
        String dbPassword = System.getenv("DB_PASSWORD");
        String dbName = System.getenv("DB_NAME");
        String dbPort = System.getenv("DB_PORT");
        System.out.println("host: " + dbHost);
        System.out.println("user: " + dbUser);
        System.out.println("pass: " + dbPassword);
        System.out.println("name: " + dbName);
        System.out.println("port: " + dbPort);

        // Step 2: Read the transformed JSON file
        //String jsonFilePath = Paths.get("/tmp/transformed_100_TS.json").toAbsolutePath().toString();
        //String jsonData = new String(Files.readAllBytes(Paths.get(jsonFilePath)));
        List<Map<String, Object>> records = parseJSON(jsonData);

        // Step 3: Connect to the AWS RDS MySQL/MariaDB database
        String jdbcUrl = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
        //+ dbHost + ":" + dbPort + "/" + dbName;
        System.out.println("MADE IT RIGHT BEFORE JDBC CONNECTION: " + jdbcUrl);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)) {
//        try (Connection connection = DriverManager.getConnection(jdbcUrl, dbUser, "")) {
            System.out.println("Connected to database successfully");


            // Step 4: Restart the database (Optional, use with caution)
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP DATABASE IF EXISTS db;");
                statement.execute("CREATE DATABASE IF NOT EXISTS db;");
                statement.execute("USE db;");

                String createTableQuery = "CREATE TABLE IF NOT EXISTS sales (item_type VARCHAR(32) NOT NULL, order_priority    VARCHAR(1)      NOT NULL, order_date VARCHAR(32) NOT NULL, order_id BIGINT NOT NULL PRIMARY KEY, units_sold BIGINT NOT NULL, unit_price DECIMAL(10,2) NOT NULL, unit_cost DECIMAL(10,2) NOT NULL, total_revenue DECIMAL(10,2) NOT NULL, total_cost DECIMAL(10,2) NOT NULL, total_profit DECIMAL(10,2) NOT NULL);";
                statement.execute(createTableQuery);
            }

            // Step 5: Insert records into the database
            String insertQuery = "INSERT INTO sales (item_type, order_priority, order_date, order_id, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                for (Map<String, Object> record : records) {
                    preparedStatement.setString(1, (String) record.get("Item Type"));
                    preparedStatement.setString(2, (String) record.get("Order Priority"));
                    preparedStatement.setString(3, (String) record.get("Order Date"));
                    preparedStatement.setLong(4, ((Number) record.get("Order ID")).longValue());
                    preparedStatement.setLong(5, ((Number) record.get("Units Sold")).longValue());
                    preparedStatement.setBigDecimal(6, new java.math.BigDecimal(((Number) record.get("Unit Price")).doubleValue()));
                    preparedStatement.setBigDecimal(7, new java.math.BigDecimal(((Number) record.get("Unit Cost")).doubleValue()));
                    preparedStatement.setBigDecimal(8, new java.math.BigDecimal(((Number) record.get("Total Revenue")).doubleValue()));
                    preparedStatement.setBigDecimal(9, new java.math.BigDecimal(((Number) record.get("Total Cost")).doubleValue()));
                    preparedStatement.setBigDecimal(10, new java.math.BigDecimal(((Number) record.get("Total Profit")).doubleValue()));

                    preparedStatement.executeUpdate();
                }
            }
            System.out.println("Data loaded successfully!");
        } catch (Exception e) {
            System.out.println("ERRED OUT");
            System.err.println("Database Connection Error");
            System.err.println("Message: " + e.getMessage());
            System.err.println("Error loading data: " + e.getMessage());
            throw e;
        }
    }

    // private static Map<String, String> loadEnv(String filePath) throws IOException {
    //     Map<String, String> properties = new HashMap<>();
    //     try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
    //         String line;
    //         while ((line = reader.readLine()) != null) {
    //             line = line.trim();
    //             if (line.isEmpty() || line.startsWith("#")) {
    //                 continue;
    //             }
    //             if (!line.isEmpty() && line.contains("=")) {
    //                 String[] parts = line.split("=", 2);
    //                 properties.put(parts[0].trim(), parts[1].trim());
    //             }
    //         }
    //     }
    //     return properties;
    // }

    private static List<Map<String, Object>> parseJSON(String jsonData) {
        List<Map<String, Object>> records = new ArrayList<>();
        jsonData = jsonData.trim();
        if (jsonData.startsWith("[") && jsonData.endsWith("]")) {
            jsonData = jsonData.substring(1, jsonData.length() - 1).trim();
            String[] objects = jsonData.split("},\\s*\\{");

            for (String object : objects) {
                object = object.replaceAll("[{}]", "").trim();
                String[] fields = object.split(",\\s*");
                Map<String, Object> record = new HashMap<>();

                for (String field : fields) {
                    String[] keyValue = field.split(":", 2);
                    String key = keyValue[0].trim().replaceAll("\"", "");
                    String value = keyValue[1].trim().replaceAll("\"", "");
                    record.put(key, parseValue(value));
                }
                records.add(record);
            }
        }
        return records;
    }

    private static Object parseValue(String value) {
        if (value.matches("^-?\\d+\\.\\d+$")) {
            return Double.parseDouble(value);
        } else if (value.matches("^-?\\d+$")) {
            return Long.parseLong(value);
        } else {
            return value;
        }
    }

}
