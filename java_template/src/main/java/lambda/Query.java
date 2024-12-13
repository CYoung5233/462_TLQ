package lambda;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
// import java.util.List;
import java.util.Map;


public class Query {

    // Database configuration (initialized in loadDB method)
    private static String DB_HOST;
    private static String DB_USER;
    private static String DB_PASSWORD;
    private static String DB_NAME;
    private static String DB_PORT;
    private static String DB_URL;

    // Method to fetch all data (joined from both Order and Item tables)
    public static String fetchData() {
        System.out.println("MADE IT TO QUERY - FetchData");
        loadDB();
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            System.out.println("QUERY - Database connected.");
            String query = "SELECT * FROM `Order` JOIN Item ON `Order`.Item_Type = Item.Item_Type";
            String result = null;
            try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
                result = rs.toString();

                System.out.println("Data Retrieved:");
                while (rs.next()) {
                    System.out.println("Order ID: " + rs.getInt("Order_ID"));
                    System.out.println("Item Type: " + rs.getString("Item_Type"));
                }
            }
            return result;
        } catch (SQLException e) {
            System.err.println("Error executing query: " + e.getMessage());
            return null;
        }
    }

    // Method to fetch aggregated data (e.g., total revenue by item type)
    public static String fetchAggregatedData() {
        System.out.println("MADE IT TO QUERY");
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            System.out.println("QUERY - Database connected.");
            String query = "SELECT Item.Item_Type, SUM(Item.Total_Revenue) AS Total_Revenue FROM Item GROUP BY Item.Item_Type";
            String result = null;
            try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
                result = rs.toString();

                System.out.println("Aggregated Data:");
                while (rs.next()) {
                    String itemType = rs.getString("Item_Type");
                    double totalRevenue = rs.getDouble("Total_Revenue");
                    System.out.println("Item Type: " + itemType + " | Total Revenue: " + totalRevenue);
                }
            }
            return result;
        } catch (SQLException e) {
            System.err.println("Error executing query: " + e.getMessage());
            return null;
        }
    }

    // Method to load environment variables from a .env file
    public static Map<String, String> loadEnv(String file) throws IOException {
        Map<String, String> env = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty() && line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    env.put(parts[0].trim(), parts[1].trim());
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + e.getMessage());
            //System.err.println(STR."Error: .env file not found: \{e.getMessage()}");
        }
        return env;
    }

    // Method to load database credentials from the .env file
    public static void loadDB() {
        try {
            //Map<String, String> db = loadEnv(".env");
            // Print the values to debug
//            System.out.println(STR."DB_HOST: \{db.get("DB_HOST")}");
//            System.out.println(STR."DB_USER: \{db.get("DB_USER")}");
//            System.out.println(STR."DB_PASSWORD: \{db.get("DB_PASSWORD")}");
//            System.out.println(STR."DB_NAME: \{db.get("DB_NAME")}");
//            System.out.println(STR."DB_PORT: \{db.get("DB_PORT")}");
            
            // DB_HOST = db.get("DB_HOST");
            // DB_USER = db.get("DB_USER");
            // DB_PASSWORD = db.get("DB_PASSWORD");
            // DB_NAME = db.get("DB_NAME");
            // DB_PORT = db.get("DB_PORT");
            DB_HOST = System.getenv("DB_HOST");
            DB_USER = System.getenv("DB_USER");
            DB_PASSWORD = System.getenv("DB_PASSWORD");
            DB_NAME = System.getenv("DB_NAME");
            DB_PORT = System.getenv("DB_PORT");
    
            // Make sure URL is properly formatted
            DB_URL = "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;
            
            // Print the final DB_URL to verify
            //System.out.println("DB_URL: " + DB_URL);
            
        // } catch (IOException e) {
        //     System.err.println("Error loading DB: " + e.getMessage());
        //     //System.err.println(STR."Error loading database configuration: \{e.getMessage()}");
        } catch (Exception e) {
            System.err.println("Unexpected Error in Query.LoadDB: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // Load database configuration
        loadDB();

        // Fetch all data
        System.out.println("Fetching All Data...");
        fetchData();

        // Fetch aggregated data
        System.out.println("\nFetching Aggregated Data...");
        fetchAggregatedData();
    }

}
