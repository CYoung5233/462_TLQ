package lambda;

import java.io.*;
//import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;
import saaf.Response;

import com.amazonaws.services.s3.model.GetObjectRequest;

/**
 * lambda.Transform::handleRequest
 *
 * @author Caleb Krauter
 * @author Corey Young
 * @author Rick Adams
 */
public class Transform implements RequestHandler<Request, HashMap<String, Object>> {
    private static Request req;

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        req = request;
        int row = request.getRow();
        int col = request.getCol();
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        Response response = new Response();
        response.setValue("Bucket:" + bucketname + " filename:" + filename + " size: " + row + " rows, " + col + " cols.");

        Transform.main(new String[]{});

        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    public static void main(String[] args) {
        System.out.println("Entered Transform main.");
        try {
            //String filePath = Paths.get("100_Sales_Records.csv").toAbsolutePath().toString();
            String json_results = parseCSV();
            System.out.println("MADE IT PAST TRANSFORM PARSE");
            Load.loadJSONtoDB(json_results); // Assuming this is from your previous Java file
            System.out.println("MADE IT PAST LOAD");
            String result = Query.fetchAggregatedData();
            System.out.println("MADE IT PAST QUERY");
            // Uncomment if you want aggregated data instead
            // List<Map<String, Object>> result = fetchAggregatedData();

            System.out.println("Result: " + result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String parseCSV() {
        List<Map<String, String>> results = new ArrayList<>();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        try (S3Object s3Object = s3Client.getObject(new GetObjectRequest(req.getBucketname(), req.getFilename()))) {
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        //scanning data line by line
        String line;
        Scanner scanner = new Scanner(objectData);
        if(!scanner.hasNext()) {
            scanner.close();
            throw new IOException("CSV file is empty");
        }
        String headerLine = scanner.nextLine();
        String[] headers = headerLine.split(",");
        while (scanner.hasNext()) {
            line = scanner.nextLine();
            String[] values = line.split(",");
            Map<String, String> record = new HashMap<>();

            for (int i = 0; i < headers.length; i++) {
                String header = headers[i].trim();
                String value = values[i].trim();

                // Skip unwanted fields
                if (!header.equals("Region") && !header.equals("Country") && !header.equals("Sales Channel") && !header.equals("Ship Date")) {
                    record.put(header, value);
                }
            }
            results.add(record);
        }
        scanner.close();
        return toJSONString(results);

        } catch (IOException e) {
            System.err.println("Error while parsing CSV: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private static String toJSONString(List<Map<String, String>> data) {
        StringBuilder json = new StringBuilder();
        json.append("[");

        for (int i = 0; i < data.size(); i++) {
            json.append("{\n");
            Map<String, String> record = data.get(i);
            List<String> fields = record.entrySet().stream()
                .map(entry -> String.format("  \"%s\": \"%s\"", entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
            json.append(String.join(",\n", fields));
            json.append("\n}");
            if (i < data.size() - 1) {
                json.append(",");
            }
        }

        json.append("\n]");
        return json.toString();
    }

    // public static List<Map<String, Object>> fetchData() {
    //     // Mock implementation
    //     List<Map<String, Object>> result = new ArrayList<>();
    //     Map<String, Object> sample = new HashMap<>();
    //     sample.put("Key", "Value");
    //     result.add(sample);
    //     return result;
    // }

    public static List<Map<String, Object>> fetchAggregatedData() {
        // Mock implementation
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> sample = new HashMap<>();
        sample.put("AggregatedKey", "AggregatedValue");
        result.add(sample);
        return result;
    }

}
