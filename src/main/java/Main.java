
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final String COUCHBASE_CONNECTION_STRING = "couchbase://127.0.0.1";
    private static final String COUCHBASE_USERNAME = "Administrator";
    private static final String COUCHBASE_PASSWORD = "Anvit@#123";
    private static final String BUCKET_NAME = "sampleTestBucket";
    private static final String REGION_NAME = "sampleScope";
    private static final String COLLECTION_NAME = "sampleCollection";
    private static final String FILE_PATH = "C:/Users/akshayc/sampleJsonFile/sample1.json";
    public static void main(String[] args) {

        Cluster cluster = Cluster.connect(COUCHBASE_CONNECTION_STRING, COUCHBASE_USERNAME, COUCHBASE_PASSWORD);
        Bucket bucket = cluster.bucket(BUCKET_NAME);

        int[] threadPoolSizes = {2, 4, 8}; // Different thread pool sizes for testing

        for (int poolSize : threadPoolSizes) {
            System.out.println("Testing with thread pool size: " + poolSize);
            ExecutorService executor = Executors.newFixedThreadPool(poolSize);
           // long endTime = System.currentTimeMillis() + 0.1 * 60 * 1000; // Run for 3 minutes
            System.out.println("!!poolSize: " + poolSize);
           // while (System.currentTimeMillis() < endTime) {
            System.out.println("!!before execute method: " + System.currentTimeMillis());
                executor.submit(() -> performCouchbaseOperations(bucket,poolSize));
           // }

            executor.shutdown();
            try {
                executor.awaitTermination(3, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        cluster.disconnect();
    }

    private static void performCouchbaseOperations(Bucket bucket,int poolSize) {
        try {
                    File jsonFile= new File(FILE_PATH);
                    Collection collection = bucket.scope(REGION_NAME).collection(COLLECTION_NAME);
                    String jsonContent = new String(Files.readAllBytes(jsonFile.toPath()));
                    JsonObject jsonObject = JsonObject.fromJson(jsonContent);
                   // String key = UUID.randomUUID().toString();
            String key ="aktest2";
                    // Insert JSON into Couchbase
                    System.out.println("!!documentId: " + key);
            System.out.println("!!before insert method: " + System.currentTimeMillis());
            System.out.println("!!before insert method pool size: " +poolSize);
            long startTime = System.nanoTime();
                    collection.upsert(key, jsonObject);
            long insertionTime = System.nanoTime() - startTime;
            System.out.println("Inserted document with key: " + key + " in " + insertionTime + " ns");

            System.out.println("!!after inserted JSON: " + jsonObject);
                    System.out.println("Inserted document with key: " + key);
            System.out.println("!!after insert method pool size: " +poolSize);
                    // Read the JSON 3 times
            System.out.println("!!before read method: " + System.currentTimeMillis());
            System.out.println("!!before read method pool size: " +poolSize);
                    for (int i = 0; i < 3; i++) {
                        startTime = System.nanoTime();
                        GetResult result = collection.get(key);
                        long retrievalTime = System.nanoTime() - startTime;
                        JsonObject retrievedJson = result.contentAsObject();
                        System.out.println("!!Retrieved JSON: " + retrievedJson);
                        System.out.println("Retrieved document: " + retrievedJson + " in " + retrievalTime + " ns");

                    }
            System.out.println("!!after read method: " + System.currentTimeMillis());
            System.out.println("!!after read method pool size: " +poolSize);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}