import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CouchBaseMultiThreadProgram {

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
        // int[] threadPoolSizes = {2, 4, 8}; // Different thread pool sizes for testing
        final int NUM_THREADS = 2;// Different thread pool sizes for testing
        System.out.println("Testing with thread pool size: " + NUM_THREADS);
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        long endTime = System.currentTimeMillis(); // Run for 3 minutes
        // long endTime = System.currentTimeMillis() + 3 * 60 * 1000; // 3 minutes
        for (int i = 0; i < NUM_THREADS; i++) {
            executor.submit(() -> performCouchbaseOperations(bucket, endTime));
        }
        executor.shutdown();
        try {
            executor.awaitTermination(3, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.disconnect();
    }

    private static void performCouchbaseOperations(Bucket bucket,Long  endTime) {
        while (System.currentTimeMillis() < endTime) {
            try {
                String key = "aktest3";
                File jsonFile = new File(FILE_PATH);
                Collection collection = bucket.scope(REGION_NAME).collection(COLLECTION_NAME);
                String jsonContent = new String(Files.readAllBytes(jsonFile.toPath()));
                JsonObject jsonObject = JsonObject.fromJson(jsonContent);
                // String key = UUID.randomUUID().toString();
                Long threadId = Thread.currentThread().getId();
                insertJsonData(key, collection, jsonObject, threadId);
                for (int i = 0; i < 3; i++) {
                    readJsonData(key, collection, threadId);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void readJsonData(String key, Collection collection, Long threadId) {
        long startTime;
        startTime = System.nanoTime();
        GetResult result = collection.get(key);
        long retrievalTime = System.nanoTime() - startTime;
        JsonObject retrievedJson = result.contentAsObject();
        //    System.out.println("!!Retrieved JSON: " + retrievedJson);
        System.out.println("Retrieved document: " + retrievedJson + " in " + retrievalTime + " ns for thread " + threadId);
    }

    private static void insertJsonData(String key, Collection collection, JsonObject jsonObject,Long threadId) {
        long startTime = System.nanoTime();
        collection.upsert(key, jsonObject);
        System.out.println("Inserted document with key: " + key + " in " + (System.nanoTime() - startTime) + " ns for thread " + threadId);
    }

}