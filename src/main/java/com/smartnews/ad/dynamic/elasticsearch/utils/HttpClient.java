package com.smartnews.ad.dynamic.elasticsearch.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class HttpClient {

    private final CloseableHttpClient httpClient = HttpClients.createDefault();

    private long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index-1);
    }


    public void query() throws IOException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("query_data.csv");
        Reader reader = new InputStreamReader(inputStream);
        List<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader).getRecords();
        List<String> query = records.stream().map(r -> r.get("keyword")).filter(str -> str.length() < 1024).collect(Collectors.toList());

        Set<String> ignored = new HashSet<>();
        List<Long> allTimeSpent = new ArrayList<>();
        List<Long> allHitCounts = new ArrayList<>();
        List<Long> uniqueTimeSpent = new ArrayList<>();
        List<Long> uniqueHitCounts = new ArrayList<>();
        long allTotalTime = 0;
        long allTotalAmount = 0;
        long allHitQueryNumber = 0;
        long uniqueTotalTime = 0;
        long uniqueTotalAmount = 0;
        long uniqueHitQueryNumber = 0;
        int illegalNum = 0;

        for (String queryString: query) {
            try {
                HttpGet request = new HttpGet("https://search-server.dynamic-ads.smartnews.net/search/" + queryString.replaceAll("\\s+", "%20"));
                long start = System.currentTimeMillis();
                CloseableHttpResponse response = httpClient.execute(request);
                long end = System.currentTimeMillis();
                allTimeSpent.add(end-start);
                if (!ignored.contains(queryString)) {
                    uniqueTimeSpent.add(end-start);
                    ignored.add(queryString);
                }
            } catch (IllegalArgumentException e) {
                illegalNum += 1;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Collections.sort(uniqueTimeSpent);
        System.out.println("Average time spent for " + (ignored.size()-illegalNum) + " queries: " + (float)uniqueTotalTime / (float)(ignored.size()-illegalNum) + "ms");
        System.out.println("Total time P50 " + percentile(uniqueTimeSpent, 50) + " ms");
        System.out.println("Total time P99 " + percentile(uniqueTimeSpent, 99) + " ms");

        Collections.sort(allTimeSpent);
        System.out.println("Average time spent for " + (query.size()-illegalNum) + " queries: " + (float)allTotalTime / (float)(query.size()-illegalNum) + "ms");
        System.out.println("Total time P50 " + percentile(allTimeSpent, 50) + " ms");
        System.out.println("Total time P99 " + percentile(allTimeSpent, 99) + " ms");
    }

}
