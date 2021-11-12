package com.smartnews.ad.dynamic.elasticsearch.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HttpClient {

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
        List<Long> uniqueTimeSpent = new ArrayList<>();
        long allTotalTime = 0;
        long uniqueTotalTime = 0;
        int illegalNum = 0;
        int num = 0;

        for (String queryString: query) {
            CloseableHttpClient client = HttpClients.createDefault();
            try {
                String newStr = queryString.replaceAll("\\s+", "%20");
                HttpGet request = new HttpGet("https://search-server.dynamic-ads.smartnews.net/search/" + newStr);
                long start = System.currentTimeMillis();
                HttpResponse response = client.execute(request);
                long end = System.currentTimeMillis();
                //                System.out.println("Query done for: " + queryString);
                num += 1;
                if (num % 1000 == 0) {
                    System.out.println(num);
                }
                allTimeSpent.add(end-start);
                allTotalTime += end-start;
                if (!ignored.contains(queryString)) {
                    uniqueTimeSpent.add(end-start);
                    uniqueTotalTime += end-start;
                    ignored.add(queryString);
                }
            } catch (IllegalArgumentException e) {
                illegalNum += 1;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Illegal num: " + illegalNum);
        Collections.sort(uniqueTimeSpent);
        System.out.println("Average time spent for " + (ignored.size()-illegalNum) + " queries: " + (float)uniqueTotalTime / (float)(ignored.size()-illegalNum) + "ms");
        System.out.println("Total time P50 " + percentile(uniqueTimeSpent, 50) + " ms");
        System.out.println("Total time P99 " + percentile(uniqueTimeSpent, 99) + " ms");

        Collections.sort(allTimeSpent);
        System.out.println("Average time spent for " + (query.size()-illegalNum) + " queries: " + (float)allTotalTime / (float)(query.size()-illegalNum) + "ms");
        System.out.println("Total time P50 " + percentile(allTimeSpent, 50) + " ms");
        System.out.println("Total time P99 " + percentile(allTimeSpent, 99) + " ms");
    }

    public void intensive_test() throws IOException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("query_data.csv");
        Reader reader = new InputStreamReader(inputStream);
        List<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader).getRecords();
        List<String> query = records.stream().map(r -> r.get("keyword")).filter(str -> str.length() < 1024).collect(Collectors.toList());
        reader.close();
        inputStream.close();

        ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(5000000), new DiscardOldestPolicyImpl());
        while (true) {
            for (String queryString: query) {
                executor.submit(() -> {
                    CloseableHttpClient client = HttpClients.createDefault();
                    try {
                        String newStr = queryString.replaceAll("\\s+", "%20");
                        HttpGet request = new HttpGet("https://search-server.dynamic-ads.smartnews.net/search/" + newStr);
                        HttpResponse response = client.execute(request);
                    } catch (IllegalArgumentException e) {
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

}
