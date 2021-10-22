package com.smartnews.ad.dynamic.elasticsearch.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Time;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Client {
    private final RestHighLevelClient highLevelClient;

    private final ThreadPoolExecutor executor;

    public Client() {
//        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 8888));
        RestClientBuilder builder = RestClient.builder(new HttpHost("es-nlb.dynamic-ads.smartnews.net", 9200));
        highLevelClient = new RestHighLevelClient(builder);
        executor = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(5000000), new DiscardOldestPolicyImpl());
    }

    public void close() throws IOException {
        this.highLevelClient.close();
    }

    private String[] retrieveIndex(String query) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(query);
        GetIndexResponse getIndexResponse = highLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(Arrays.toString(getIndexResponse.getIndices()));
        return getIndexResponse.getIndices();
    }

    private void swapIndex(String prefix) throws IOException {
        String[] indices = this.retrieveIndex(prefix + "*");
        assert indices.length == 1;

        String oldIndex = indices[0];
        int indexId = Integer.parseInt(oldIndex.split("-")[1]) + 1;
        String newIndex = "test-" + indexId;

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(newIndex);
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        // could configure optional argument for timeout...
        createIndexRequest.source(readMappingConfig(), XContentType.JSON);
        this.highLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        // write data to the index

        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
        AliasActions aliasAddAction =
                new AliasActions(AliasActions.Type.ADD)
                        .index(newIndex)
                        .alias(prefix);
        AliasActions removeIndexAction =
                new AliasActions(AliasActions.Type.REMOVE_INDEX)
                        .index(oldIndex);
        aliasesRequest.addAliasAction(aliasAddAction).addAliasAction(removeIndexAction);
        this.highLevelClient.indices().updateAliases(aliasesRequest, RequestOptions.DEFAULT);
        this.retrieveIndex("test*");
    }

    private String readMappingConfig() throws IOException {
        return new String(Files.readAllBytes(Paths.get("src/main/java/com/smartnews/ad/dynamic/elasticsearch/config/index.json")));
    }

    private SearchResponse query(String index, String queryString, int limit) throws IOException {
        String query = "\"bool\": {\n" +
                "  \"must\": [\n" +
                "    {\n" +
                "      \"match\":\n" +
                "      {\n" +
                "        \"title\": {\n" +
                "          \"query\": \"" +queryString+ "\",\n" +
                "          \"minimum_should_match\": \"90%\"\n" +
                "        }\n" +
                "      },\n" +
                "    },\n" +
                "    {\n" +
                "      \"match\":\n" +
                "      {\n" +
                "        \"title.ngram\": {\n" +
                "          \"query\": \"" +queryString+ "\",\n" +
                "          \"minimum_should_match\": \"90%\"\n" +
                "        }\n" +
                "      },\n" +
                "    }\n" +
                "  ],\n" +
                "  \"should\": [\n" +
                "    {\n" +
                "      \"match\": {\n" +
                "        \"second_category\": {\n" +
                "          \"query\": \"" +queryString+ "\",\n" +
                "          \"operator\": \"and\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"match\": {\n" +
                "        \"third_category\": {\n" +
                "          \"query\":\"" +queryString+ "\",\n" +
                "          \"operator\": \"and\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
//        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(queryString, "title", "title.ngram", "description", "second_category", "third_category");
//        multiMatchQueryBuilder.operator(Operator.AND);

        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(new InputStreamStreamInput(new ByteArrayInputStream(query.getBytes())));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(multiMatchQueryBuilder);
        searchSourceBuilder.size(limit);

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        return highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    public void queryTest(String index, int limit) throws IOException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("query_data.csv");
        Reader reader = new InputStreamReader(inputStream);
        List<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader).getRecords();
        Set<String> uniqueQuery = records.stream().map(r -> r.get("keyword")).filter(str -> str.length() < 1024).collect(Collectors.toSet());

        List<Long> timeSpent = new ArrayList<>();
        List<Long> hitCounts = new ArrayList<>();
        long totalTime = 0;
        long totalAmount = 0;
        long hitQueryNumber = 0;

        for (String queryString: uniqueQuery) {
            long start = System.currentTimeMillis();
            SearchResponse response = query(index, queryString, limit);
            long end = System.currentTimeMillis();
            timeSpent.add(end-start);

            int hitCount = response.getHits().getHits().length;
            if (hitCount > 0) hitQueryNumber += 1;
            hitCounts.add((long) hitCount);

            // getHits().getTotalHits() will always return the size instead of required limited size
            totalAmount += hitCount;
            totalTime += end-start;
        }

        Collections.sort(timeSpent);
        Collections.sort(hitCounts);
        System.out.println("Average time spent for " + uniqueQuery.size() + " queries: " + (float)totalTime / (float)uniqueQuery.size() + "ms");
        System.out.println("Total time P50 " + percentile(timeSpent, 50));
        System.out.println("Total time P99 " + percentile(timeSpent, 99));

        System.out.println("Average return size: " + totalAmount / (float)uniqueQuery.size());
        System.out.println("Total hits P50 " + percentile(hitCounts, 50));
        System.out.println("Total hits P99 " + percentile(hitCounts, 99));

        System.out.println("Among " + uniqueQuery.size() + " queries, " + hitQueryNumber + " queries got result, which is " + (float)hitQueryNumber/uniqueQuery.size());
    }

    private long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index-1);
    }

    public void load(String index, int threads, int maxListSize) throws InterruptedException, IOException, ExecutionException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("mock_data.csv");
        Reader reader = new InputStreamReader(inputStream);
        CSVParser records = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t').parse(reader);
        int count = 0;
        List<CSVRecord> list = new ArrayList<>();
        for (CSVRecord record: records) {
            list.add(record);
            count++;
            if (count == maxListSize) {
                loadData(list, index, threads);
                list = new ArrayList<>();
                count = 0;
            }
        }
    }

    private void loadData(List<CSVRecord> records, String index, int threads) throws InterruptedException, ExecutionException {
        List<BulkRequest> bulkRequests = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            bulkRequests.add(new BulkRequest());
        }
        List<Future<Object>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            int finalI = i;
            futures.add(executor.submit(() -> {
                try {
                    helper(index, highLevelClient, records, finalI, bulkRequests.get(finalI), threads);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }));
        }
        for (Future<Object> future: futures) {
            future.get();
        }
    }

    private void helper(String indexName, RestHighLevelClient client, List<CSVRecord> records, int index, BulkRequest bulkRequest, int threads) throws IOException {
        bulkRequest.timeout("1m");
        for (int i = 0 ; i < records.size(); i++) {
            if (i % threads == index) {
                CSVRecord record = records.get(i);
                bulkRequest.add(new IndexRequest(indexName).source(XContentType.JSON, "title", record.get("title"), "description", record.get("description"), "second_category", record.get("second_category"), "third_category", record.get("third_category"), "image_link", record.get("image_link")));
            }
            if (bulkRequest.numberOfActions() > 0 && bulkRequest.numberOfActions() % 2000 == 0) {
                System.out.println("Collect " + bulkRequest.numberOfActions() + " items, inserting...");
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulkRequest = new BulkRequest();
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            System.out.println("Last " + bulkRequest.numberOfActions() + " items, inserting...");
            highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        }
    }
}