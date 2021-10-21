package com.smartnews.ad.dynamic.elasticsearch.utils;

import org.apache.commons.csv.CSVFormat;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class Client {
    private final RestHighLevelClient highLevelClient;

    private final ThreadPoolExecutor executor;

    public Client() {
        RestClientBuilder builder = RestClient.builder(new HttpHost("10.1.115.24", 9200),
                new HttpHost("10.1.130.153", 9200),
                new HttpHost("10.1.131.122", 9200));
//        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 8888));
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
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(queryString, "title", "title.ngram", "description", "second_category", "third_category");
        multiMatchQueryBuilder.operator(Operator.AND);

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
        long start = System.currentTimeMillis();
        long totalAmount = 0;
        for (CSVRecord record: records) {
            SearchResponse response = query(index, record.get("keyword"), limit);
            // getHits().getTotalHits() will always return the size instead of required limited size
            totalAmount += response.getHits().getHits().length;
        }
        long end = System.currentTimeMillis();
        System.out.println("Total time spent for " + records.size() + " queries: " + (end-start) / 1000 + "s");
        System.out.println("Average return size: " + totalAmount / (float)records.size());
    }

    public void loadData(String index, int threads) throws IOException, InterruptedException {
//        BulkRequest bulkRequest = new BulkRequest();

        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("mock_data.csv");
        Reader reader = new InputStreamReader(inputStream);
        List<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t').parse(reader).getRecords();
//        for (int i = 0; i < records.size(); i++) {
//            CSVRecord record = records.get(i);
//            if (i > 0 && i % 1000 == 0) {
//                System.out.println("Collect " + i + " items, inserting...");
//                BulkResponse bulkResponse = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
//                System.out.println(bulkResponse.status());
//                System.out.println(bulkResponse.hasFailures());
//                bulkRequest = new BulkRequest();
//            }
//            bulkRequest.add(new IndexRequest(index).source(XContentType.JSON, "title", record.get("title"), "description", record.get("description"), "second_category", record.get("second_category"), "third_category", record.get("third_category"), "image_link", record.get("image_link")));
//        }
//        if (bulkRequest.numberOfActions() > 0) {
//            System.out.println("Last " + bulkRequest.numberOfActions() + " items, inserting...");
//            BulkResponse bulkResponse = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
//            System.out.println(bulkResponse.status());
//            System.out.println(bulkResponse.hasFailures());
//        }
        List<BulkRequest> bulkRequests = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            bulkRequests.add(new BulkRequest());
        }
        List<Callable<Object>> todo = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            int finalI = i;
            todo.add(Executors.callable(() -> {
                try {
                    helper(index, highLevelClient, records, finalI, bulkRequests.get(finalI), threads);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }));

//            executor.submit(() -> {
//                try {
//                    helper(index, highLevelClient, records, finalI, bulkRequests.get(finalI), threads);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            });
        }
        executor.invokeAll(todo);
        while (true);
    }

    private void helper(String indexName, RestHighLevelClient client, List<CSVRecord> records, int index, BulkRequest bulkRequest, int threads) throws IOException {
        bulkRequest.timeout("1m");
        for (int i = 0 ; i < records.size(); i++) {
            if (i % threads == index) {
                CSVRecord record = records.get(i);
                bulkRequest.add(new IndexRequest(indexName).source(XContentType.JSON, "title", record.get("title"), "description", record.get("description"), "second_category", record.get("second_category"), "third_category", record.get("third_category"), "image_link", record.get("image_link")));
            }
            if (bulkRequest.numberOfActions() > 0 && bulkRequest.numberOfActions() % 1000 == 0) {
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