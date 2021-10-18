package com.smartnews.ad.dynamic.elasticsearch.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Client {
    private final RestHighLevelClient highLevelClient;

    private final ThreadPoolExecutor executor;

    public Client() {
//        RestClientBuilder builder = RestClient.builder(new HttpHost("172.22.43.158", 9200),
//                new HttpHost("172.22.53.157", 9200),
//                new HttpHost("172.22.76.122", 9200));
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 8888));
        highLevelClient = new RestHighLevelClient(builder);
        executor = new ThreadPoolExecutor(6, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(5000000), new DiscardOldestPolicyImpl());
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

    public void loadData(String index, int threads) throws IOException {
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
//        BulkRequest bulkRequest1 = new BulkRequest();
//        BulkRequest bulkRequest2 = new BulkRequest();
//        BulkRequest bulkRequest3 = new BulkRequest();
        for (int i = 0; i < threads; i++) {
            int finalI = i;
            executor.submit(() -> {
                try {
                    helper(highLevelClient, records, finalI, bulkRequests.get(finalI), threads);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
//        executor.submit(() -> {
//            try {
//                helper(highLevelClient, records, 0, bulkRequest1);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//        executor.submit(() -> {
//            try {
//                helper(highLevelClient, records, 1, bulkRequest2);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//        executor.submit(() -> {
//            try {
//                helper(highLevelClient, records, 2, bulkRequest3);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
        while (true);
    }

    private void helper(RestHighLevelClient client, List<CSVRecord> records, int index, BulkRequest bulkRequest, int threads) throws IOException {
        for (int i = 0 ; i < records.size(); i++) {
            if (i % threads == index) {
                CSVRecord record = records.get(i);
                bulkRequest.add(new IndexRequest("test-1").source(XContentType.JSON, "title", record.get("title"), "description", record.get("description"), "second_category", record.get("second_category"), "third_category", record.get("third_category"), "image_link", record.get("image_link")));
            }
            if (bulkRequest.numberOfActions() > 0 && bulkRequest.numberOfActions() % 1000 == 0) {
                System.out.println("Collect " + bulkRequest.numberOfActions() + " items, inserting...");
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulkRequest = new BulkRequest();
            }
        }
    }
}