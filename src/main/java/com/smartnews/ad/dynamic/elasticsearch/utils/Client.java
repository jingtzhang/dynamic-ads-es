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
import java.util.Arrays;
import java.util.List;

public class Client {
    private final RestHighLevelClient highLevelClient;

    public Client() {
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 8888));
        highLevelClient = new RestHighLevelClient(builder);
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

    public void loadData(String index) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();

        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("mock_data.csv");
        Reader reader = new InputStreamReader(inputStream);
        List<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t').parse(reader).getRecords();
        for (int i = 0; i < records.size(); i++) {
            CSVRecord record = records.get(i);
            if (i > 0 && i % 10000 == 0) {
                System.out.println("Collect " + i + " items, inserting...");
                BulkResponse bulkResponse = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                System.out.println(bulkResponse.status());
                System.out.println(bulkResponse.hasFailures());
                bulkRequest = new BulkRequest();
            }
            bulkRequest.add(new IndexRequest(index).source(XContentType.JSON, "title", record.get("title"), "description", record.get("description"), "second_category", record.get("second_category"), "third_category", record.get("third_category"), "image_link", record.get("image_link")));
        }
        if (bulkRequest.numberOfActions() > 0) {
            System.out.println("Last " + bulkRequest.numberOfActions() + " items, inserting...");
            BulkResponse bulkResponse = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            System.out.println(bulkResponse.status());
            System.out.println(bulkResponse.hasFailures());
        }
    }
}
