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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Client {
    private final RestHighLevelClient highLevelClient;

    private final ThreadPoolExecutor executor;

//    private final String[] includeFetchSource = new String[]{"item_id", "click_count", "third_category"};
    private final String[] includeFetchSource = new String[]{"_id"};


    public Client() {
//        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 8888))
        RestClientBuilder builder = RestClient.builder(new HttpHost("es-nlb.dynamic-ads.smartnews.net", 9200))
            .setRequestConfigCallback(
                    requestConfigBuilder -> requestConfigBuilder
                            .setSocketTimeout(60000)
            );
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

    public SearchResponse query(String index, String queryString, int limit) throws IOException {
        MatchQueryBuilder mustMultiMatchQueryBuilder1 = new MatchQueryBuilder( "title", queryString).minimumShouldMatch("90%");
        MatchQueryBuilder mustMultiMatchQueryBuilder2 = new MatchQueryBuilder( "title.ngram", queryString).minimumShouldMatch("90%");
        MatchQueryBuilder shouldMultiMatchQueryBuilder1 = new MatchQueryBuilder( "second_category", queryString);
        shouldMultiMatchQueryBuilder1.operator(Operator.AND);
        MatchQueryBuilder shouldMultiMatchQueryBuilder2 = new MatchQueryBuilder( "third_category", queryString);
        shouldMultiMatchQueryBuilder2.operator(Operator.AND);
        BoolQueryBuilder booleanQueryBuilder = QueryBuilders.boolQuery();
        booleanQueryBuilder.must(mustMultiMatchQueryBuilder1).must(mustMultiMatchQueryBuilder2).should(shouldMultiMatchQueryBuilder1).should(shouldMultiMatchQueryBuilder2);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // return item_id only
        searchSourceBuilder.fetchSource(includeFetchSource, null);
        searchSourceBuilder.query(booleanQueryBuilder);
        searchSourceBuilder.size(limit);

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        return highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    public void queryTest(String index, int limit) throws IOException {
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

        for (String queryString: query) {
            long start = System.currentTimeMillis();
            SearchResponse response = query(index, queryString, limit);
            long end = System.currentTimeMillis();
            int hitCount = response.getHits().getHits().length;

            allTimeSpent.add(end-start);
            if (hitCount > 0) allHitQueryNumber += 1;
            allHitCounts.add((long) hitCount);
            allTotalAmount += hitCount;
            allTotalTime += end-start;

            if (!ignored.contains(queryString)) {
                uniqueTimeSpent.add(end-start);
                if (hitCount > 0) uniqueHitQueryNumber += 1;
                uniqueHitCounts.add((long) hitCount);
                uniqueTotalAmount += hitCount;
                uniqueTotalTime += end-start;
                ignored.add(queryString);
            }
        }

        Collections.sort(uniqueTimeSpent);
        Collections.sort(uniqueHitCounts);
        System.out.println("Average time spent for " + ignored.size() + " queries: " + (float)uniqueTotalTime / (float)ignored.size() + "ms");
        System.out.println("Total time P50 " + percentile(uniqueTimeSpent, 50) + " ms");
        System.out.println("Total time P99 " + percentile(uniqueTimeSpent, 99) + " ms");

        System.out.println("Average return size: " + uniqueTotalAmount / (float)ignored.size());
        System.out.println("Total hits P50 " + percentile(uniqueHitCounts, 50));
        System.out.println("Total hits P99 " + percentile(uniqueHitCounts, 99));

        System.out.println("Among " + ignored.size() + " queries, " + uniqueHitQueryNumber + " queries got result, which is " + (float)uniqueHitQueryNumber/ignored.size());
        System.out.println("-------------------------------------------------------------------------------------");

        Collections.sort(allTimeSpent);
        Collections.sort(allHitCounts);
        System.out.println("Average time spent for " + query.size() + " queries: " + (float)allTotalTime / (float)query.size() + "ms");
        System.out.println("Total time P50 " + percentile(allTimeSpent, 50) + " ms");
        System.out.println("Total time P99 " + percentile(allTimeSpent, 99) + " ms");

        System.out.println("Average return size: " + allTotalAmount / (float)query.size());
        System.out.println("Total hits P50 " + percentile(allHitCounts, 50));
        System.out.println("Total hits P99 " + percentile(allHitCounts, 99));

        System.out.println("Among " + query.size() + " queries, " + allHitQueryNumber + " queries got result, which is " + (float)allHitQueryNumber/query.size());
    }

    private long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index-1);
    }

    public void load(String index, int threads, int maxListSize) throws InterruptedException, IOException, ExecutionException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("ichiba.csv");
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
//        bulkRequest.timeout("1m");
        for (int i = 0 ; i < records.size(); i++) {
            if (i % threads == index) {
                CSVRecord record = records.get(i);
//                bulkRequest.add(new IndexRequest(indexName).source(XContentType.JSON, "title", record.get("title"), "description", record.get("description"), "second_category", record.get("second_category"), "third_category", record.get("third_category"), "image_link", record.get("image_link")));
                String click = record.get("click");
                if (click == null || click.equals("")) System.out.println("Empty Click");
                else
//                    bulkRequest.add(new IndexRequest(indexName).source(XContentType.JSON, "title", record.get("title"), "item_id", record.get("item_id"), "second_category", record.get("second_category"), "third_category", record.get("third_category"), "click_count", Integer.parseInt(click)));
                    bulkRequest.add(new IndexRequest(indexName).id(record.get("item_id")).source(XContentType.JSON, "title", record.get("title"), "second_category", record.get("second_category"), "third_category", record.get("third_category"), "click_count", Integer.parseInt(click)));
            }
            if (bulkRequest.numberOfActions() > 0 && bulkRequest.numberOfActions() % 2000 == 0) {
                System.out.println("Collect " + bulkRequest.numberOfActions() + " items, inserting...");
                int count = 0;
                int maxTries = 3;
                while (true) {
                    try {
                        client.bulk(bulkRequest, RequestOptions.DEFAULT);
                        bulkRequest = new BulkRequest();
                        break;
                    } catch (Exception e) {
                        if (++count == maxTries) {
                            System.out.println("Retry 3 times failed, program exit");
                            return;
                        }
                    }
                }
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            System.out.println("Last " + bulkRequest.numberOfActions() + " items, inserting...");
            highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        }
    }
}