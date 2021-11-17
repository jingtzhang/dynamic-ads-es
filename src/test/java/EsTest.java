import com.smartnews.ad.dynamic.elasticsearch.utils.Client;
import com.smartnews.ad.dynamic.elasticsearch.utils.HttpClient;
import org.junit.Test;


import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class EsTest {

    private final Client client = new Client();
    private final HttpClient httpClient = new HttpClient();

    @Test
    public void loadTest() throws IOException, InterruptedException, ExecutionException {
        client.load("ichiba-2", 10, 100000);
    }

    @Test
    public void queryTest() throws IOException {
        client.queryTest("ichiba-2", 500);
    }

    @Test
    public void indexSwitchTest() throws IOException, InterruptedException {
        client.keepQueryEs("rakuten", 500);
    }

    @Test
    public void testHttp() throws IOException {
        httpClient.query();
    }

    @Test
    public void intensiveTest() throws IOException, InterruptedException {
        httpClient.intensive_test();
    }
}