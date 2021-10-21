import com.smartnews.ad.dynamic.elasticsearch.utils.Client;
import org.junit.Test;


import java.io.IOException;

public class EsTest {

    private final Client client = new Client();

    @Test
    public void loadTest() throws IOException, InterruptedException {
        client.loadData("test-2", 5);
    }

    @Test
    public void queryTest() throws IOException {
        client.queryTest("test-4", 50);
    }
}