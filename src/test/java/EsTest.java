import com.smartnews.ad.dynamic.elasticsearch.utils.Client;
import org.junit.Test;


import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class EsTest {

    private final Client client = new Client();

    @Test
    public void loadTest() throws IOException, InterruptedException, ExecutionException {
        client.load("ichiba-2", 10, 100000);
    }

    @Test
    public void queryTest() throws IOException {
        client.queryTest("ichiba-2", 500);
    }
}