import com.smartnews.ad.dynamic.elasticsearch.utils.Client;
import org.junit.Test;


import java.io.IOException;

public class EsTest {

    private final Client client = new Client();

    @Test
    public void loadTest() throws IOException, InterruptedException {
        client.loadData("test-2", 5);
    }
}