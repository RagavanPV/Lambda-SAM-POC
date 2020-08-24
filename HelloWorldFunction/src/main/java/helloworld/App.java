package helloworld;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.json.JSONObject;
import org.json.XML;

/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private Logger logger = Logger.getLogger(App.class.getName());
    private AmazonS3Client s3;
    String ACCESS_KEY = System.getenv("AWS_ACCESS_KEY");
    String SECRET_KEY = System.getenv("AWS_SECRET_KEY");
    public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");
        init(ACCESS_KEY,SECRET_KEY);
        logger.info(String.format("Started parsing file %s",input.getBody()));
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent()
                .withHeaders(headers);
        try {
            String json = convertToJSON(readFromS3(System.getenv("AWS_S3_BUCKET"),input.getBody()));
            return response
                    .withStatusCode(200)
                    .withBody(json);
        } catch (IOException e) {
            return response
                    .withBody("{}")
                    .withStatusCode(500);
        }
    }

    private String convertToJSON(String xmlString) {
        JSONObject json = XML.toJSONObject(xmlString);
        return json.toString();
    }

    /**
     * Read a character oriented file from S3
     *
     * @param bucketName  Name of bucket
     * @param key         File Name
     * @throws IOException
     */
    public String readFromS3(String bucketName, String key) throws IOException {
        S3Object s3object = s3.getObject(new GetObjectRequest(
                bucketName, key));
        BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
        String output = reader.lines().collect(Collectors.joining());
        return output;
    }

    public void init(String accessKey, String secretKey) {
        s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    }
}
