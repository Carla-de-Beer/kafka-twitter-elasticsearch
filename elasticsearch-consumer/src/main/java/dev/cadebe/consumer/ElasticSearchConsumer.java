package dev.cadebe.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private static String hostname; // localhost or bonsai url
    private static String username; // needed only for bonsai
    private static String password; // needed only for bonsai
    private static String bootstrapServer;
    private static String port;
    private static String scheme;
    private static String index;
    private static String type;
    private static String groupId;
    private static String topic;
    private static String autoOffsetReset;
    private static String enableAutoCommit;
    private static String maxPollRecords;

    public static void main(String[] args) throws IOException {
        setProperties();

        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            logger.info("Received " + records.count() + " records");

            for (ConsumerRecord<String, String> record : records) {
                try {
                    // Insert data into ElasticSearch
                    String jsonString = record.value();
                    String id = extractTweetId(jsonString);

                    IndexRequest request = new IndexRequest(index, type);
                    // Use id to make the consumer idempotent
                    request.id(id);
                    request.source(jsonString, XContentType.JSON);

                    IndexResponse response = client.index(request, RequestOptions.DEFAULT);
                    System.out.println(response.getId());
                    logger.info("ID: " + response.getId());
                } catch (NullPointerException e) {
                    logger.warn("Skipping bad data: " + record.value());
                }

                // Committing offsets
                consumer.commitSync();

                logger.info("Key: " + record.key() + "\n" +
                        "Value: " + record.value() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n");
            }
        }
    }

    public static KafkaConsumer<String, String> createConsumer() {
        // Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit); // disable auto-commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }

    public static RestHighLevelClient createClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, Integer.parseInt(port), scheme))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

    private static String extractTweetId(String jsonTweet) {
        return JsonParser.parseString(jsonTweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void setProperties() {
        Properties prop = loadPropertiesFile("application.properties");

        hostname = prop.getProperty("hostname");
        username = prop.getProperty("username");
        password = prop.getProperty("password");
        bootstrapServer = prop.getProperty("bootstrapServer");
        port = prop.getProperty("port");
        scheme = prop.getProperty("scheme");
        index = prop.getProperty("index");
        type = prop.getProperty("type");
        groupId = prop.getProperty("groupId");
        topic = prop.getProperty("topic");
        autoOffsetReset = prop.getProperty("auto.offset.reset");
        enableAutoCommit = prop.getProperty("enable.auto.commit");
        maxPollRecords = prop.getProperty("max.poll.records");
    }

    private static Properties loadPropertiesFile(String filePath) {
        Properties properties = new Properties();
        try (InputStream resourceAsStream = ElasticSearchConsumer.class.getClassLoader().getResourceAsStream(filePath)) {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            System.err.println("Unable to load properties file: " + filePath);
        }
        return properties;
    }
}
