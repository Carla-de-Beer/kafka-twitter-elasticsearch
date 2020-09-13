package dev.cadebe.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private final List<String> TERMS = Lists.newArrayList("corona", "news", "fake", "money");
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private static String consumerKey;
    private static String consumerSecret;
    private static String token;
    private static String secret;
    private static String bootstrapServer;
    private static String topic;
    private static String acksConfig;
    private static String enableIdempotence;
    private static String compressionType;
    private static String lingerMs;
    private static String maxInFlightRequestsPerConnection;

    public static void main(String[] args) {
        setProperties();
        new TwitterProducer().run();
    }

    public void run() {
        // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        // Create twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // Create kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping client");
            client.stop();
            logger.info("Stopping producer");
            producer.close();
        }));

        // Loop to send tweets to kafka
        while (!client.isDone()) {
            String msg;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    logger.info(msg);
                    System.out.println(msg);
                    producer.send(new ProducerRecord<>(topic, null, msg), (recordMetadata, e) -> {
                        if (e != null) {
                            logger.error("Error: " + e.getMessage());
                        }
                    });
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                client.stop();
            }
        }
        logger.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hoseBirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hoseBirdEndpoint = new StatusesFilterEndpoint();

        hoseBirdEndpoint.trackTerms(TERMS);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("HoseBird-Client-01")
                .hosts(hoseBirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hoseBirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        // Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, acksConfig);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection);

        // Make producer high-throughput
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, lingerMs);

        return new KafkaProducer<>(properties);
    }

    public static void setProperties() {
        Properties prop = loadPropertiesFile("application.properties");

        consumerKey = prop.getProperty("consumer.key");
        consumerSecret = prop.getProperty("consumer.secret");
        token = prop.getProperty("token");
        secret = prop.getProperty("secret");
        bootstrapServer = prop.getProperty("bootstrapServer");
        topic = prop.getProperty("topic");
        enableIdempotence = prop.getProperty("enable.idempotence");
        acksConfig = prop.getProperty("acks.config");
        compressionType = prop.getProperty("compression.type");
        maxInFlightRequestsPerConnection = prop.getProperty("max.in.flight.requests.per.connection");
        lingerMs = prop.getProperty("linger.ms");
    }

    private static Properties loadPropertiesFile(String filePath) {
        Properties properties = new Properties();
        try (InputStream resourceAsStream = TwitterProducer.class.getClassLoader().getResourceAsStream(filePath)) {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            System.err.println("Unable to load properties file: " + filePath);
        }
        return properties;
    }

}
