package ir.keyvan.springboot;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;


@Service
public class WikiMediaChangesProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikiMediaChangesProducer .class);


    private KafkaTemplate<String, String> kafkaTemplate;

    public WikiMediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    public void sendMessage () throws InterruptedException {
        String topic = "wikimedia_recentChange";
        EventHandler eventListener = new WikiMediaChangesHandler(kafkaTemplate, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(eventListener, URI.create(url));

        EventSource eventSource = builder.build();

        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
