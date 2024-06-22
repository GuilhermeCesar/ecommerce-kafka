package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrdermain {

    public static void main(String[] args) {
        var producer = new KafkaProducer<String, String>(properties());

        try (producer) {
            var value = "123123,145646544,465456445";
            var records = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
            producer
                    .send(records, (data, ex) -> {
                        if (ex != null) {
                            ex.printStackTrace();
                            return;
                        }
                        System.out.println("Sucesso enviando " +
                                data.topic() + ":::partition " + data.partition()
                                + "/ offset " + data.offset() + "/ timestemp " + data.timestamp()
                        );
                    })
                    .get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
