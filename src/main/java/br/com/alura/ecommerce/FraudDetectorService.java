package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudeService = new FraudDetectorService();
        try (var service = new KafkaService<>(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER", fraudeService::parse, Order.class,
                Map.of())) {
            service.run();
        }

    }


    public void parse(ConsumerRecord<String, Order> record) {
        System.out.println("---------------------------------");
        System.out.println("Processing new order, checking for fraude");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }
}
