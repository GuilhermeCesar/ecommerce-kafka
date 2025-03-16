package br.com.alura.ecommerce;

import org.apache.kafka.common.protocol.types.Field;

public class Email {


    private final String subject, body;

    public Email(String subject, String body) {
        this.subject = subject;
        this.body = body;
    }
}
