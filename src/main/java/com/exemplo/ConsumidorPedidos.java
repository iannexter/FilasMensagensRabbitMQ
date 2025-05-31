package com.exemplo;

import com.exemplo.modelo.Pedido;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class ConsumidorPedidos {
    private static final String EXCHANGE = "pedidos_exchange";
    private static final String ROUTING_KEY = "pedidos";
    private static final String FILA = "pedidos_restaurante";

    private static final String DLX = "dlx_pedidos";
    private static final String FILA_DLQ = "dlq_pedidos";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Declara o exchange principal (direct)
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT);

        // Declara o exchange da DLQ
        channel.exchangeDeclare(DLX, BuiltinExchangeType.FANOUT);

        // Declara a fila de Dead Letter
        channel.queueDeclare(FILA_DLQ, false, false, false, null);
        channel.queueBind(FILA_DLQ, DLX, "");

        // Argumentos da fila principal para definir a DLX
        Map<String, Object> argsFila = new HashMap<>();
        argsFila.put("x-dead-letter-exchange", DLX);

        // Declara a fila principal com DLX configurada
        channel.queueDeclare(FILA, false, false, false, argsFila);

        // Faz binding da fila principal ao exchange com a routing key
        channel.queueBind(FILA, EXCHANGE, ROUTING_KEY);

        //channel.basicQos(1); // distribuir em muitos consumidores

        System.out.println("[*] Aguardando pedidos...");

        ObjectMapper mapper = new ObjectMapper();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String json = new String(delivery.getBody(), "UTF-8");

            try {
                Pedido pedido = mapper.readValue(json, Pedido.class);
                System.out.printf("[x] Recebido: %s%n", pedido);

                if ("urgente".equalsIgnoreCase(pedido.getPrioridade())) {
                    Thread.sleep(1000); // Processamento mais rápido
                } else {
                    Thread.sleep(3000); // Processamento normal
                }

                System.out.printf("[x] Pedido '%s' concluído!%n", pedido.getPrato());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

            } catch (Exception e) {
                System.err.println("Erro ao processar pedido: " + e.getMessage());
                // Envia para a DLQ ao fazer Nack sem reencaminhamento
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
            }
        };

        boolean autoAck = false; // Controle manual de confirmação
        channel.basicConsume(FILA, autoAck, deliverCallback, consumerTag -> {});
    }
}
