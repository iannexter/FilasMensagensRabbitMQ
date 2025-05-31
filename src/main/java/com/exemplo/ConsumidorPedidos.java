package com.exemplo;

import com.exemplo.modelo.Pedido;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ConsumidorPedidos {
    private static final String EXCHANGE = "pedidos_exchange";
    private static final String DLX = "dlx_pedidos";

    private static final String FILA_NORMAL = "fila_pedidos_normal";
    private static final String FILA_URGENTE = "fila_pedidos_urgente";
    private static final String DLQ = "dlq_pedidos"; // Dead Letter Queue única para erros JSON

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // exchanges duráveis
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
            channel.exchangeDeclare(DLX, BuiltinExchangeType.FANOUT, true);

            // fila DLQ (durável)
            channel.queueDeclare(DLQ, true, false, false, null);
            channel.queueBind(DLQ, DLX, "");

            //filas principais com DLX configurado e duráveis
            Map<String, Object> argsNormal = new HashMap<>();
            argsNormal.put("x-dead-letter-exchange", DLX);
            channel.queueDeclare(FILA_NORMAL, true, false, false, argsNormal);
            channel.queueBind(FILA_NORMAL, EXCHANGE, "pedidos.normal");

            Map<String, Object> argsUrgente = new HashMap<>();
            argsUrgente.put("x-dead-letter-exchange", DLX);
            channel.queueDeclare(FILA_URGENTE, true, false, false, argsUrgente);
            channel.queueBind(FILA_URGENTE, EXCHANGE, "pedidos.urgente");

            ObjectMapper mapper = new ObjectMapper();

            System.out.println("[*] Aguardando pedidos (urgente e normal)...");

            while (true) {
                GetResponse urgenteMsg = channel.basicGet(FILA_URGENTE, false);
                if (urgenteMsg != null) {
                    String json = new String(urgenteMsg.getBody(), StandardCharsets.UTF_8);
                    try {
                        Pedido pedido = mapper.readValue(json, Pedido.class);
                        System.out.printf("[URGENTE] %s%n", pedido);
                        Thread.sleep(1000);
                        System.out.printf("[x] Pedido URGENTE '%s' concluído!%n", pedido.getPrato());
                        channel.basicAck(urgenteMsg.getEnvelope().getDeliveryTag(), false);
                    } catch (Exception e) {
                        System.err.println("[!] JSON inválido no pedido urgente. Enviando para DLQ.");
                        // Nack com requeue = false envia para DLX (DLQ)
                        channel.basicNack(urgenteMsg.getEnvelope().getDeliveryTag(), false, false);
                    }
                    continue; // Para não consumir fila normal quando urgente tiver mensagem
                }

                GetResponse normalMsg = channel.basicGet(FILA_NORMAL, false);
                if (normalMsg != null) {
                    String json = new String(normalMsg.getBody(), StandardCharsets.UTF_8);
                    try {
                        Pedido pedido = mapper.readValue(json, Pedido.class);
                        System.out.printf("[NORMAL] %s%n", pedido);
                        Thread.sleep(3000);
                        System.out.printf("[x] Pedido NORMAL '%s' concluído!%n", pedido.getPrato());
                        channel.basicAck(normalMsg.getEnvelope().getDeliveryTag(), false);
                    } catch (Exception e) {
                        System.err.println("[!] JSON inválido no pedido normal. Enviando para DLQ.");
                        channel.basicNack(normalMsg.getEnvelope().getDeliveryTag(), false, false);
                    }
                } else {
                    Thread.sleep(500); // Espera para não consumir CPU demais
                }
            }
        }
    }
}
