package com.exemplo;

import com.exemplo.modelo.Pedido;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConsumidorPedidos {
    private static final String EXCHANGE = "pedidos_exchange";
    private static final String DLX = "dlx_pedidos";

    private static final String FILA_NORMAL = "fila_pedidos_normal";
    private static final String FILA_URGENTE = "fila_pedidos_urgente";
    private static final String DLQ_NORMAL = "dlq_pedidos_normal";
    private static final String DLQ_URGENTE = "dlq_pedidos_urgente";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Declara as exchanges com durable=true
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
            channel.exchangeDeclare(DLX, BuiltinExchangeType.FANOUT, true);

            // Declara DLQs como duráveis (durable=true)
            channel.queueDeclare(DLQ_NORMAL, true, false, false, null);
            channel.queueBind(DLQ_NORMAL, DLX, "");

            channel.queueDeclare(DLQ_URGENTE, true, false, false, null);
            channel.queueBind(DLQ_URGENTE, DLX, "");

            // Declara filas principais com DLX e durable=true
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
                    String json = new String(urgenteMsg.getBody(), "UTF-8");
                    try {
                        Pedido pedido = mapper.readValue(json, Pedido.class);
                        System.out.printf("[URGENTE] %s%n", pedido);
                        Thread.sleep(1000);
                        System.out.printf("[x] Pedido URGENTE '%s' concluído!%n", pedido.getPrato());
                        channel.basicAck(urgenteMsg.getEnvelope().getDeliveryTag(), false);
                    } catch (Exception e) {
                        System.err.println("[!] Erro ao processar pedido urgente.");
                        channel.basicNack(urgenteMsg.getEnvelope().getDeliveryTag(), false, false);
                    }
                } else {
                    GetResponse normalMsg = channel.basicGet(FILA_NORMAL, false);
                    if (normalMsg != null) {
                        String json = new String(normalMsg.getBody(), "UTF-8");
                        try {
                            Pedido pedido = mapper.readValue(json, Pedido.class);
                            System.out.printf("[NORMAL] %s%n", pedido);
                            Thread.sleep(3000);
                            System.out.printf("[x] Pedido NORMAL '%s' concluído!%n", pedido.getPrato());
                            channel.basicAck(normalMsg.getEnvelope().getDeliveryTag(), false);
                        } catch (Exception e) {
                            System.err.println("[!] Erro ao processar pedido normal.");
                            channel.basicNack(normalMsg.getEnvelope().getDeliveryTag(), false, false);
                        }
                    } else {
                        Thread.sleep(500); // Aguarda um pouco antes de tentar novamente
                    }
                }
            }
        }
    }
}
