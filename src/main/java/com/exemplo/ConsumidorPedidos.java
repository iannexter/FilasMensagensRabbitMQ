package com.exemplo;

import com.exemplo.modelo.Pedido;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

public class ConsumidorPedidos {
    private static final String FILA = "pedidos_restaurante";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(FILA, false, false, false, null);
        channel.basicQos(1); // processar um por vez

        System.out.println("[*] Aguardando pedidos. Pressione CTRL+C para sair");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            ObjectMapper mapper = new ObjectMapper();
            String json = new String(delivery.getBody(), "UTF-8");

            try {
                Pedido pedido = mapper.readValue(json, Pedido.class);
                System.out.println("[x] Recebido: " + pedido);

                System.out.printf("[x] Processando pedido: %s (Mesa %d)%n", pedido.getPrato(), pedido.getMesa());

                // Simula tempo de preparo baseado na prioridade
                if ("urgente".equalsIgnoreCase(pedido.getPrioridade())) {
                    Thread.sleep(1000);
                } else {
                    Thread.sleep(3000);
                }

                System.out.printf("[x] Pedido '%s' concluído!%n", pedido.getPrato());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                System.err.println("Erro ao processar pedido: " + e.getMessage());
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
            }
        };

        boolean autoAck = false; // confirmação manual
        channel.basicConsume(FILA, autoAck, deliverCallback, consumerTag -> {});
    }
}
