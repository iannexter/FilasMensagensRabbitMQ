package com.exemplo;

import com.exemplo.modelo.Pedido;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;

public class ProdutorPedidos {
    private static final String EXCHANGE = "pedidos_exchange";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Exchange com durable=true
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);

            ObjectMapper mapper = new ObjectMapper();

            // Lê o arquivo CSV (ajuste o caminho se necessário)
            try (BufferedReader br = new BufferedReader(new FileReader("pedidos.csv"))) {
                String linha;
                br.readLine(); // Pula o cabeçalho

                while ((linha = br.readLine()) != null) {
                    String[] partes = linha.split(",");
                    if (partes.length == 3) {
                        String prato = partes[0].trim();
                        int mesa = Integer.parseInt(partes[1].trim());
                        String prioridade = partes[2].trim().toLowerCase();

                        Pedido pedido = new Pedido(prato, mesa, prioridade);
                        String json = mapper.writeValueAsString(pedido);

                        String routingKey;
                        if ("urgente".equals(prioridade)) {
                            routingKey = "pedidos.urgente";
                        } else {
                            routingKey = "pedidos.normal";
                        }

                        channel.basicPublish(EXCHANGE, routingKey, null, json.getBytes(StandardCharsets.UTF_8));
                        System.out.printf("[x] Pedido enviado: %s (%s)%n", prato, prioridade);
                    }
                }
            }
        }
    }
}
