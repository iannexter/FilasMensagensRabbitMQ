package com.exemplo;

import com.exemplo.modelo.Pedido;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.BuiltinExchangeType;

import com.opencsv.CSVReader;

import java.io.FileReader;
import java.util.List;

public class ProdutorPedidos {
    private static final String EXCHANGE = "pedidos_exchange";
    private static final String ROUTING_KEY = "pedidos";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT);

            ObjectMapper mapper = new ObjectMapper();

            // dentro da pasta
            String arquivoCsv = "pedidos.csv";

            try (CSVReader reader = new CSVReader(new FileReader(arquivoCsv))) {
                List<String[]> linhas = reader.readAll();

                
                for (int i = 1; i < linhas.size(); i++) {
                    String[] linha = linhas.get(i);
                    String prato = linha[0];
                    int mesa = Integer.parseInt(linha[1]);
                    String prioridade = linha[2];

                    Pedido pedido = new Pedido(prato, mesa, prioridade);
                    String json = mapper.writeValueAsString(pedido);

                    channel.basicPublish(EXCHANGE, ROUTING_KEY, null, json.getBytes());
                    System.out.println("[x] Enviado: " + json);
                }
            }
        }
    }
}
