package com.exemplo;

import com.exemplo.modelo.Pedido;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import com.rabbitmq.client.BuiltinExchangeType;


import java.util.Scanner;

public class ProdutorPedidos {
    private static final String EXCHANGE = "pedidos_exchange";
    private static final String ROUTING_KEY = "pedidos";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            // Declara o exchange (direct)
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT);

            Scanner scanner = new Scanner(System.in);
            ObjectMapper mapper = new ObjectMapper();

            while (true) {
                System.out.print("Digite o prato (ou 'sair'): ");
                String prato = scanner.nextLine();
                if (prato.equalsIgnoreCase("sair")) break;

                System.out.print("Número da mesa: ");
                int mesa = Integer.parseInt(scanner.nextLine());

                System.out.print("Prioridade (normal/urgente): ");
                String prioridade = scanner.nextLine();

                Pedido pedido = new Pedido(prato, mesa, prioridade);
                String json = mapper.writeValueAsString(pedido);

                // Publica no exchange com chave de roteamento
                channel.basicPublish(EXCHANGE, ROUTING_KEY, null, json.getBytes());
                System.out.println("[x] Enviado: " + json);
            }
        }
    }
}
