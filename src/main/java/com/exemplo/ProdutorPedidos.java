package com.exemplo;

import com.exemplo.modelo.Pedido;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class ProdutorPedidos {
    private static final String FILA = "pedidos_restaurante";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(FILA, false, false, false, null);
            Scanner scanner = new Scanner(System.in);
            ObjectMapper mapper = new ObjectMapper();

            while (true) {
                System.out.print("Digite o prato (ou 'sair' para encerrar): ");
                String prato = scanner.nextLine();
                if (prato.equalsIgnoreCase("sair")) break;

                System.out.print("Digite o número da mesa: ");
                int mesa = Integer.parseInt(scanner.nextLine());

                System.out.print("Digite a prioridade (normal/urgente): ");
                String prioridade = scanner.nextLine();

                Pedido pedido = new Pedido(prato, mesa, prioridade);
                String json = mapper.writeValueAsString(pedido);

                channel.basicPublish("", FILA, null, json.getBytes());
                System.out.println("[x] Enviado: " + json);
            }
        }
    }
}
