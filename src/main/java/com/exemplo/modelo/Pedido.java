package com.exemplo.modelo;

public class Pedido {
    private String prato;
    private int mesa;
    private String prioridade;

    public Pedido() {} // Necessário para Jackson

    public Pedido(String prato, int mesa, String prioridade) {
        this.prato = prato;
        this.mesa = mesa;
        this.prioridade = prioridade;
    }

    public String getPrato() { return prato; }
    public int getMesa() { return mesa; }
    public String getPrioridade() { return prioridade; }

    @Override
    public String toString() {
        return "Pedido{" +
                "prato='" + prato + '\'' +
                ", mesa=" + mesa +
                ", prioridade='" + prioridade + '\'' +
                '}';
    }
}
