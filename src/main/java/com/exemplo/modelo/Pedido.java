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

    public String getPrato() {
        return prato;
    }

    public void setPrato(String prato) {
        this.prato = prato;
    }

    public int getMesa() {
        return mesa;
    }

    public void setMesa(int mesa) {
        this.mesa = mesa;
    }

    public String getPrioridade() {
        return prioridade;
    }

    public void setPrioridade(String prioridade) {
        this.prioridade = prioridade;
    }

    @Override
    public String toString() {
        return "Pedido{" +
                "prato='" + prato + '\'' +
                ", mesa=" + mesa +
                ", prioridade='" + prioridade + '\'' +
                '}';
    }
}
