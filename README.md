# Dashboard de Quebra de Funil Operacional — Olist

> **Vender muito não adianta se a operação trava na ponta.**

Dashboard interativo construído com **Python + Streamlit + SQLite** para analisar a "Quebra de Funil" entre a Venda Realizada (esforço do time comercial) e a Entrega/Instalação (gargalo da área técnica/logística), utilizando dados reais do e-commerce brasileiro Olist.

---

## Estrutura do Projeto

```
olist_dashboard/
├── data/                          ← Pasta dos CSVs (você cria)
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   └── olist_order_payments_dataset.csv
├── app.py                         ← Dashboard Streamlit (arquivo principal)
├── download_data.py               ← Script auxiliar para download dos dados
├── requirements.txt               ← Dependências Python
└── README.md                      ← Este arquivo
```

---

## Arquivos CSV Necessários

Você precisa de **3 arquivos** do dataset [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce):

| Arquivo | Descrição | Linhas aprox. |
|---------|-----------|---------------|
| `olist_orders_dataset.csv` | Pedidos com datas de compra, aprovação, entrega e status | ~100 mil |
| `olist_order_items_dataset.csv` | Itens dos pedidos com preço e frete | ~113 mil |
| `olist_order_payments_dataset.csv` | Pagamentos por pedido | ~104 mil |

---

## Passo a Passo para Rodar no seu Notebook (Windows)

### 1. Baixe os dados do Kaggle

Acesse: **https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce**

Clique em **Download**, extraia o ZIP e copie os 3 CSVs listados acima para a pasta `data/` dentro do projeto.

### 2. Instale as dependências

Abra o terminal (CMD ou PowerShell) na pasta do projeto e execute:

```bash
pip install -r requirements.txt
```

### 3. Execute o dashboard

```bash
streamlit run app.py
```

O dashboard abrirá automaticamente no navegador em **http://localhost:8501**.

---

## Resumo em 4 Comandos (copie e cole)

```bash
cd olist_dashboard
pip install -r requirements.txt
# (coloque os CSVs na pasta data/)
streamlit run app.py
```

---

## O que o Dashboard Mostra

O pipeline completo roda dentro do `app.py`:

1. **Importação** dos CSVs com Pandas
2. **Limpeza** de datas e valores nulos
3. **Carga em SQLite** em memória (banco relacional)
4. **Query SQL** com JOINs que classifica cada pedido em 3 buckets:
   - **Entregue no Prazo** — operação de sucesso
   - **Entregue com Atraso** — operação com atrito
   - **Cancelado / Indisponível** — quebra total do funil
5. **Dashboard** com:
   - 4 KPIs executivos (Receita Total, Receita no Prazo, Receita Comprometida, Receita Perdida)
   - Gráfico de Funil (Pedidos Realizados → Entregues → Atrasados → Cancelados)
   - Gráfico de Barras (Receita por Status Operacional)
   - Gráfico Donut (Distribuição percentual)
   - Tabela Resumo Executiva

---

## Tecnologias Utilizadas

- **Python 3.10+**
- **Pandas** — manipulação de dados
- **SQLite3** — banco relacional em memória (built-in do Python)
- **Streamlit** — framework de dashboard
- **Plotly** — gráficos interativos

---

*Desenvolvido como laboratório de análise de dados para demonstrar habilidades de cruzamento de dados (JOINs), SQL e visualização de Business Intelligence.*
