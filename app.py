"""
=============================================================================
 Dashboard de Análise de Quebra de Funil Operacional — Nível Diretoria
 Base de Dados: Olist Brazilian E-Commerce (Kaggle)
 Autor: Lead Data Analyst — S&OP / Business Intelligence
 Objetivo: Provar que vender muito não adianta se a operação trava na ponta.
 Versão: 2.1 — Visões Avançadas com UX/UI e Tipografia Otimizadas
=============================================================================
"""

import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta, date

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DA PÁGINA
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Quebra de Funil Operacional — Olist",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# TEMA DARK EXECUTIVO — CSS customizado
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    /* Fundo geral */
    .stApp {
        background-color: #0e1117;
        color: #fafafa;
    }
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #161b22;
    }
    /* Cards de métrica */
    div[data-testid="stMetric"] {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 16px 20px;
    }
    div[data-testid="stMetric"] label {
        color: #8b949e;
        font-size: 1.25rem; /* Aumentado */
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #fafafa;
        font-weight: 700;
        font-size: 2rem; /* Aumentado */
    }
    /* Separadores */
    hr {
        border-color: #30363d;
    }
    /* Subheaders */
    .stSubheader, h2, h3 {
        color: #fafafa !important;
    }
    /* Dataframe */
    .stDataFrame {
        border: 1px solid #30363d;
        border-radius: 8px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────
DATA_PATH = os.path.join(os.path.dirname(__file__), "data")

REQUIRED_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
]

# Arquivo opcional para análise regional
CUSTOMERS_FILE = "olist_customers_dataset.csv"

# Template Plotly para visual executivo dark
PLOTLY_TEMPLATE = "plotly_dark"

# Paleta de cores para os buckets de status
COLOR_MAP = {
    "Entregue no Prazo": "#2ecc71",
    "Entregue com Atraso": "#f39c12",
    "Cancelado / Indisponível": "#e74c3c",
    "Em Trânsito / Outros": "#636e72",
}

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 1 — VERIFICAÇÃO DOS ARQUIVOS CSV
# ─────────────────────────────────────────────────────────────────────────────
def check_data_files() -> bool:
    """Verifica se todos os CSVs obrigatórios existem na pasta data/."""
    missing = [f for f in REQUIRED_FILES if not os.path.isfile(os.path.join(DATA_PATH, f))]
    if missing:
        st.error("⚠️ Arquivos CSV não encontrados na pasta `data/`.")
        st.markdown(
            "**Como resolver:**\n"
            "1. Acesse [Olist Brazilian E-Commerce no Kaggle]"
            "(https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)\n"
            "2. Faça o download do ZIP e extraia os CSVs\n"
            f"3. Copie os arquivos abaixo para a pasta `{DATA_PATH}/`:\n"
        )
        for f in missing:
            st.code(f, language="text")
        st.info("💡 Para a análise regional (Heatmap por UF), inclua também: `olist_customers_dataset.csv`")
        st.stop()
    return True


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 2 — CARGA, LIMPEZA E MODELO SQL (SQLite em memória)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="🔄 Carregando e processando dados…")
def load_and_process_data():
    # --- 1. Importação dos CSVs ---
    orders = pd.read_csv(os.path.join(DATA_PATH, "olist_orders_dataset.csv"))
    items = pd.read_csv(os.path.join(DATA_PATH, "olist_order_items_dataset.csv"))
    payments = pd.read_csv(os.path.join(DATA_PATH, "olist_order_payments_dataset.csv"))

    # Tentar carregar customers (opcional — para análise regional)
    customers_path = os.path.join(DATA_PATH, CUSTOMERS_FILE)
    has_customers = os.path.isfile(customers_path)
    if has_customers:
        customers = pd.read_csv(customers_path)
    else:
        customers = None

    # --- 2. Limpeza e tratamento de datas ---
    date_cols_orders = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in date_cols_orders:
        orders[col] = pd.to_datetime(orders[col], errors="coerce")

    items["shipping_limit_date"] = pd.to_datetime(items["shipping_limit_date"], errors="coerce")

    orders["order_approved_at"] = orders["order_approved_at"].fillna(
        orders["order_purchase_timestamp"]
    )

    # --- 3. Subir para SQLite em memória ---
    conn = sqlite3.connect(":memory:")
    orders.to_sql("orders", conn, index=False, if_exists="replace")
    items.to_sql("order_items", conn, index=False, if_exists="replace")
    payments.to_sql("order_payments", conn, index=False, if_exists="replace")
    if has_customers:
        customers.to_sql("customers", conn, index=False, if_exists="replace")

    # --- 4. Query SQL Principal — Detalhe com Buckets ---
    query_detail = """
    WITH pedido_receita AS (
        SELECT
            o.order_id,
            o.customer_id,
            o.order_status,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            COALESCE(SUM(oi.price + oi.freight_value), 0) AS receita,
            COALESCE(SUM(oi.price), 0) AS receita_produto,
            COALESCE(SUM(oi.freight_value), 0) AS receita_frete
        FROM orders AS o
        LEFT JOIN order_items AS oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, o.customer_id, o.order_status,
                 o.order_purchase_timestamp, o.order_approved_at,
                 o.order_delivered_customer_date, o.order_estimated_delivery_date
    )
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        receita,
        receita_produto,
        receita_frete,
        CASE
            WHEN order_status IN ('canceled', 'unavailable')
                THEN 'Cancelado / Indisponível'
            WHEN order_delivered_customer_date IS NOT NULL
                 AND order_delivered_customer_date <= order_estimated_delivery_date
                THEN 'Entregue no Prazo'
            WHEN order_delivered_customer_date IS NOT NULL
                 AND order_delivered_customer_date > order_estimated_delivery_date
                THEN 'Entregue com Atraso'
            ELSE 'Em Trânsito / Outros'
        END AS bucket,
        CASE
            WHEN order_delivered_customer_date IS NOT NULL
                 AND order_estimated_delivery_date IS NOT NULL
                THEN ROUND(JULIANDAY(order_delivered_customer_date)
                           - JULIANDAY(order_estimated_delivery_date), 1)
            ELSE NULL
        END AS dias_atraso
    FROM pedido_receita
    """
    df_detail = pd.read_sql_query(query_detail, conn)

    for col in ["order_purchase_timestamp", "order_approved_at",
                "order_delivered_customer_date", "order_estimated_delivery_date"]:
        df_detail[col] = pd.to_datetime(df_detail[col], errors="coerce")

    # --- 5. Query Agregada para KPIs ---
    query_summary = """
    WITH pedido_receita AS (
        SELECT
            o.order_id,
            o.order_status,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            COALESCE(SUM(oi.price + oi.freight_value), 0) AS receita
        FROM orders AS o
        LEFT JOIN order_items AS oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, o.order_status,
                 o.order_delivered_customer_date, o.order_estimated_delivery_date
    ),
    classificado AS (
        SELECT *,
            CASE
                WHEN order_status IN ('canceled', 'unavailable')
                    THEN 'Cancelado / Indisponível'
                WHEN order_delivered_customer_date IS NOT NULL
                     AND order_delivered_customer_date <= order_estimated_delivery_date
                    THEN 'Entregue no Prazo'
                WHEN order_delivered_customer_date IS NOT NULL
                     AND order_delivered_customer_date > order_estimated_delivery_date
                    THEN 'Entregue com Atraso'
                ELSE 'Em Trânsito / Outros'
            END AS bucket
        FROM pedido_receita
    )
    SELECT
        bucket,
        COUNT(order_id) AS total_pedidos,
        ROUND(SUM(receita), 2) AS receita_total
    FROM classificado
    GROUP BY bucket
    ORDER BY
        CASE bucket
            WHEN 'Entregue no Prazo'        THEN 1
            WHEN 'Entregue com Atraso'       THEN 2
            WHEN 'Cancelado / Indisponível'  THEN 3
            ELSE 4
        END
    """
    df_summary = pd.read_sql_query(query_summary, conn)

    # --- 6. Query Regional (se customers disponível) ---
    df_regional = None
    if has_customers:
        query_regional = """
        WITH pedido_receita AS (
            SELECT
                o.order_id,
                o.customer_id,
                o.order_status,
                o.order_delivered_customer_date,
                o.order_estimated_delivery_date,
                COALESCE(SUM(oi.price + oi.freight_value), 0) AS receita
            FROM orders AS o
            LEFT JOIN order_items AS oi ON o.order_id = oi.order_id
            GROUP BY o.order_id, o.customer_id, o.order_status,
                     o.order_delivered_customer_date, o.order_estimated_delivery_date
        ),
        classificado AS (
            SELECT pr.*,
                c.customer_state,
                c.customer_city,
                CASE
                    WHEN pr.order_status IN ('canceled', 'unavailable')
                        THEN 'Cancelado / Indisponível'
                    WHEN pr.order_delivered_customer_date IS NOT NULL
                         AND pr.order_delivered_customer_date <= pr.order_estimated_delivery_date
                        THEN 'Entregue no Prazo'
                    WHEN pr.order_delivered_customer_date IS NOT NULL
                         AND pr.order_delivered_customer_date > pr.order_estimated_delivery_date
                        THEN 'Entregue com Atraso'
                    ELSE 'Em Trânsito / Outros'
                END AS bucket
            FROM pedido_receita AS pr
            LEFT JOIN customers AS c ON pr.customer_id = c.customer_id
        )
        SELECT
            customer_state AS uf,
            COUNT(order_id) AS total_pedidos,
            SUM(CASE WHEN bucket = 'Entregue no Prazo' THEN 1 ELSE 0 END) AS no_prazo,
            SUM(CASE WHEN bucket = 'Entregue com Atraso' THEN 1 ELSE 0 END) AS com_atraso,
            SUM(CASE WHEN bucket = 'Cancelado / Indisponível' THEN 1 ELSE 0 END) AS cancelados,
            ROUND(SUM(receita), 2) AS receita_total,
            ROUND(SUM(CASE WHEN bucket = 'Entregue com Atraso' THEN receita ELSE 0 END), 2) AS receita_atraso,
            ROUND(SUM(CASE WHEN bucket = 'Cancelado / Indisponível' THEN receita ELSE 0 END), 2) AS receita_cancelada
        FROM classificado
        WHERE customer_state IS NOT NULL
        GROUP BY customer_state
        ORDER BY total_pedidos DESC
        """
        df_regional = pd.read_sql_query(query_regional, conn)
        df_regional["pct_atraso"] = (df_regional["com_atraso"] / df_regional["total_pedidos"] * 100).round(1)
        df_regional["pct_cancelado"] = (df_regional["cancelados"] / df_regional["total_pedidos"] * 100).round(1)
        df_regional["pct_problema"] = ((df_regional["com_atraso"] + df_regional["cancelados"]) / df_regional["total_pedidos"] * 100).round(1)

    conn.close()
    return df_detail, df_summary, df_regional


# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES AUXILIARES
# ─────────────────────────────────────────────────────────────────────────────

def format_brl(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_k(value: float) -> str:
    if abs(value) >= 1_000_000:
        return f"R$ {value / 1_000_000:,.1f}M".replace(",", "X").replace(".", ",").replace("X", ".")
    elif abs(value) >= 1_000:
        return f"R$ {value / 1_000:,.0f}K".replace(",", "X").replace(".", ",").replace("X", ".")
    return format_brl(value)


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 3 — DASHBOARD STREAMLIT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    check_data_files()
    df_detail, df_summary, df_regional = load_and_process_data()

    # ══════════════════════════════════════════════════════════════════════════
    # SIDEBAR — FILTROS
    # ══════════════════════════════════════════════════════════════════════════
    with st.sidebar:
        st.markdown(
            """
            <div style='text-align: center; padding: 10px 0 20px 0;'>
                <h2 style='color: #fafafa; margin-bottom: 0;'>📊 Painel de Controle</h2>
                <p style='color: #8b949e; font-size: 1rem;'>Filtros & Navegação</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("---")

        st.markdown("### 📅 Período de Análise")
        min_date = date(2017, 1, 1)
        max_date = date(2018, 7, 31)

        col_d1, col_d2 = st.columns(2)
        with col_d1:
            data_inicio = st.date_input("Data Início", value=min_date, min_value=min_date, max_value=max_date)
        with col_d2:
            data_fim = st.date_input("Data Fim", value=max_date, min_value=min_date, max_value=max_date)

        st.markdown("---")

        st.markdown("### 🎯 Status Operacional")
        buckets_disponiveis = df_detail["bucket"].unique().tolist()
        buckets_selecionados = st.multiselect("Selecione os status:", options=buckets_disponiveis, default=buckets_disponiveis)

        st.markdown("---")

        st.markdown("### 💰 Faixa de Receita (R$)")
        min_receita = float(df_detail["receita"].min())
        max_receita = float(df_detail["receita"].max())
        faixa_receita = st.slider("Faixa de valor do pedido:", min_value=min_receita, max_value=min(max_receita, 5000.0), value=(min_receita, min(max_receita, 5000.0)), format="R$ %.0f")

        st.markdown("---")

        st.markdown("### 🧭 Navegação Rápida")
        secao = st.radio(
            "Ir para:",
            options=[
                "🏠 Visão Geral (KPIs + Funil)",
                "🎯 Matriz de Impacto Financeiro",
                "💧 Cascata de Erosão de Receita",
                "🗺️ Gargalo Regional por UF",
                "📈 Linha do Tempo Operacional",
            ],
            index=0,
        )

        st.markdown("---")
        st.markdown("<p style='text-align: center; color: #484f58; font-size: 0.85rem;'>Dashboard v2.1 — UX Otimizada<br>Dados: Olist Brazilian E-Commerce</p>", unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # APLICAR FILTROS
    # ══════════════════════════════════════════════════════════════════════════
    mask = (
        (df_detail["order_purchase_timestamp"].dt.date >= data_inicio)
        & (df_detail["order_purchase_timestamp"].dt.date <= data_fim)
        & (df_detail["bucket"].isin(buckets_selecionados))
        & (df_detail["receita"] >= faixa_receita[0])
        & (df_detail["receita"] <= faixa_receita[1])
    )
    df_filtered = df_detail[mask].copy()

    df_summary_f = df_filtered.groupby("bucket").agg(total_pedidos=("order_id", "count"), receita_total=("receita", "sum")).reset_index()
    bucket_order = {"Entregue no Prazo": 1, "Entregue com Atraso": 2, "Cancelado / Indisponível": 3, "Em Trânsito / Outros": 4}
    df_summary_f["_order"] = df_summary_f["bucket"].map(bucket_order)
    df_summary_f = df_summary_f.sort_values("_order").drop(columns="_order")

    # ══════════════════════════════════════════════════════════════════════════
    # CABEÇALHO
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown(
        f"""
        <div style='text-align: center; padding: 10px 0;'>
            <h1 style='color: #fafafa; font-size: 2.5rem; margin-bottom: 5px;'>
                📊 Quebra de Funil Operacional
            </h1>
            <h4 style='color: #8b949e; font-weight: 400; margin-top: 0; font-size: 1.4rem;'>
                Vender muito não adianta se a operação trava na ponta.
            </h4>
            <p style='color: #484f58; font-size: 1rem;'>
                Base: Olist Brazilian E-Commerce &nbsp;|&nbsp;
                Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
                &nbsp;|&nbsp; {len(df_filtered):,} pedidos filtrados
            </p>
        </div>
        <hr style='border-color: #30363d;'>
        """,
        unsafe_allow_html=True,
    )

    # ══════════════════════════════════════════════════════════════════════════
    # SEÇÃO 1 — KPIs + FUNIL (Visão Geral)
    # ══════════════════════════════════════════════════════════════════════════

    def get_bucket_values(df, bucket_name):
        row = df[df["bucket"] == bucket_name]
        pedidos = int(row["total_pedidos"].values[0]) if len(row) > 0 else 0
        receita = float(row["receita_total"].values[0]) if len(row) > 0 else 0.0
        return pedidos, receita

    receita_total = df_summary_f["receita_total"].sum()
    total_pedidos = df_summary_f["total_pedidos"].sum()

    ped_prazo, rec_prazo = get_bucket_values(df_summary_f, "Entregue no Prazo")
    ped_atraso, rec_atraso = get_bucket_values(df_summary_f, "Entregue com Atraso")
    ped_cancel, rec_cancel = get_bucket_values(df_summary_f, "Cancelado / Indisponível")
    ped_outros, rec_outros = get_bucket_values(df_summary_f, "Em Trânsito / Outros")

    receita_comprometida = rec_atraso + rec_cancel

    st.markdown("<a id='visao-geral'></a>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("💰 Receita Total Vendida", format_brl(receita_total), help="Soma de preço + frete de todos os pedidos no período")
    with col2:
        pct_prazo = (rec_prazo / receita_total * 100) if receita_total > 0 else 0
        st.metric("✅ Receita Entregue no Prazo", format_brl(rec_prazo), delta=f"{pct_prazo:.1f}% do total")
    with col3:
        pct_comp = (receita_comprometida / receita_total * 100) if receita_total > 0 else 0
        st.metric("⚠️ Receita Comprometida", format_brl(receita_comprometida), delta=f"-{pct_comp:.1f}% do total", delta_color="normal", help="Atrasos + Cancelamentos")
    with col4:
        pct_cancel = (rec_cancel / receita_total * 100) if receita_total > 0 else 0
        st.metric("🚫 Receita Perdida (Cancelados)", format_brl(rec_cancel), delta=f"-{pct_cancel:.1f}% do total", delta_color="normal")

    st.markdown("<br>", unsafe_allow_html=True)

    left_col, right_col = st.columns(2)

    with left_col:
        st.markdown("<h3 style='font-size: 1.5rem;'>Funil de Pedidos: da Venda à Entrega</h3>", unsafe_allow_html=True)
        funnel_labels = ["Pedidos Realizados", "Entregue no Prazo", "Entregue com Atraso", "Cancelado / Indisponível"]
        funnel_values = [total_pedidos, ped_prazo, ped_atraso, ped_cancel]
        funnel_colors = ["#58a6ff", "#2ecc71", "#f39c12", "#e74c3c"]

        fig_funnel = go.Figure(
            go.Funnel(
                y=funnel_labels, x=funnel_values,
                textinfo="value+percent initial",
                marker=dict(color=funnel_colors),
                connector=dict(line=dict(color="#30363d", width=2)),
                textfont=dict(size=18) # Fonte do texto no gráfico
            )
        )
        fig_funnel.update_layout(
            template=PLOTLY_TEMPLATE, height=450,
            margin=dict(l=10, r=10, t=10, b=10), 
            font=dict(size=18), # Fonte dos rótulos dos eixos
            hoverlabel=dict(font_size=18) # Tamanho da fonte do mouse hover
        )
        st.plotly_chart(fig_funnel, use_container_width=True)

    with right_col:
        st.markdown("<h3 style='font-size: 1.5rem;'>Receita por Status Operacional</h3>", unsafe_allow_html=True)
        df_chart = df_summary_f[df_summary_f["bucket"] != "Em Trânsito / Outros"].copy()

        fig_bar = px.bar(
            df_chart, x="bucket", y="receita_total", color="bucket",
            color_discrete_map=COLOR_MAP, text_auto=".2s",
            labels={"bucket": "", "receita_total": "Receita (R$)"},
        )
        fig_bar.update_layout(
            template=PLOTLY_TEMPLATE, height=450, showlegend=False,
            margin=dict(l=10, r=10, t=10, b=10), 
            font=dict(size=20), # Fonte dos rótulos
            yaxis_tickprefix="R$ ",
            hoverlabel=dict(font_size=20) # Tamanho da fonte do mouse hover
        )
        fig_bar.update_traces(textposition="outside", textfont=dict(size=22))
        st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("<hr style='border-color: #30363d;'>", unsafe_allow_html=True)

    left_col2, right_col2 = st.columns(2)

    with left_col2:
        st.markdown("<h3 style='font-size: 1.5rem;'>Distribuição dos Pedidos (%)</h3>", unsafe_allow_html=True)
        fig_donut = go.Figure(
            go.Pie(
                labels=df_summary_f["bucket"], values=df_summary_f["total_pedidos"],
                hole=0.55,
                marker=dict(colors=[COLOR_MAP.get(b, "#636e72") for b in df_summary_f["bucket"]]),
                textinfo="label+percent", textposition="outside",
                textfont=dict(size=20)
            )
        )
        fig_donut.update_layout(
            template=PLOTLY_TEMPLATE, height=450,
            margin=dict(l=10, r=10, t=10, b=10), showlegend=False, 
            font=dict(size=20),
            hoverlabel=dict(font_size=20) # Tamanho da fonte do mouse hover
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    with right_col2:
        st.markdown("<h3 style='font-size: 1.5rem;'>Resumo Executivo</h3>", unsafe_allow_html=True)
        df_table = df_summary_f.copy()
        df_table.columns = ["Status", "Qtd. Pedidos", "Receita (R$)"]
        
        # Correção segura para divisão por zero na tabela
        total_qtd = df_table["Qtd. Pedidos"].sum()
        if total_qtd > 0:
            df_table["% Pedidos"] = (df_table["Qtd. Pedidos"] / total_qtd * 100).round(1).apply(lambda x: f"{x}%")
        else:
            df_table["% Pedidos"] = "0%"
            
        df_table["Receita (R$)"] = df_table["Receita (R$)"].apply(format_brl)

        st.dataframe(df_table, use_container_width=True, hide_index=True, height=250)

        st.markdown(
            f"""
            <div style='background-color: #1c1f26; padding: 20px; border-radius: 10px;
                        border-left: 6px solid #f39c12; margin-top: 15px;'>
                <strong style='color: #f39c12; font-size: 1.2rem;'>💡 Insight Principal:</strong><br><br>
                <span style='color: #c9d1d9; font-size: 1.2rem;'>
                O time comercial gerou <strong>{format_brl(receita_total)}</strong> em vendas,
                porém <strong>{format_brl(receita_comprometida)}</strong>
                ({pct_comp:.1f}%) foram comprometidos por falhas logísticas.<br><br>
                <em>Vender sem entregar é custo, não receita.</em>
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ══════════════════════════════════════════════════════════════════════════
    # SEÇÃO 2 — VISÕES AVANÇADAS (Nível Diretoria)
    # ══════════════════════════════════════════════════════════════════════════

    st.markdown(
        """
        <br>
        <div style='text-align: center; padding: 20px 0 10px 0;'>
            <h2 style='color: #58a6ff; font-size: 2rem;'>
                🔬 Análises Avançadas — Nível Diretoria
            </h2>
            <p style='color: #8b949e; font-size: 1.1rem;'>
                Investigação de causa raiz e impacto estratégico da ineficiência logística
            </p>
        </div>
        <hr style='border-color: #30363d;'>
        """,
        unsafe_allow_html=True,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # GRÁFICO AVANÇADO 1
    # ──────────────────────────────────────────────────────────────────────────
    st.markdown("<a id='matriz-impacto'></a>", unsafe_allow_html=True)
    st.markdown("<h3 style='font-size: 1.6rem;'>🎯 Matriz de Impacto Financeiro — Scatter Plot de Risco</h3>", unsafe_allow_html=True)
    st.markdown(
        "<p style='color: #8b949e; font-size: 1.25rem;'>"
        "Cada ponto é um pedido. O eixo X mostra os dias de atraso e o eixo Y o valor do pedido. "
        "Pedidos no quadrante superior-direito são <strong style='color: #e74c3c;'>alto valor + alto atraso</strong> "
        "— o pior cenário para retenção de clientes.</p>",
        unsafe_allow_html=True,
    )

    df_scatter = df_filtered[
        (df_filtered["dias_atraso"].notna())
        & (df_filtered["bucket"].isin(["Entregue no Prazo", "Entregue com Atraso"]))
    ].copy()

    if len(df_scatter) > 0:
        if len(df_scatter) > 5000:
            df_scatter = df_scatter.sample(n=5000, random_state=42)

        fig_scatter = px.scatter(
            df_scatter,
            x="dias_atraso", y="receita", color="bucket",
            color_discrete_map=COLOR_MAP, opacity=0.6, size="receita", size_max=15,
            labels={"dias_atraso": "Dias de Atraso (vs. Previsão)", "receita": "Valor do Pedido (R$)", "bucket": "Status"},
            hover_data={"order_id": True, "receita": ":.2f", "dias_atraso": ":.1f"},
        )

        fig_scatter.add_vline(
            x=0, line_dash="dash", line_color="#e74c3c", line_width=2,
            annotation_text="Prazo Prometido", annotation_position="top right", annotation_font_color="#e74c3c",
        )

        fig_scatter.add_vrect(x0=0, x1=df_scatter["dias_atraso"].max() + 5, fillcolor="#e74c3c", opacity=0.05, line_width=0)

        fig_scatter.update_layout(
            template=PLOTLY_TEMPLATE, height=550,
            margin=dict(l=10, r=10, t=30, b=10),
            font=dict(size=14), # Aumentado
            xaxis_title="Dias de Atraso (negativo = antecipado | positivo = atrasado)",
            yaxis_title="Valor do Pedido (R$)",
            yaxis_tickprefix="R$ ",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=14)),
            hoverlabel=dict(font_size=18) # Aumentado tooltip
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        df_high_ticket_late = df_scatter[(df_scatter["dias_atraso"] > 0) & (df_scatter["receita"] > df_scatter["receita"].quantile(0.75))]
        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1: st.metric("Pedidos High-Ticket Atrasados", f"{len(df_high_ticket_late):,}")
        with col_s2: st.metric("Receita em Risco (High-Ticket)", format_brl(df_high_ticket_late["receita"].sum()))
        with col_s3: 
            atraso_medio = df_scatter[df_scatter["dias_atraso"] > 0]["dias_atraso"].mean()
            st.metric("Atraso Médio (quando atrasa)", f"{atraso_medio:.1f} dias" if not pd.isna(atraso_medio) else "N/A")
    else:
        st.info("Sem dados suficientes para a Matriz de Impacto no período selecionado.")

    st.markdown("<hr style='border-color: #30363d;'>", unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────────────────
    # GRÁFICO AVANÇADO 2
    # ──────────────────────────────────────────────────────────────────────────
    st.markdown("<a id='waterfall'></a>", unsafe_allow_html=True)
    st.markdown("<h3 style='font-size: 1.6rem;'>💧 Cascata de Erosão de Receita — Waterfall Chart</h3>", unsafe_allow_html=True)
    st.markdown(
        "<p style='color: #8b949e; font-size: 1.1rem;'>"
        "O caminho do dinheiro: da venda bruta gerada pelo comercial até a receita líquida saudável. "
        "Cada barra vermelha representa receita que <strong style='color: #e74c3c;'>virou fumaça</strong> "
        "na ponta operacional.</p>",
        unsafe_allow_html=True,
    )

    venda_bruta = receita_total
    perda_cancelamento = -rec_cancel
    perda_atraso = -rec_atraso
    receita_saudavel = rec_prazo

    fig_waterfall = go.Figure(
        go.Waterfall(
            name="Erosão de Receita", orientation="v",
            x=["Venda Bruta<br>(Comercial)", "Perda por<br>Cancelamento", "Receita Comprometida<br>por Atraso", "Receita em<br>Trânsito/Outros", "Receita Líquida<br>Saudável"],
            y=[venda_bruta, perda_cancelamento, perda_atraso, -rec_outros, 0],
            measure=["absolute", "relative", "relative", "relative", "total"],
            text=[format_k(venda_bruta), format_k(perda_cancelamento), format_k(perda_atraso), format_k(-rec_outros), format_k(receita_saudavel)],
            textposition="outside",
            textfont=dict(size=16, color="#fafafa"), # Fonte das barras aumentada
            connector=dict(line=dict(color="#30363d", width=2)),
            increasing=dict(marker=dict(color="#2ecc71")), decreasing=dict(marker=dict(color="#e74c3c")), totals=dict(marker=dict(color="#2ecc71")),
        )
    )

    fig_waterfall.update_layout(
        template=PLOTLY_TEMPLATE, height=550,
        margin=dict(l=10, r=10, t=30, b=10),
        font=dict(size=18), # Fonte dos rótulos aumentada
        yaxis_tickprefix="R$ ", yaxis_title="Receita (R$)", showlegend=False,
        hoverlabel=dict(font_size=18) # Hover text aumentado
    )
    st.plotly_chart(fig_waterfall, use_container_width=True)

    col_w1, col_w2, col_w3 = st.columns(3)
    with col_w1:
        erosao_total = rec_cancel + rec_atraso
        pct_erosao = (erosao_total / receita_total * 100) if receita_total > 0 else 0
        st.metric("Erosão Total", format_brl(erosao_total), delta=f"-{pct_erosao:.1f}%", delta_color="normal")
    with col_w2: st.metric("Perda Definitiva (Cancelados)", format_brl(rec_cancel))
    with col_w3: st.metric("Receita Líquida Saudável", format_brl(receita_saudavel), delta=f"{(receita_saudavel / receita_total * 100):.1f}% aproveitado" if receita_total > 0 else "0%")

    st.markdown("<hr style='border-color: #30363d;'>", unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────────────────
    # GRÁFICO AVANÇADO 3
    # ──────────────────────────────────────────────────────────────────────────
    st.markdown("<a id='regional'></a>", unsafe_allow_html=True)
    st.markdown("<h3 style='font-size: 1.6rem;'>🗺️ Análise de Gargalo Regional — Performance por UF</h3>", unsafe_allow_html=True)
    st.markdown(
        "<p style='color: #8b949e; font-size: 1.1rem;'>"
        "O comercial está vendendo para regiões onde a malha logística não tem capacidade? "
        "Estados com <strong style='color: #e74c3c;'>alto volume de vendas + alta taxa de problemas</strong> "
        "são os gargalos prioritários.</p>",
        unsafe_allow_html=True,
    )

    if df_regional is not None and len(df_regional) > 0:
        left_r, right_r = st.columns(2)

        with left_r:
            df_top_uf = df_regional.nlargest(15, "total_pedidos").sort_values("pct_problema", ascending=True)
            fig_regional = go.Figure()

            fig_regional.add_trace(go.Bar(
                y=df_top_uf["uf"], x=df_top_uf["pct_atraso"], name="% Atraso", orientation="h",
                marker_color="#f39c12", text=df_top_uf["pct_atraso"].apply(lambda x: f"{x:.1f}%"), textposition="inside",
                textfont=dict(size=14)
            ))
            fig_regional.add_trace(go.Bar(
                y=df_top_uf["uf"], x=df_top_uf["pct_cancelado"], name="% Cancelado", orientation="h",
                marker_color="#e74c3c", text=df_top_uf["pct_cancelado"].apply(lambda x: f"{x:.1f}%"), textposition="inside",
                textfont=dict(size=18)
            ))

            fig_regional.update_layout(
                template=PLOTLY_TEMPLATE, height=550, barmode="stack",
                margin=dict(l=10, r=10, t=30, b=10), font=dict(size=14),
                xaxis_title="% de Pedidos com Problema", yaxis_title="",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=14)),
                title=dict(text="Taxa de Problemas por UF (Top 15)", font=dict(size=16)),
                hoverlabel=dict(font_size=16) # Tooltip
            )
            st.plotly_chart(fig_regional, use_container_width=True)

        with right_r:
            fig_uf_scatter = px.scatter(
                df_regional, x="total_pedidos", y="pct_problema", size="receita_total", size_max=40,
                color="pct_problema", color_continuous_scale=["#2ecc71", "#f39c12", "#e74c3c"], text="uf",
                labels={"total_pedidos": "Volume de Pedidos", "pct_problema": "% Problemas (Atraso + Cancelamento)", "receita_total": "Receita Total", "uf": "Estado"},
                hover_data={"receita_total": ":.2f", "pct_atraso": ":.1f", "pct_cancelado": ":.1f"},
            )
            fig_uf_scatter.update_traces(textposition="top center", textfont=dict(size=14))
            fig_uf_scatter.update_layout(
                template=PLOTLY_TEMPLATE, height=550,
                margin=dict(l=10, r=10, t=30, b=10), font=dict(size=14),
                title=dict(text="Volume × Taxa de Problemas por UF", font=dict(size=16)),
                coloraxis_colorbar=dict(title="% Prob.", tickfont=dict(size=14)),
                hoverlabel=dict(font_size=18) # Tooltip
            )
            st.plotly_chart(fig_uf_scatter, use_container_width=True)

        st.markdown("<p style='font-size: 1.2rem; font-weight: bold;'>📋 Ranking Completo por UF:</p>", unsafe_allow_html=True)
        df_reg_display = df_regional.copy()
        df_reg_display = df_reg_display.rename(columns={"uf": "UF", "total_pedidos": "Pedidos", "no_prazo": "No Prazo", "com_atraso": "Atrasados", "cancelados": "Cancelados", "receita_total": "Receita Total", "pct_atraso": "% Atraso", "pct_cancelado": "% Cancelado", "pct_problema": "% Problema Total"})
        df_reg_display["Receita Total"] = df_reg_display["Receita Total"].apply(format_brl)
        cols_show = ["UF", "Pedidos", "No Prazo", "Atrasados", "Cancelados", "% Atraso", "% Cancelado", "% Problema Total", "Receita Total"]
        st.dataframe(df_reg_display[cols_show].sort_values("Pedidos", ascending=False), use_container_width=True, hide_index=True, height=300)
    else:
        st.warning("⚠️ Para a análise regional, inclua o arquivo `olist_customers_dataset.csv` na pasta `data/`.")

    st.markdown("<hr style='border-color: #30363d;'>", unsafe_allow_html=True)

    # ──────────────────────────────────────────────────────────────────────────
    # GRÁFICO AVANÇADO 4
    # ──────────────────────────────────────────────────────────────────────────
    st.markdown("<a id='timeline'></a>", unsafe_allow_html=True)
    st.markdown("<h3 style='font-size: 1.6rem;'>📈 Linha do Tempo Operacional — Evolução Mensal</h3>", unsafe_allow_html=True)
    st.markdown(
        "<p style='color: #8b949e; font-size: 1.25rem;'>"
        "Como a performance operacional evoluiu ao longo do tempo? "
        "A taxa de problemas está <strong style='color: #2ecc71;'>melhorando</strong> ou "
        "<strong style='color: #e74c3c;'>piorando</strong>?</p>",
        unsafe_allow_html=True,
    )

    df_timeline = df_filtered.copy()
    df_timeline["mes"] = df_timeline["order_purchase_timestamp"].dt.to_period("M").astype(str)

    df_month = df_timeline.groupby(["mes", "bucket"]).agg(pedidos=("order_id", "count"), receita=("receita", "sum")).reset_index()
    df_month_total = df_timeline.groupby("mes").agg(total_mes=("order_id", "count")).reset_index()
    df_month = df_month.merge(df_month_total, on="mes")
    df_month["pct"] = (df_month["pedidos"] / df_month["total_mes"] * 100).round(1)

    fig_timeline = px.area(
        df_month, x="mes", y="pedidos", color="bucket", color_discrete_map=COLOR_MAP,
        labels={"mes": "Mês", "pedidos": "Qtd. Pedidos", "bucket": "Status"}, groupnorm="percent",
    )
    fig_timeline.update_layout(
        template=PLOTLY_TEMPLATE, height=500,
        margin=dict(l=10, r=10, t=30, b=10), font=dict(size=18),
        xaxis_title="", yaxis_title="% dos Pedidos",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=14)),
        title=dict(text="Evolução Mensal — Composição dos Status (%)", font=dict(size=18)),
        hoverlabel=dict(font_size=18) # Tooltip
    )
    st.plotly_chart(fig_timeline, use_container_width=True)

    df_prob_month = df_timeline.copy()
    df_prob_month["problema"] = df_prob_month["bucket"].isin(["Entregue com Atraso", "Cancelado / Indisponível"]).astype(int)
    df_trend = df_prob_month.groupby("mes").agg(total=("order_id", "count"), problemas=("problema", "sum"), receita=("receita", "sum")).reset_index()
    df_trend["pct_problema"] = (df_trend["problemas"] / df_trend["total"] * 100).round(1)

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(x=df_trend["mes"], y=df_trend["pct_problema"], mode="lines+markers", name="% Problemas", line=dict(color="#e74c3c", width=3), marker=dict(size=8)))
    fig_trend.add_trace(go.Bar(x=df_trend["mes"], y=df_trend["total"], name="Volume de Pedidos", marker_color="#58a6ff", opacity=0.3, yaxis="y2"))
    
    fig_trend.update_layout(
        template=PLOTLY_TEMPLATE, height=450,
        margin=dict(l=10, r=50, t=30, b=10), font=dict(size=14),
        xaxis_title="",
        yaxis=dict(title="% Problemas", side="left", ticksuffix="%"),
        yaxis2=dict(title="Volume de Pedidos", side="right", overlaying="y", showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=14)),
        title=dict(text="Tendência: Taxa de Problemas vs. Volume", font=dict(size=16)),
        hoverlabel=dict(font_size=16) # Tooltip
    )
    st.plotly_chart(fig_trend, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # INSIGHT EXECUTIVO FINAL
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown(
        f"""
        <div style='background-color: #1c1f26; padding: 30px; border-radius: 12px;
                    border: 1px solid #30363d; margin: 20px 0;'>
            <h3 style='color: #58a6ff; margin-top: 0; font-size: 1.8rem;'>📋 Insight Executivo — Para sua Apresentação</h3>
            <p style='color: #c9d1d9; font-size: 1.15rem; line-height: 1.8;'>
                A análise dos dados da Olist revela um cenário clássico de <strong style='color: #f39c12;'>
                desalinhamento entre a capacidade comercial e a capacidade operacional</strong>.
                O time de vendas gerou <strong>{format_brl(receita_total)}</strong> em receita bruta,
                porém <strong style='color: #e74c3c;'>{format_brl(receita_comprometida)}</strong>
                ({pct_comp:.1f}%) foram comprometidos por falhas logísticas — entre cancelamentos
                e entregas fora do prazo.
            </p>
            <p style='color: #c9d1d9; font-size: 1.15rem; line-height: 1.8;'>
                A <strong>Matriz de Impacto Financeiro</strong> mostra que os pedidos de alto valor
                (high-ticket) não estão sendo priorizados pela operação, gerando risco direto de churn
                nos clientes mais rentáveis. O <strong>Waterfall de Erosão</strong> deixa claro que cada
                real perdido em cancelamento é receita que o comercial suou para trazer e a operação
                deixou escapar. A <strong>Análise Regional</strong> expõe que existem estados onde o
                comercial vende agressivamente, mas a malha logística simplesmente não acompanha.
            </p>
            <p style='color: #8b949e; font-size: 1.1rem; margin-top: 15px; font-style: italic;'>
                "A empresa que escala vendas sem escalar operações não está crescendo — está acumulando
                dívida de experiência com o cliente."
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── RODAPÉ ──
    st.markdown(
        """
        <hr style='border-color: #30363d;'>
        <p style='text-align: center; color: #484f58; font-size: 0.9rem;'>
            Dashboard de Quebra de Funil Operacional v2.1 — UX Otimizada &nbsp;|&nbsp;
            Dados: Olist Brazilian E-Commerce (Kaggle) &nbsp;|&nbsp;
            Stack: Python · SQLite · Pandas · Streamlit · Plotly
        </p>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# EXECUÇÃO
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()