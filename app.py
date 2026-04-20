"""
=============================================================================
 Dashboard de Análise de Quebra de Funil Operacional
 Base de Dados: Olist Brazilian E-Commerce (Kaggle)
 Autor: Wilson Félix
 Objetivo: Provar que vender muito não adianta se a operação trava na ponta.
=============================================================================
"""

import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DA PÁGINA
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Quebra de Funil Operacional — Olist",
    page_icon="📊",
    layout="wide",
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

# Paleta de cores para os buckets de status
COLOR_MAP = {
    "Entregue no Prazo": "#2ecc71",       # verde
    "Entregue com Atraso": "#f39c12",     # laranja
    "Cancelado / Indisponível": "#e74c3c", # vermelho
    "Em Trânsito / Outros": "#95a5a6",    # cinza
}

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 1 — VERIFICAÇÃO DOS FICHEIROS CSV
# ─────────────────────────────────────────────────────────────────────────────
def check_data_files() -> bool:
    """Verifica se todos os CSVs obrigatórios existem na pasta data/."""
    missing = [f for f in REQUIRED_FILES if not os.path.isfile(os.path.join(DATA_PATH, f))]
    if missing:
        st.error("⚠️ Ficheiros CSV não encontrados na pasta `data/`.")
        st.stop()
    return True

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 2 — CARGA, LIMPEZA E MODELO SQL (SQLite em memória)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="A carregar e processar dados…")
def load_and_process_data():
    orders = pd.read_csv(os.path.join(DATA_PATH, "olist_orders_dataset.csv"))
    items = pd.read_csv(os.path.join(DATA_PATH, "olist_order_items_dataset.csv"))
    payments = pd.read_csv(os.path.join(DATA_PATH, "olist_order_payments_dataset.csv"))

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
    orders["order_approved_at"] = orders["order_approved_at"].fillna(orders["order_purchase_timestamp"])

    conn = sqlite3.connect(":memory:")
    orders.to_sql("orders", conn, index=False, if_exists="replace")
    items.to_sql("order_items", conn, index=False, if_exists="replace")
    payments.to_sql("order_payments", conn, index=False, if_exists="replace")

    query_buckets = """
    WITH pedido_receita AS (
        SELECT
            o.order_id,
            o.order_status,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            COALESCE(SUM(oi.price + oi.freight_value), 0) AS receita
        FROM orders AS o
        LEFT JOIN order_items AS oi ON o.order_id = oi.order_id
        GROUP BY 1, 2, 3, 4, 5, 6
    )
    SELECT
        order_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        receita,
        CASE
            WHEN order_status IN ('canceled', 'unavailable') THEN 'Cancelado / Indisponível'
            WHEN order_delivered_customer_date IS NOT NULL AND order_delivered_customer_date <= order_estimated_delivery_date THEN 'Entregue no Prazo'
            WHEN order_delivered_customer_date IS NOT NULL AND order_delivered_customer_date > order_estimated_delivery_date THEN 'Entregue com Atraso'
            ELSE 'Em Trânsito / Outros'
        END AS bucket
    FROM pedido_receita
    """
    df_detail = pd.read_sql_query(query_buckets, conn)
    conn.close()
    return df_detail

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 3 — DASHBOARD STREAMLIT
# ─────────────────────────────────────────────────────────────────────────────
def format_brl(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def main():
    check_data_files()
    df_detail = load_and_process_data()

    df_detail['data_compra'] = pd.to_datetime(df_detail['order_purchase_timestamp']).dt.date
    min_date = df_detail['data_compra'].min()
    max_date = df_detail['data_compra'].max()

    # ── BARRA LATERAL (FILTROS) ──
    st.sidebar.title("⚙️ Filtros Estratégicos")
    st.sidebar.markdown("<span style='font-size: 16px;'>Filtre por período para analisar o impacto do volume logístico (ex: picos de Black Friday).</span>", unsafe_allow_html=True)
    
    datas = st.sidebar.date_input(
        "Período da Venda Realizada",
        value=[min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )

    if len(datas) == 2:
        start_date, end_date = datas
        df_filtered = df_detail[(df_detail['data_compra'] >= start_date) & (df_detail['data_compra'] <= end_date)]
    else:
        df_filtered = df_detail

    df_summary = df_filtered.groupby("bucket").agg(total_pedidos=("order_id", "count"), receita_total=("receita", "sum")).reset_index()

    ordem_status = {'Entregue no Prazo': 1, 'Entregue com Atraso': 2, 'Cancelado / Indisponível': 3, 'Em Trânsito / Outros': 4}
    df_summary['ordem'] = df_summary['bucket'].map(ordem_status)
    df_summary = df_summary.sort_values('ordem').drop('ordem', axis=1)

    # ── CABEÇALHO ──
    st.markdown(
        """
        <h1 style='text-align: center; color: #3498db; font-size: 48px;'>
            📊 Quebra de Funil Operacional
        </h1>
        <h4 style='text-align: center; color: #7f8c8d; font-weight: 400; font-size: 24px;'>
            Vender muito não adianta se a operação trava na ponta.
        </h4>
        <hr>
        """,
        unsafe_allow_html=True,
    )

    # ── KPIs ──
    receita_total = df_summary["receita_total"].sum()

    def get_bucket_values(bucket_name):
        row = df_summary[df_summary["bucket"] == bucket_name]
        pedidos = int(row["total_pedidos"].values[0]) if len(row) > 0 else 0
        receita = float(row["receita_total"].values[0]) if len(row) > 0 else 0.0
        return pedidos, receita

    ped_prazo, rec_prazo = get_bucket_values("Entregue no Prazo")
    ped_atraso, rec_atraso = get_bucket_values("Entregue com Atraso")
    ped_cancel, rec_cancel = get_bucket_values("Cancelado / Indisponível")

    total_pedidos = df_summary["total_pedidos"].sum()
    receita_comprometida = rec_atraso + rec_cancel

    col1, col2, col3, col4 = st.columns(4)
    with col1: st.metric("💰 Receita Vendida (Filtro)", format_brl(receita_total))
    with col2: st.metric("✅ Entregue no Prazo", format_brl(rec_prazo), delta=f"{(rec_prazo / receita_total * 100):.1f}%" if receita_total > 0 else "0%")
    with col3: st.metric("⚠️ Receita Comprometida", format_brl(receita_comprometida), delta=f"-{(receita_comprometida / receita_total * 100):.1f}%" if receita_total > 0 else "0%", delta_color="inverse")
    with col4: st.metric("🚫 Perdida (Cancelados)", format_brl(rec_cancel), delta=f"-{(rec_cancel / receita_total * 100):.1f}%" if receita_total > 0 else "0%", delta_color="inverse")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── GRÁFICOS ──
    left_col, right_col = st.columns(2)

    # --- Gráfico 1: Funil de Pedidos ---
    with left_col:
        st.markdown("<h3 style='font-size: 22px;'>Funil de Pedidos: da Venda à Entrega</h3>", unsafe_allow_html=True)

        funnel_labels = ["Pedidos Realizados", "Entregue no Prazo", "Entregue com Atraso", "Cancelado / Indisponível"]
        funnel_values = [total_pedidos, ped_prazo, ped_atraso, ped_cancel]
        funnel_colors = ["#3498db", "#2ecc71", "#f39c12", "#e74c3c"]

        fig_funnel = go.Figure(go.Funnel(
            y=funnel_labels, 
            x=funnel_values, 
            textinfo="value+percent initial",
            marker=dict(color=funnel_colors),
            textfont=dict(size=18, color="white"), # Fonte interna maior
        ))
        fig_funnel.update_layout(
            height=500, 
            margin=dict(l=20, r=20, t=20, b=20), 
            font=dict(size=16), # Fonte global do gráfico maior
            yaxis=dict(tickfont=dict(size=16)) # Fonte do eixo Y maior
        )
        st.plotly_chart(fig_funnel, use_container_width=True)

    # --- Gráfico 2: Receita por Bucket ---
    with right_col:
        st.markdown("<h3 style='font-size: 22px;'>Receita por Status Operacional</h3>", unsafe_allow_html=True)

        df_chart = df_summary[df_summary["bucket"] != "Em Trânsito / Outros"].copy()

        fig_bar = px.bar(
            df_chart, x="bucket", y="receita_total", color="bucket", 
            color_discrete_map=COLOR_MAP, text_auto=".2s", 
            labels={"bucket": "", "receita_total": "Receita (R$)"}
        )
        fig_bar.update_layout(
            height=500, 
            showlegend=False, 
            margin=dict(l=20, r=20, t=20, b=20), 
            font=dict(size=16), # Fonte global do gráfico maior
            xaxis=dict(tickfont=dict(size=16)), # Rótulos do eixo X (categorias)
            yaxis_tickprefix="R$ "
        )
        fig_bar.update_traces(
            textposition="outside", 
            textfont=dict(size=18, color="white") # Fonte dos valores acima das barras
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("---")

    # --- Insight Final ---
    perc_comp = (receita_comprometida / receita_total * 100) if receita_total > 0 else 0
    st.markdown(
        f"""
        <div style='background-color: #34495e; padding: 25px; border-radius: 10px; border-left: 8px solid #f39c12; margin-top: 15px;'>
            <strong style='font-size: 20px;'>💡 Insight do Período:</strong><br><br>
            <span style='font-size: 18px;'>
            O comercial gerou <strong>{format_brl(receita_total)}</strong> em vendas,
            mas <strong>{format_brl(receita_comprometida)}</strong>
            ({perc_comp:.1f}%)
            foram comprometidos por falhas logísticas.
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

if __name__ == "__main__":
    main()