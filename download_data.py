"""
=============================================================================
 Script de Download dos Dados — Olist Brazilian E-Commerce
 Execução: python download_data.py
=============================================================================
Duas opções de download:
  1. Automático via API do Kaggle (requer kaggle.json configurado)
  2. Manual — o script exibe as instruções passo a passo
=============================================================================
"""

import os
import sys
import zipfile

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

REQUIRED_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
]

KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
KAGGLE_URL = "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"


def download_via_kaggle_api():
    """Tenta baixar usando a biblioteca kaggle (pip install kaggle)."""
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()

        print(f"Baixando dataset '{KAGGLE_DATASET}' para '{DATA_DIR}'...")
        api.dataset_download_files(KAGGLE_DATASET, path=DATA_DIR, unzip=True)
        print("Download concluído com sucesso!")
        return True

    except ImportError:
        print("Biblioteca 'kaggle' não instalada. Instale com: pip install kaggle")
        return False
    except Exception as e:
        print(f"Erro ao usar a API do Kaggle: {e}")
        return False


def show_manual_instructions():
    """Exibe instruções para download manual."""
    print()
    print("=" * 70)
    print("  INSTRUÇÕES PARA DOWNLOAD MANUAL DOS DADOS")
    print("=" * 70)
    print()
    print("  1. Acesse o link abaixo e faça login no Kaggle:")
    print(f"     {KAGGLE_URL}")
    print()
    print("  2. Clique no botão 'Download' para baixar o arquivo ZIP.")
    print()
    print(f"  3. Extraia o conteúdo do ZIP para a pasta:")
    print(f"     {os.path.abspath(DATA_DIR)}")
    print()
    print("  4. Verifique se os seguintes arquivos estão presentes:")
    for f in REQUIRED_FILES:
        print(f"     - {f}")
    print()
    print("  5. Execute o dashboard com: streamlit run app.py")
    print()
    print("=" * 70)


def check_files():
    """Verifica se os arquivos necessários já existem."""
    missing = [f for f in REQUIRED_FILES if not os.path.isfile(os.path.join(DATA_DIR, f))]
    if not missing:
        print("Todos os arquivos necessários já estão presentes na pasta data/.")
        return True
    return False


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    if check_files():
        return

    print("Tentando download automático via API do Kaggle...")
    if download_via_kaggle_api():
        if check_files():
            return

    print("\nDownload automático não disponível. Siga as instruções abaixo:\n")
    show_manual_instructions()


if __name__ == "__main__":
    main()
