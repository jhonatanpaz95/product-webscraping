import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import sqlite3
import asyncio
from telegram import Bot
import os
from dotenv import load_dotenv

load_dotenv()

# Configurações do bot do Telegram
TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
bot = Bot(token=TOKEN)

URL = "https://www.mercadolivre.com.br/notebook-gamer-lenovo-loq-intel-core-i5-12450h-16gb-ram-512gb-ssd-rtx-2050-windows-11-home-tela-156-full-hd-83eu0001br/p/MLB29758046?pdp_filters=item_id:MLB3805227995#polycard_client=recommendations_pdp-pads-up&reco_backend=recos-merge-experimental-pdp-up-c_marketplace&reco_model=ranker_entity_v2-retrieval-system/mlb/productos-promocionados/catalog-product-triggers/1&reco_client=pdp-pads-up&reco_item_pos=1&reco_backend_type=low_level&reco_id=a4a47d82-6c3e-4c68-a95a-e1e27a625858&wid=MLB3805227995&sid=recos&is_advertising=true&ad_domain=PDPDESKTOP_UP&ad_position=2&ad_click_id=YTFmZTU3ZGItYTBhOC00ZjJkLWE2MzAtYmExYjk5MGI5NzYz"

def fetch_page(URL):
    response = requests.get(URL)
    return response.text


def parse_page(html):
    soup = BeautifulSoup(html, 'html.parser')
    product_name = soup.find('h1', class_='ui-pdp-title').get_text()
    prices: list = soup.find_all('span', class_='andes-money-amount__fraction')
    old_price: int = int(prices[0].get_text().replace('.', ''))
    new_price: int = int(prices[1].get_text().replace('.', ''))
    installment_price: int = int(prices[2].get_text().replace('.', ''))
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
   
    return {
        'product_name': product_name,
        'old_price': old_price,
        'new_price': new_price,
        'installment_price': installment_price,
        'timestamp': timestamp
    }

def create_connection(db_name='notebook_prices.db'):
    """Cria uma conexão com o banco de dados SQLite"""
    conn = sqlite3.connect(db_name)
    return conn

def setup_database(conn):
    """Cria a tabela de preços se ela não existir."""
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT,
            old_price INTEGER,
            new_price INTEGER,
            installment_price INTEGER,
            timestamp TEXT
        )
    ''')
    conn.commit()  


def save_to_database(conn, products_info):
    new_row = pd.DataFrame([products_info])
    new_row.to_sql('prices', conn, if_exists='append', index=False)


def get_max_price(conn):
    #conecta no banco de dados
    cursor = conn.cursor()
    #o preço máximo histórico (SELECT max(price)...)
    cursor.execute("SELECT MAX(new_price), timestamp from prices")
    #retorna esse valor
    result = cursor.fetchone()
    return result[0], result[1]


async def send_telegram_message(text):
    await bot.send_message(CHAT_ID=CHAT_ID, text=text)


async def main():

    conn = create_connection()
    setup_database(conn)

    while True:
        page_content = fetch_page(URL)
        products_info = parse_page(page_content)

        max_price, max_timestamp = get_max_price(conn)

        current_price = products_info["new_price"]

        if current_price > max_price:
            print("Preço maior detectado")
            await send_telegram_message(text=f"Preço maior detectado{current_price}")
            max_price = current_price
            max_timestamp = products_info["timestamp"]
        else:
            print('O preço máximo registrado é o antigo')
            await send_telegram_message(text=f'O preço máximo registrado é o antigo{max_price} em {max_timestamp}')

        save_to_database(conn, products_info)
        print('Dados Salvos no Banco de Dados: ', products_info)
        await time.sleep(10)
    
    # Fecha a conexão com o banco de dados
    conn.close()

asyncio.run(main())