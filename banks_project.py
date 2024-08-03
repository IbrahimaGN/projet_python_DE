# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import datetime
import sqlite3


def log_progress(message):
    with open('code_log.txt', 'a') as log_file:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file.write(f"{timestamp} : {message}\n")

# Exemple d'utilisation de la fonction log_progress
log_progress("Preliminaries complete. Initiating ETL process")



def extract():
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table', {'class': 'wikitable'})

    rows = table.find_all('tr')
    data = []

    for row in rows[1:]:
        cols = row.find_all('td')
        name = cols[1].text.strip()
        mc_usd = float(cols[2].text.strip().replace('\n', ''))
        data.append([name, mc_usd])

    df = pd.DataFrame(data, columns=['Name', 'MC_USD_Billion'])
    return df

# # Exemple d'utilisation de la fonction extract
# if __name__ == "__main__":
#     df = extract()
#     print(df)
#     log_progress("Data extraction complete. Initiating Transformation process")



def transform(df):
    # Lire le fichier CSV des taux de change
    exchange_rates = pd.read_csv('exchange_rate.csv')
    
    # Convertir les taux de change en dictionnaire
    exchange_rate_dict = dict(zip(exchange_rates['Currency'], exchange_rates['Rate']))
    
    # Ajouter les colonnes transformées
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df

# # Exemple d'utilisation de la fonction transform
# if __name__ == "__main__":
#     df = extract()  # Appel à la fonction extract définie précédemment
#     df = transform(df)
#     print(df)
#     print(df['MC_EUR_Billion'][4])
#     log_progress("Data transformation complete. Initiating Loading process")


def load_to_csv(df, file_path):
    df.to_csv(file_path, index=False)
    log_progress("Data saved to CSV file")

# # Exemple d'utilisation de la fonction load_to_csv
# if __name__ == "__main__":
#     # Extraction et transformation des données
#     df = extract()  # Appel à la fonction extract définie précédemment
#     df = transform(df)  # Appel à la fonction transform définie précédemment
    
#     # Chemin de sortie du fichier CSV
#     csv_file_path = './Largest_banks_data.csv'
    
#     # Chargement des données dans le fichier CSV
#     load_to_csv(df, csv_file_path)
    
#     # Vérifier le contenu du fichier CSV
#     print(f"Data successfully saved to {csv_file_path}")
#     log_progress("Data loading to CSV complete.")


def load_to_db(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")

# # Exemple d'utilisation des fonctions load_to_csv() et load_to_db()
# if __name__ == "__main__":
#     # Extraction et transformation des données
#     df = extract()  # Appel à la fonction extract définie précédemment
#     df = transform(df)  # Appel à la fonction transform définie précédemment
    
#     # Chemin de sortie du fichier CSV
#     csv_file_path = './Largest_banks_data.csv'
    
#     # Chargement des données dans le fichier CSV
#     load_to_csv(df, csv_file_path)
    
#     # Connexion à la base de données SQLite3
#     conn = sqlite3.connect('Banks.db')
#     log_progress("SQL Connection initiated")
    
#     # Chargement des données dans la base de données
#     load_to_db(df, conn, 'Largest_banks')
    
#     # Fermeture de la connexion à la base de données
#     conn.close()
#     log_progress("Server Connection closed")
    
#     # Vérifier le contenu de la base de données
#     print(f"Data successfully loaded to the database 'Banks.db' in the table 'Largest_banks'")
#     log_progress("Process Complete")


def run_queries(query, conn):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    print(f"Query: {query}")
    for row in result:
        print(row)
    log_progress(f"Executed query: {query}")

if __name__ == "__main__":
    # Extraction et transformation des données
    df = extract()  # Appel à la fonction extract définie précédemment
    df = transform(df)  # Appel à la fonction transform définie précédemment
    
    # Chargement des données dans le fichier CSV
    load_to_csv(df, './Largest_banks_data.csv')
    
    # Connexion à la base de données SQLite3
    conn = sqlite3.connect('Banks.db')
    log_progress("SQL Connection initiated")
    
    # Chargement des données dans la base de données
    load_to_db(df, conn, 'Largest_banks')
    
    # Exécuter les requêtes
    run_queries("SELECT * FROM Largest_banks", conn)
    run_queries("SELECT AVG(MC_USD_Billion) FROM Largest_banks", conn)
    run_queries("SELECT Name FROM Largest_banks LIMIT 5", conn)
    
    # Fermeture de la connexion à la base de données
    conn.close()
    log_progress("Server Connection closed")
    log_progress("Process Complete")

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''