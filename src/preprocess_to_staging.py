import argparse
import boto3
import mysql.connector
from mysql.connector import Error
import pandas as pd
from io import StringIO


def get_data_from_raw(endpoint_url, bucket_name, file_name="wikitext-2-combined.txt"):
    """Récupère les données depuis le bucket raw."""
    try:
        s3_client = boto3.client('s3', endpoint_url=endpoint_url)
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        print(f"Erreur lors de la récupération des données depuis S3: {e}")
        return None

def clean_data(content):
    """Nettoie les données (suppression des doublons et des lignes vides)."""
    # Convertir le contenu en DataFrame
    df = pd.DataFrame(content.split('\n'), columns=['text'])
    
    # Supprimer les lignes vides
    df = df[df['text'].str.strip().astype(bool)]
    
    # Supprimer les doublons
    df = df.drop_duplicates()
    
    # Réinitialiser l'index
    df = df.reset_index(drop=True)
    
    return df

def create_mysql_connection(host, user, password, database):
    """Crée une connexion MySQL."""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return connection
    except Error as e:
        print(f"Erreur lors de la connexion à MySQL: {e}")
        return None

def create_table(connection):
    """Crée la table texts si elle n'existe pas."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS texts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        connection.commit()
    except Error as e:
        print(f"Erreur lors de la création de la table: {e}")

def insert_data(connection, df):
    """Insère les données dans la table texts."""
    try:
        cursor = connection.cursor()
        
        # Préparer la requête d'insertion
        insert_query = "INSERT INTO texts (text) VALUES (%s)"
        
        # Insérer les données
        values = [(row,) for row in df['text']]
        cursor.executemany(insert_query, values)
        
        connection.commit()
        print(f"{cursor.rowcount} lignes insérées avec succès.")
    except Error as e:
        print(f"Erreur lors de l'insertion des données: {e}")

def validate_data(connection):
    """Valide les données insérées avec des requêtes SQL."""
    try:
        cursor = connection.cursor()
        
        # Compte total des lignes
        cursor.execute("SELECT COUNT(*) FROM texts")
        total_count = cursor.fetchone()[0]
        print(f"Nombre total de lignes: {total_count}")
        
        # Compte des lignes non vides
        cursor.execute("SELECT COUNT(*) FROM texts WHERE text IS NOT NULL")
        non_null_count = cursor.fetchone()[0]
        print(f"Nombre de lignes non nulles: {non_null_count}")
        
        # Exemple de quelques lignes
        cursor.execute("SELECT id, text FROM texts LIMIT 5")
        print("\nExemple de 5 premières lignes:")
        for row in cursor.fetchall():
            print(f"ID: {row[0]}, Text: {row[1][:50]}...")
            
    except Error as e:
        print(f"Erreur lors de la validation des données: {e}")

def main():
    parser = argparse.ArgumentParser(description='Prépare les données pour le staging dans MySQL')
    parser.add_argument('--bucket_raw', type=str, required=True, help='Nom du bucket raw')
    parser.add_argument('--db_host', type=str, required=True, help='Hôte MySQL')
    parser.add_argument('--db_user', type=str, required=True, help='Utilisateur MySQL')
    parser.add_argument('--db_password', type=str, required=True, help='Mot de passe MySQL')
    parser.add_argument('--endpoint-url', type=str, default='http://localhost:4566',
                        help='URL du endpoint S3 (LocalStack)')
    
    args = parser.parse_args()
    
    # Récupérer les données depuis raw
    print("Récupération des données depuis le bucket raw...")
    content = get_data_from_raw(args.endpoint_url, args.bucket_raw)
    if content is None:
        return
    
    # Nettoyer les données
    print("Nettoyage des données...")
    df = clean_data(content)
    
    # Connexion à MySQL
    print("Connexion à MySQL...")
    connection = create_mysql_connection(
        args.db_host,
        args.db_user,
        args.db_password,
        'staging'
    )
    if connection is None:
        return
    
    # Créer la table
    print("Création de la table si nécessaire...")
    create_table(connection)
    
    # Insérer les données
    print("Insertion des données...")
    insert_data(connection, df)
    
    # Valider les données
    print("\nValidation des données...")
    validate_data(connection)
    
    # Fermer la connexion
    connection.close()
    print("\nTraitement terminé.")

if __name__ == "__main__":
    main()