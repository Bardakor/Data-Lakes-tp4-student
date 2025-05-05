import argparse
from datetime import datetime
import mysql.connector
from pymongo import MongoClient
from transformers import GPT2Tokenizer
import pandas as pd
from tqdm import tqdm

def get_mysql_data(host, user, password, database):
    """Récupère les données depuis MySQL."""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        query = "SELECT id, text FROM texts"
        df = pd.read_sql(query, connection)
        connection.close()
        
        return df
    except Exception as e:
        print(f"Erreur lors de la récupération des données MySQL: {e}")
        return None

def tokenize_texts(texts, tokenizer):
    """Tokenize les textes avec le tokenizer spécifié."""
    tokenized = []
    for text in tqdm(texts, desc="Tokenization"):
        tokens = tokenizer.encode(text, add_special_tokens=True)
        tokenized.append(tokens)
    return tokenized

def prepare_mongodb_documents(df, tokenized_texts):
    """Prépare les documents pour MongoDB."""
    documents = []
    for idx, row in df.iterrows():
        document = {
            "id": str(row['id']),
            "text": row['text'],
            "tokens": tokenized_texts[idx],
            "metadata": {
                "source": "mysql",
                "processed_at": datetime.utcnow().isoformat(),
                "tokenizer": "gpt2"
            }
        }
        documents.append(document)
    return documents

def insert_to_mongodb(documents, mongo_uri="mongodb://localhost:27017/"):
    """Insère les documents dans MongoDB."""
    try:
        client = MongoClient(mongo_uri)
        db = client.curated
        collection = db.wikitext
        
        # Supprime les documents existants
        collection.delete_many({})
        
        # Insère les nouveaux documents
        result = collection.insert_many(documents)
        print(f"Nombre de documents insérés: {len(result.inserted_ids)}")
        
        # Vérifie quelques documents
        print("\nExemple de documents insérés:")
        for doc in collection.find().limit(2):
            print(f"\nID: {doc['id']}")
            print(f"Text (premiers 50 caractères): {doc['text'][:50]}...")
            print(f"Nombre de tokens: {len(doc['tokens'])}")
            print(f"Processed at: {doc['metadata']['processed_at']}")
        
        client.close()
        return True
    except Exception as e:
        print(f"Erreur lors de l'insertion dans MongoDB: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Traitement des données de staging vers curated')
    parser.add_argument('--mysql_host', type=str, default='localhost', help='Hôte MySQL')
    parser.add_argument('--mysql_user', type=str, default='root', help='Utilisateur MySQL')
    parser.add_argument('--mysql_password', type=str, default='root', help='Mot de passe MySQL')
    parser.add_argument('--mongo_uri', type=str, default='mongodb://localhost:27017/', 
                        help='URI MongoDB')
    
    args = parser.parse_args()
    
    # 1. Récupérer les données de MySQL
    print("Récupération des données depuis MySQL...")
    df = get_mysql_data(
        args.mysql_host,
        args.mysql_user,
        args.mysql_password,
        'staging'
    )
    
    if df is None or df.empty:
        print("Aucune donnée récupérée depuis MySQL")
        return
    
    print(f"Nombre de textes récupérés: {len(df)}")
    
    # 2. Initialiser le tokenizer
    print("\nInitialisation du tokenizer GPT-2...")
    tokenizer = GPT2Tokenizer.from_pretrained('gpt2')
    
    # 3. Tokenizer les textes
    print("\nTokenization des textes...")
    tokenized_texts = tokenize_texts(df['text'].tolist(), tokenizer)
    
    # 4. Préparer les documents pour MongoDB
    print("\nPréparation des documents pour MongoDB...")
    documents = prepare_mongodb_documents(df, tokenized_texts)
    
    # 5. Insérer dans MongoDB
    print("\nInsertion des documents dans MongoDB...")
    success = insert_to_mongodb(documents, args.mongo_uri)
    
    if success:
        print("\nTraitement terminé avec succès!")
    else:
        print("\nErreur lors du traitement")

if __name__ == "__main__":
    main()