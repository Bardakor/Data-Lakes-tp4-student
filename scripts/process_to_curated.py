import argparse
from datetime import datetime
import mysql.connector
from pymongo import MongoClient
from transformers import GPT2Tokenizer
import pandas as pd
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/curated_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def validate_mysql_data(df):
    """Validate data retrieved from MySQL."""
    if df is None or df.empty:
        return False, "DataFrame is empty or None"
    
    # Check required columns
    if 'id' not in df.columns or 'text' not in df.columns:
        return False, "Missing required columns (id, text)"
    
    # Check data quality
    null_count = df['text'].isnull().sum()
    empty_count = (df['text'] == '').sum()
    duplicate_count = df.duplicated().sum()
    
    validation_info = {
        "total_rows": len(df),
        "null_texts": null_count,
        "empty_texts": empty_count,
        "duplicates": duplicate_count,
        "avg_text_length": df['text'].str.len().mean()
    }
    
    logger.info(f"MySQL data validation: {validation_info}")
    
    # Quality checks
    issues = []
    if null_count > 0:
        issues.append(f"Found {null_count} null text values")
    if empty_count > 0:
        issues.append(f"Found {empty_count} empty text values")
    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate rows")
    
    if issues:
        for issue in issues:
            logger.warning(f"Data quality issue: {issue}")
        return False, issues
    
    logger.info("MySQL data validation passed")
    return True, validation_info

def get_mysql_data(host, user, password, database):
    """Récupère les données depuis MySQL."""
    try:
        logger.info(f"Connecting to MySQL at {host}")
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        # Check if table exists and has data
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES LIKE 'texts'")
        if not cursor.fetchone():
            raise ValueError("Table 'texts' does not exist")
        
        cursor.execute("SELECT COUNT(*) FROM texts")
        row_count = cursor.fetchone()[0]
        if row_count == 0:
            raise ValueError("Table 'texts' is empty")
        
        logger.info(f"Found {row_count} rows in texts table")
        
        query = "SELECT id, text FROM texts WHERE text IS NOT NULL AND text != '' ORDER BY id"
        df = pd.read_sql(query, connection)
        connection.close()
        
        # Validate retrieved data
        is_valid, validation_result = validate_mysql_data(df)
        if not is_valid:
            raise ValueError(f"Data validation failed: {validation_result}")
        
        logger.info(f"Successfully retrieved {len(df)} valid records from MySQL")
        return df
        
    except Exception as e:
        logger.error(f"Error retrieving data from MySQL: {e}")
        return None

def validate_tokenized_data(texts, tokenized_texts):
    """Validate tokenization results."""
    if len(texts) != len(tokenized_texts):
        return False, "Mismatch between text count and tokenized count"
    
    # Check tokenization quality
    empty_tokenizations = sum(1 for tokens in tokenized_texts if not tokens)
    very_short_tokenizations = sum(1 for tokens in tokenized_texts if len(tokens) < 2)
    very_long_tokenizations = sum(1 for tokens in tokenized_texts if len(tokens) > 1000)
    
    validation_info = {
        "total_texts": len(texts),
        "empty_tokenizations": empty_tokenizations,
        "very_short": very_short_tokenizations,
        "very_long": very_long_tokenizations,
        "avg_tokens": sum(len(tokens) for tokens in tokenized_texts) / len(tokenized_texts)
    }
    
    logger.info(f"Tokenization validation: {validation_info}")
    
    issues = []
    if empty_tokenizations > 0:
        issues.append(f"Found {empty_tokenizations} empty tokenizations")
    if very_short_tokenizations > len(texts) * 0.1:
        issues.append(f"Many very short tokenizations: {very_short_tokenizations}")
    
    if issues:
        for issue in issues:
            logger.warning(f"Tokenization issue: {issue}")
        return False, issues
    
    logger.info("Tokenization validation passed")
    return True, validation_info

def tokenize_texts(texts, tokenizer):
    """Tokenize les textes avec le tokenizer spécifié."""
    try:
        logger.info(f"Starting tokenization of {len(texts)} texts")
        tokenized = []
        failed_count = 0
        
        for i, text in enumerate(tqdm(texts, desc="Tokenization")):
            try:
                if not text or not text.strip():
                    logger.warning(f"Empty text at index {i}")
                    tokenized.append([])
                    failed_count += 1
                    continue
                
                tokens = tokenizer.encode(text, add_special_tokens=True, max_length=1024, truncation=True)
                tokenized.append(tokens)
                
            except Exception as e:
                logger.warning(f"Failed to tokenize text at index {i}: {e}")
                tokenized.append([])
                failed_count += 1
        
        # Validate tokenization results
        is_valid, validation_result = validate_tokenized_data(texts, tokenized)
        if not is_valid:
            logger.warning(f"Tokenization validation issues: {validation_result}")
        
        logger.info(f"Tokenization completed. Failed: {failed_count}/{len(texts)}")
        return tokenized
        
    except Exception as e:
        logger.error(f"Error during tokenization: {e}")
        raise

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

def validate_mongodb_documents(documents):
    """Validate documents before MongoDB insertion."""
    if not documents:
        return False, "No documents to validate"
    
    # Check document structure
    required_fields = ['id', 'text', 'tokens', 'metadata']
    invalid_docs = 0
    empty_tokens = 0
    
    for i, doc in enumerate(documents):
        # Check required fields
        missing_fields = [field for field in required_fields if field not in doc]
        if missing_fields:
            logger.warning(f"Document {i} missing fields: {missing_fields}")
            invalid_docs += 1
            continue
        
        # Check token quality
        if not doc['tokens']:
            empty_tokens += 1
    
    validation_info = {
        "total_documents": len(documents),
        "invalid_documents": invalid_docs,
        "empty_token_docs": empty_tokens,
        "avg_tokens_per_doc": sum(len(doc.get('tokens', [])) for doc in documents) / len(documents)
    }
    
    logger.info(f"Document validation: {validation_info}")
    
    if invalid_docs > 0:
        logger.warning(f"Found {invalid_docs} invalid documents")
        return False, validation_info
    
    logger.info("Document validation passed")
    return True, validation_info

def insert_to_mongodb(documents, mongo_uri="mongodb://localhost:27017/"):
    """Insère les documents dans MongoDB."""
    try:
        logger.info(f"Connecting to MongoDB at {mongo_uri}")
        
        # Validate documents before insertion
        is_valid, validation_result = validate_mongodb_documents(documents)
        if not is_valid:
            logger.warning(f"Document validation issues: {validation_result}")
        
        client = MongoClient(mongo_uri)
        
        # Test connection
        client.admin.command('ping')
        logger.info("MongoDB connection successful")
        
        db = client.curated
        collection = db.wikitext
        
        # Get existing document count
        existing_count = collection.count_documents({})
        if existing_count > 0:
            logger.info(f"Found {existing_count} existing documents, will replace them")
        
        # Supprime les documents existants
        delete_result = collection.delete_many({})
        logger.info(f"Deleted {delete_result.deleted_count} existing documents")
        
        # Insérer les nouveaux documents par batch pour éviter les timeouts
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            try:
                result = collection.insert_many(batch, ordered=False)
                total_inserted += len(result.inserted_ids)
                logger.info(f"Inserted batch: {total_inserted}/{len(documents)} documents")
            except Exception as e:
                logger.error(f"Error inserting batch {i//batch_size + 1}: {e}")
                continue
        
        logger.info(f"Total documents inserted: {total_inserted}")
        
        # Valider l'insertion
        final_count = collection.count_documents({})
        if final_count != total_inserted:
            logger.warning(f"Count mismatch: inserted {total_inserted}, found {final_count}")
        
        # Vérifie quelques documents
        logger.info("Sample inserted documents:")
        for doc in collection.find().limit(2):
            logger.info(f"ID: {doc['id']}, Text length: {len(doc['text'])}, Tokens: {len(doc['tokens'])}")
        
        client.close()
        return total_inserted > 0
        
    except Exception as e:
        logger.error(f"Error inserting into MongoDB: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Traitement des données de staging vers curated')
    parser.add_argument('--mysql_host', type=str, default='localhost', help='Hôte MySQL')
    parser.add_argument('--mysql_user', type=str, default='root', help='Utilisateur MySQL')
    parser.add_argument('--mysql_password', type=str, default='root', help='Mot de passe MySQL')
    parser.add_argument('--mongo_uri', type=str, default='mongodb://localhost:27017/', 
                        help='URI MongoDB')
    
    args = parser.parse_args()
    
    try:
        logger.info("=== Starting curated data processing pipeline ===")
        
        # 1. Récupérer les données de MySQL
        logger.info("Retrieving data from MySQL staging...")
        df = get_mysql_data(
            args.mysql_host,
            args.mysql_user,
            args.mysql_password,
            'staging'
        )
        
        if df is None or df.empty:
            raise ValueError("No data retrieved from MySQL staging")
        
        logger.info(f"Retrieved {len(df)} records from MySQL")
        
        # 2. Initialiser le tokenizer
        logger.info("Initializing GPT-2 tokenizer...")
        try:
            tokenizer = GPT2Tokenizer.from_pretrained('gpt2')
            logger.info("Tokenizer loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load tokenizer: {e}")
            raise
        
        # 3. Tokenizer les textes
        logger.info("Tokenizing texts...")
        tokenized_texts = tokenize_texts(df['text'].tolist(), tokenizer)
        
        # 4. Préparer les documents pour MongoDB
        logger.info("Preparing documents for MongoDB...")
        documents = prepare_mongodb_documents(df, tokenized_texts)
        
        if not documents:
            raise ValueError("No documents prepared for MongoDB")
        
        # 5. Insérer dans MongoDB
        logger.info("Inserting documents into MongoDB...")
        success = insert_to_mongodb(documents, args.mongo_uri)
        
        if success:
            logger.info("=== Curated processing pipeline completed successfully ===")
        else:
            raise RuntimeError("MongoDB insertion failed")
            
    except Exception as e:
        logger.error(f"=== Curated processing pipeline failed: {e} ===")
        raise

if __name__ == "__main__":
    main()