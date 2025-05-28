import argparse
import boto3
import mysql.connector
from mysql.connector import Error
import pandas as pd
from io import StringIO
import logging
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/preprocessing_validation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def validate_content_quality(content):
    """Validate content quality and return quality metrics."""
    if not content:
        return False, {"error": "Content is empty"}
    
    lines = content.split('\n')
    total_lines = len(lines)
    empty_lines = sum(1 for line in lines if not line.strip())
    short_lines = sum(1 for line in lines if len(line.strip()) < 10)
    non_ascii_lines = sum(1 for line in lines if not line.isascii())
    
    metrics = {
        "total_lines": total_lines,
        "empty_lines": empty_lines,
        "short_lines": short_lines,
        "non_ascii_lines": non_ascii_lines,
        "empty_ratio": empty_lines / total_lines if total_lines > 0 else 0,
        "short_ratio": short_lines / total_lines if total_lines > 0 else 0
    }
    
    # Quality checks
    quality_issues = []
    if metrics["empty_ratio"] > 0.3:
        quality_issues.append(f"High empty line ratio: {metrics['empty_ratio']:.2%}")
    if metrics["short_ratio"] > 0.5:
        quality_issues.append(f"High short line ratio: {metrics['short_ratio']:.2%}")
    if total_lines < 100:
        quality_issues.append(f"Very few lines: {total_lines}")
    
    is_valid = len(quality_issues) == 0
    logger.info(f"Content quality metrics: {metrics}")
    
    if quality_issues:
        for issue in quality_issues:
            logger.warning(f"Quality issue: {issue}")
    
    return is_valid, metrics

def get_data_from_raw(endpoint_url, bucket_name, file_name="wikitext-2-combined.txt"):
    """Récupère les données depuis le bucket raw."""
    try:
        logger.info(f"Retrieving data from s3://{bucket_name}/{file_name}")
        s3_client = boto3.client('s3', endpoint_url=endpoint_url)
        
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            logger.error(f"Bucket {bucket_name} not accessible: {e}")
            return None
        
        # Check if file exists
        try:
            s3_client.head_object(Bucket=bucket_name, Key=file_name)
        except Exception as e:
            logger.error(f"File {file_name} not found in bucket {bucket_name}: {e}")
            return None
        
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response['Body'].read().decode('utf-8')
        
        # Validate content quality
        is_valid, metrics = validate_content_quality(content)
        if not is_valid:
            logger.warning("Content quality issues detected, but proceeding with processing")
        
        logger.info(f"Successfully retrieved {len(content)} characters from S3")
        return content
        
    except Exception as e:
        logger.error(f"Error retrieving data from S3: {e}")
        return None

def validate_dataframe(df):
    """Validate cleaned DataFrame."""
    validation_results = {
        "total_rows": len(df),
        "empty_texts": df['text'].isnull().sum() + (df['text'] == '').sum(),
        "duplicate_rows": df.duplicated().sum(),
        "avg_text_length": df['text'].str.len().mean(),
        "min_text_length": df['text'].str.len().min(),
        "max_text_length": df['text'].str.len().max()
    }
    
    # Validation checks
    issues = []
    if validation_results["total_rows"] == 0:
        issues.append("DataFrame is empty")
    if validation_results["empty_texts"] > 0:
        issues.append(f"Found {validation_results['empty_texts']} empty texts")
    if validation_results["avg_text_length"] < 20:
        issues.append(f"Average text length too short: {validation_results['avg_text_length']:.1f}")
    
    logger.info(f"DataFrame validation results: {validation_results}")
    
    if issues:
        for issue in issues:
            logger.warning(f"DataFrame validation issue: {issue}")
        return False, validation_results
    
    logger.info("DataFrame validation passed")
    return True, validation_results

def clean_data(content):
    """Nettoie les données (suppression des doublons et des lignes vides)."""
    try:
        # Convertir le contenu en DataFrame
        lines = content.split('\n')
        df = pd.DataFrame(lines, columns=['text'])
        
        initial_count = len(df)
        logger.info(f"Initial data count: {initial_count}")
        
        # Supprimer les lignes vides et nettoyer
        df['text'] = df['text'].str.strip()
        df = df[df['text'].astype(bool)]
        
        # Remove lines that are too short (likely noise)
        df = df[df['text'].str.len() >= 5]
        
        # Supprimer les doublons
        df = df.drop_duplicates()
        
        # Réinitialiser l'index
        df = df.reset_index(drop=True)
        
        final_count = len(df)
        logger.info(f"After cleaning: {final_count} rows (removed {initial_count - final_count})")
        
        # Validate cleaned data
        is_valid, metrics = validate_dataframe(df)
        if not is_valid:
            logger.warning("Data validation issues found, but proceeding")
        
        return df
        
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
        raise

def create_mysql_connection(host, user, password, database):
    """Crée une connexion MySQL."""
    try:
        logger.info(f"Connecting to MySQL at {host}")
        
        # First try to connect without database to create it if needed
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        cursor.execute(f"USE {database}")
        connection.commit()
        cursor.close()
        
        logger.info(f"Successfully connected to MySQL database: {database}")
        return connection
        
    except Error as e:
        logger.error(f"MySQL connection error: {e}")
        return None

def create_table(connection):
    """Crée la table texts si elle n'existe pas."""
    try:
        cursor = connection.cursor()
        
        # Check if table exists
        cursor.execute("SHOW TABLES LIKE 'texts'")
        if cursor.fetchone():
            logger.info("Table 'texts' already exists")
        else:
            logger.info("Creating table 'texts'")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS texts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                text TEXT NOT NULL,
                text_length INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_text_length (text_length),
                INDEX idx_created_at (created_at)
            )
        """)
        connection.commit()
        logger.info("Table creation/verification completed")
        
    except Error as e:
        logger.error(f"Error creating table: {e}")
        raise

def insert_data(connection, df):
    """Insère les données dans la table texts."""
    try:
        cursor = connection.cursor()
        
        # Clear existing data
        cursor.execute("DELETE FROM texts")
        logger.info("Cleared existing data from texts table")
        
        # Préparer la requête d'insertion avec validation
        insert_query = "INSERT INTO texts (text, text_length) VALUES (%s, %s)"
        
        # Prepare and validate data
        valid_records = 0
        invalid_records = 0
        values = []
        
        for _, row in df.iterrows():
            text = row['text']
            text_length = len(text)
            
            # Additional validation
            if text and len(text.strip()) >= 5 and text_length <= 65535:  # MySQL TEXT limit
                values.append((text, text_length))
                valid_records += 1
            else:
                invalid_records += 1
        
        if not values:
            raise ValueError("No valid records to insert")
        
        # Insérer les données par batch pour éviter les timeouts
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            total_inserted += len(batch)
            logger.info(f"Inserted batch: {total_inserted}/{len(values)} records")
        
        connection.commit()
        logger.info(f"Successfully inserted {total_inserted} records, skipped {invalid_records} invalid records")
        
    except Error as e:
        logger.error(f"Error inserting data: {e}")
        connection.rollback()
        raise

def validate_data(connection):
    """Valide les données insérées avec des requêtes SQL."""
    try:
        cursor = connection.cursor()
        
        # Basic counts and statistics
        validations = {}
        
        # Compte total des lignes
        cursor.execute("SELECT COUNT(*) FROM texts")
        validations['total_count'] = cursor.fetchone()[0]
        
        # Compte des lignes non vides
        cursor.execute("SELECT COUNT(*) FROM texts WHERE text IS NOT NULL AND text != ''")
        validations['non_empty_count'] = cursor.fetchone()[0]
        
        # Text length statistics
        cursor.execute("SELECT AVG(text_length), MIN(text_length), MAX(text_length) FROM texts")
        avg_len, min_len, max_len = cursor.fetchone()
        validations['avg_length'] = avg_len
        validations['min_length'] = min_len
        validations['max_length'] = max_len
        
        # Log validation results
        logger.info(f"Data validation results: {validations}")
        
        # Validation checks
        issues = []
        if validations['total_count'] == 0:
            issues.append("No data found in database")
        if validations['non_empty_count'] != validations['total_count']:
            issues.append(f"Found {validations['total_count'] - validations['non_empty_count']} empty records")
        if avg_len and avg_len < 20:
            issues.append(f"Average text length is very short: {avg_len:.1f}")
        
        if issues:
            for issue in issues:
                logger.warning(f"Validation issue: {issue}")
            return False
        
        # Afficher quelques exemples
        cursor.execute("SELECT id, text, text_length FROM texts ORDER BY RAND() LIMIT 3")
        logger.info("Sample records:")
        for row in cursor.fetchall():
            logger.info(f"ID: {row[0]}, Length: {row[2]}, Text: {row[1][:50]}...")
        
        logger.info("Data validation completed successfully")
        return True
            
    except Error as e:
        logger.error(f"Error validating data: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Prépare les données pour le staging dans MySQL')
    parser.add_argument('--bucket_raw', type=str, required=True, help='Nom du bucket raw')
    parser.add_argument('--db_host', type=str, required=True, help='Hôte MySQL')
    parser.add_argument('--db_user', type=str, required=True, help='Utilisateur MySQL')
    parser.add_argument('--db_password', type=str, required=True, help='Mot de passe MySQL')
    parser.add_argument('--endpoint-url', type=str, default='http://localhost:4566',
                        help='URL du endpoint S3 (LocalStack)')
    
    args = parser.parse_args()
    
    try:
        logger.info("=== Starting preprocessing pipeline ===")
        
        # Récupérer les données depuis raw
        logger.info("Retrieving data from raw bucket...")
        content = get_data_from_raw(args.endpoint_url, args.bucket_raw)
        if content is None:
            raise ValueError("Failed to retrieve data from raw bucket")
        
        # Nettoyer les données
        logger.info("Cleaning and validating data...")
        df = clean_data(content)
        if df.empty:
            raise ValueError("No valid data after cleaning")
        
        # Connexion à MySQL
        logger.info("Connecting to MySQL...")
        connection = create_mysql_connection(
            args.db_host,
            args.db_user,
            args.db_password,
            'staging'
        )
        if connection is None:
            raise ConnectionError("Failed to connect to MySQL")
        
        # Créer la table
        logger.info("Setting up database table...")
        create_table(connection)
        
        # Insérer les données
        logger.info("Inserting data into MySQL...")
        insert_data(connection, df)
        
        # Valider les données
        logger.info("Validating inserted data...")
        validation_success = validate_data(connection)
        
        # Fermer la connexion
        connection.close()
        
        if validation_success:
            logger.info("=== Preprocessing pipeline completed successfully ===")
        else:
            logger.warning("=== Pipeline completed with validation warnings ===")
            
    except Exception as e:
        logger.error(f"=== Preprocessing pipeline failed: {e} ===")
        if 'connection' in locals() and connection.is_connected():
            connection.close()
        raise

if __name__ == "__main__":
    main()