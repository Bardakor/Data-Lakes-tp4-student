#!/usr/bin/env python3
"""Test script to run the enhanced data pipeline locally"""

import subprocess
import sys
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_command(cmd, description):
    """Run a command and handle errors."""
    logger.info(f"Starting: {description}")
    logger.info(f"Command: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        logger.info(f"✅ {description} completed successfully")
        if result.stdout:
            logger.info(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {description} failed")
        logger.error(f"Error: {e.stderr}")
        return False

def main():
    """Run the enhanced data pipeline."""
    logger.info("=== Starting Enhanced Data Pipeline Test ===")
    
    # Create necessary directories
    os.makedirs("./logs", exist_ok=True)
    os.makedirs("./data/raw", exist_ok=True)
    
    # Step 1: Extract data
    extract_cmd = """python -c "
import argparse
import os
import logging
from datasets import load_dataset
import boto3
from pathlib import Path

# Configure logging for local run
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info('=== Starting data extraction pipeline ===')
    
    # Create directories
    output_dir = './data/raw'
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    for split in ['train', 'test', 'validation']:
        Path(os.path.join(output_dir, split)).mkdir(exist_ok=True)
    
    # Download dataset
    logger.info('Downloading WikiText-2 dataset...')
    dataset = load_dataset('wikitext', 'wikitext-2-raw-v1')
    logger.info(f'Dataset loaded with splits: {list(dataset.keys())}')
    
    # Validate and save data
    total_items = sum(len(dataset[split]) for split in dataset.keys())
    logger.info(f'Total items in dataset: {total_items}')
    
    combined_content = []
    for split in dataset.keys():
        output_file = os.path.join(output_dir, split, f'wikitext-2-{split}.txt')
        valid_items = 0
        split_content = []
        
        for item in dataset[split]['text']:
            if item and item.strip():
                split_content.append(item + '\\n')
                valid_items += 1
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(split_content)
        
        combined_content.extend(split_content)
        logger.info(f'Saved {valid_items} valid items to {split} split')
    
    # Upload to S3
    logger.info('Uploading to S3...')
    s3_client = boto3.client('s3', endpoint_url='http://localhost:4566')
    
    # Create bucket if not exists
    try:
        s3_client.create_bucket(Bucket='raw')
        logger.info('Created raw bucket')
    except:
        logger.info('Raw bucket already exists')
    
    # Save combined file temporarily
    combined_file = os.path.join(output_dir, 'wikitext-2-combined.txt')
    with open(combined_file, 'w', encoding='utf-8') as f:
        f.writelines(combined_content)
    
    # Upload to S3
    s3_client.upload_file(combined_file, 'raw', 'wikitext-2-combined.txt')
    logger.info(f'Successfully uploaded {len(combined_content)} lines to S3')
    
    # Clean up
    os.remove(combined_file)
    logger.info('=== Extraction completed successfully ===')

if __name__ == '__main__':
    main()
" """
    
    if not run_command(extract_cmd, "Data Extraction"):
        return False
    
    # Step 2: Transform data
    transform_cmd = f"""python -c "
import mysql.connector
import boto3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info('=== Starting data transformation ===')
    
    # Get data from S3
    s3_client = boto3.client('s3', endpoint_url='http://localhost:4566')
    response = s3_client.get_object(Bucket='raw', Key='wikitext-2-combined.txt')
    content = response['Body'].read().decode('utf-8')
    logger.info(f'Retrieved {{len(content)}} characters from S3')
    
    # Clean data
    lines = content.split('\\n')
    df = pd.DataFrame(lines, columns=['text'])
    initial_count = len(df)
    
    # Remove empty lines and clean
    df['text'] = df['text'].str.strip()
    df = df[df['text'].astype(bool)]
    df = df[df['text'].str.len() >= 5]
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    
    final_count = len(df)
    logger.info(f'Cleaned data: {{final_count}} rows (removed {{initial_count - final_count}})')
    
    # Connect to MySQL
    conn = mysql.connector.connect(
        host='localhost',
        port=3307,
        user='root',
        password='root'
    )
    
    cursor = conn.cursor()
    cursor.execute('CREATE DATABASE IF NOT EXISTS staging')
    cursor.execute('USE staging')
    
    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS texts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            text TEXT NOT NULL,
            text_length INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Clear existing data
    cursor.execute('DELETE FROM texts')
    
    # Insert data
    insert_query = 'INSERT INTO texts (text, text_length) VALUES (%s, %s)'
    values = [(row['text'], len(row['text'])) for _, row in df.iterrows() 
              if len(row['text']) <= 65535]
    
    cursor.executemany(insert_query, values)
    conn.commit()
    
    logger.info(f'Successfully inserted {{len(values)}} records into MySQL')
    
    # Validate
    cursor.execute('SELECT COUNT(*) FROM texts')
    count = cursor.fetchone()[0]
    logger.info(f'Validation: {{count}} records in database')
    
    conn.close()
    logger.info('=== Transformation completed successfully ===')

if __name__ == '__main__':
    main()
" """
    
    if not run_command(transform_cmd, "Data Transformation"):
        return False
    
    # Step 3: Load data
    load_cmd = """python -c "
import mysql.connector
from pymongo import MongoClient
from transformers import GPT2Tokenizer
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info('=== Starting data loading ===')
    
    # Get data from MySQL
    conn = mysql.connector.connect(
        host='localhost',
        port=3307,
        user='root',
        password='root',
        database='staging'
    )
    
    df = pd.read_sql('SELECT id, text FROM texts WHERE text IS NOT NULL LIMIT 100', conn)
    conn.close()
    logger.info(f'Retrieved {len(df)} records from MySQL')
    
    # Initialize tokenizer
    logger.info('Loading GPT-2 tokenizer...')
    tokenizer = GPT2Tokenizer.from_pretrained('gpt2')
    
    # Tokenize texts
    logger.info('Tokenizing texts...')
    tokenized_texts = []
    for text in df['text']:
        try:
            tokens = tokenizer.encode(text, add_special_tokens=True, max_length=512, truncation=True)
            tokenized_texts.append(tokens)
        except:
            tokenized_texts.append([])
    
    # Prepare documents
    documents = []
    for idx, row in df.iterrows():
        document = {
            'id': str(row['id']),
            'text': row['text'],
            'tokens': tokenized_texts[idx],
            'metadata': {
                'source': 'mysql',
                'processed_at': datetime.utcnow().isoformat(),
                'tokenizer': 'gpt2'
            }
        }
        documents.append(document)
    
    # Insert to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client.curated
    collection = db.wikitext
    
    # Clear existing data
    collection.delete_many({})
    
    # Insert new documents
    result = collection.insert_many(documents)
    logger.info(f'Inserted {len(result.inserted_ids)} documents into MongoDB')
    
    # Validate
    count = collection.count_documents({})
    logger.info(f'Validation: {count} documents in MongoDB')
    
    # Show sample
    sample = collection.find_one()
    if sample:
        logger.info(f'Sample document: ID={sample[\"id\"]}, tokens={len(sample[\"tokens\"])}, text_length={len(sample[\"text\"])}')
    
    client.close()
    logger.info('=== Loading completed successfully ===')

if __name__ == '__main__':
    main()
" """
    
    if not run_command(load_cmd, "Data Loading"):
        return False
    
    logger.info("🎉 === Enhanced Data Pipeline Test Completed Successfully! ===")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 