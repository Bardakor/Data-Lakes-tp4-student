#!/usr/bin/env python3
"""Complete Enhanced Data Pipeline Runner"""

import logging
import sys
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./logs/complete_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def step1_extraction():
    """Step 1: Data Extraction with Validation"""
    logger.info("=== STEP 1: DATA EXTRACTION WITH VALIDATION ===")
    
    try:
        from datasets import load_dataset
        import boto3
        from pathlib import Path
        
        # Create directories
        output_dir = './data/raw'
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Download and validate dataset
        logger.info('Downloading WikiText-2 dataset...')
        dataset = load_dataset('wikitext', 'wikitext-2-raw-v1')
        logger.info(f'✅ Dataset loaded with splits: {list(dataset.keys())}')
        
        # Process and save data with validation
        combined_content = []
        total_valid = 0
        for split in dataset.keys():
            split_content = []
            valid_items = 0
            for item in dataset[split]['text']:
                if item and item.strip() and len(item.strip()) >= 5:
                    split_content.append(item + '\n')
                    valid_items += 1
            combined_content.extend(split_content)
            total_valid += valid_items
            logger.info(f'✅ {split}: {valid_items} valid items processed')
        
        logger.info(f'✅ Total valid items: {total_valid}')
        
        # Upload to S3 with validation
        s3_client = boto3.client('s3', endpoint_url='http://localhost:4566')
        try:
            s3_client.create_bucket(Bucket='raw')
            logger.info('✅ Created raw bucket')
        except:
            logger.info('✅ Raw bucket already exists')
        
        # Save and upload combined file
        combined_file = os.path.join(output_dir, 'wikitext-2-combined.txt')
        with open(combined_file, 'w', encoding='utf-8') as f:
            f.writelines(combined_content)
        
        s3_client.upload_file(combined_file, 'raw', 'wikitext-2-combined.txt')
        logger.info(f'✅ Successfully uploaded {len(combined_content)} lines to S3')
        
        # Clean up local file
        os.remove(combined_file)
        logger.info("🎯 STEP 1: EXTRACTION COMPLETED SUCCESSFULLY!")
        return True
        
    except Exception as e:
        logger.error(f"❌ STEP 1 FAILED: {e}")
        return False

def step2_transformation():
    """Step 2: Data Transformation with Quality Checks"""
    logger.info("=== STEP 2: DATA TRANSFORMATION WITH QUALITY CHECKS ===")
    
    try:
        import mysql.connector
        import boto3
        import pandas as pd
        
        # Get data from S3
        s3_client = boto3.client('s3', endpoint_url='http://localhost:4566')
        response = s3_client.get_object(Bucket='raw', Key='wikitext-2-combined.txt')
        content = response['Body'].read().decode('utf-8')
        logger.info(f'✅ Retrieved {len(content)} characters from S3')
        
        # Clean and validate data
        lines = content.split('\n')
        df = pd.DataFrame(lines, columns=['text'])
        initial_count = len(df)
        
        # Quality checks
        df['text'] = df['text'].str.strip()
        df = df[df['text'].astype(bool)]  # Remove empty
        df = df[df['text'].str.len() >= 5]  # Minimum length
        df = df[df['text'].str.len() <= 5000]  # Maximum length
        df = df.drop_duplicates()  # Remove duplicates
        df = df.reset_index(drop=True)
        
        final_count = len(df)
        quality_rate = (final_count / initial_count) * 100
        logger.info(f'✅ Data quality: {final_count} records ({quality_rate:.1f}% retention)')
        
        # Connect to MySQL with validation
        conn = mysql.connector.connect(
            host='localhost',
            port=3307,
            user='root',
            password='root'
        )
        
        cursor = conn.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS staging')
        cursor.execute('USE staging')
        
        # Create table with proper schema
        cursor.execute('''
            DROP TABLE IF EXISTS texts
        ''')
        cursor.execute('''
            CREATE TABLE texts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                text TEXT NOT NULL,
                text_length INT GENERATED ALWAYS AS (LENGTH(text)) STORED,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_length (text_length)
            )
        ''')
        
        # Insert data in batches with validation
        insert_query = 'INSERT INTO texts (text) VALUES (%s)'
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            values = [(row['text'],) for _, row in batch.iterrows() if len(row['text']) <= 65535]
            
            if values:
                cursor.executemany(insert_query, values)
                conn.commit()
                total_inserted += len(values)
                logger.info(f'✅ Inserted batch: {len(values)} records (total: {total_inserted})')
        
        # Validate final results
        cursor.execute('SELECT COUNT(*), AVG(text_length), MIN(text_length), MAX(text_length) FROM texts')
        stats = cursor.fetchone()
        logger.info(f'✅ Final MySQL stats: {stats[0]} records, avg_len={stats[1]:.1f}, range={stats[2]}-{stats[3]}')
        
        conn.close()
        logger.info("🎯 STEP 2: TRANSFORMATION COMPLETED SUCCESSFULLY!")
        return True
        
    except Exception as e:
        logger.error(f"❌ STEP 2 FAILED: {e}")
        return False

def step3_loading():
    """Step 3: Data Loading with Tokenization"""
    logger.info("=== STEP 3: DATA LOADING WITH TOKENIZATION ===")
    
    try:
        import mysql.connector
        from pymongo import MongoClient
        from transformers import GPT2Tokenizer
        import pandas as pd
        from datetime import datetime
        
        # Get data from MySQL
        conn = mysql.connector.connect(
            host='localhost',
            port=3307,
            user='root',
            password='root',
            database='staging'
        )
        
        # Get a representative sample for processing
        df = pd.read_sql('SELECT id, text FROM texts WHERE text IS NOT NULL ORDER BY RAND() LIMIT 100', conn)
        conn.close()
        logger.info(f'✅ Retrieved {len(df)} records from MySQL for processing')
        
        # Initialize tokenizer
        logger.info('Loading GPT-2 tokenizer...')
        tokenizer = GPT2Tokenizer.from_pretrained('gpt2')
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token
        
        # Tokenize with error handling
        logger.info('Tokenizing texts...')
        documents = []
        successful_tokenizations = 0
        
        for idx, row in df.iterrows():
            try:
                # Tokenize with proper truncation
                tokens = tokenizer.encode(
                    row['text'], 
                    add_special_tokens=True, 
                    max_length=512, 
                    truncation=True
                )
                
                # Create document with metadata
                document = {
                    'id': str(row['id']),
                    'text': row['text'],
                    'tokens': tokens,
                    'token_count': len(tokens),
                    'text_length': len(row['text']),
                    'metadata': {
                        'source': 'mysql_staging',
                        'processed_at': datetime.utcnow().isoformat(),
                        'tokenizer': 'gpt2',
                        'max_length': 512,
                        'truncated': len(tokenizer.encode(row['text'])) > 512
                    }
                }
                documents.append(document)
                successful_tokenizations += 1
                
            except Exception as e:
                logger.warning(f'Tokenization failed for record {row["id"]}: {e}')
        
        logger.info(f'✅ Successfully tokenized {successful_tokenizations}/{len(df)} texts')
        
        # Insert to MongoDB with validation
        client = MongoClient('mongodb://localhost:27017/')
        db = client.curated
        collection = db.wikitext
        
        # Clear existing data
        collection.delete_many({})
        logger.info('✅ Cleared existing MongoDB data')
        
        # Insert documents in batches
        if documents:
            result = collection.insert_many(documents)
            logger.info(f'✅ Inserted {len(result.inserted_ids)} documents into MongoDB')
            
            # Validate and show statistics
            total_docs = collection.count_documents({})
            avg_tokens = list(collection.aggregate([
                {'$group': {'_id': None, 'avg_tokens': {'$avg': '$token_count'}}}
            ]))[0]['avg_tokens']
            
            sample = collection.find_one()
            logger.info(f'✅ MongoDB validation: {total_docs} docs, avg_tokens={avg_tokens:.1f}')
            logger.info(f'✅ Sample doc: ID={sample["id"]}, tokens={sample["token_count"]}, metadata_keys={list(sample["metadata"].keys())}')
        
        client.close()
        logger.info("🎯 STEP 3: LOADING COMPLETED SUCCESSFULLY!")
        return True
        
    except Exception as e:
        logger.error(f"❌ STEP 3 FAILED: {e}")
        return False

def main():
    """Run the complete enhanced pipeline"""
    start_time = datetime.now()
    logger.info("🚀 STARTING COMPLETE ENHANCED DATA PIPELINE")
    logger.info("=" * 60)
    
    # Create logs directory
    os.makedirs('./logs', exist_ok=True)
    
    # Run all steps
    steps = [
        ("EXTRACTION", step1_extraction),
        ("TRANSFORMATION", step2_transformation),
        ("LOADING", step3_loading)
    ]
    
    results = {}
    for step_name, step_func in steps:
        logger.info(f"\n{'='*20} {step_name} {'='*20}")
        success = step_func()
        results[step_name] = success
        
        if not success:
            logger.error(f"❌ Pipeline failed at step: {step_name}")
            return False
    
    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("\n" + "="*60)
    logger.info("🎉 COMPLETE ENHANCED PIPELINE RESULTS:")
    logger.info("="*60)
    for step_name, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        logger.info(f"{step_name}: {status}")
    
    logger.info(f"\n⏱️  Total execution time: {duration.total_seconds():.1f} seconds")
    logger.info("🎯 ENHANCED DATA PIPELINE COMPLETED SUCCESSFULLY!")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 