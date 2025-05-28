import argparse
import os
import logging
from datasets import load_dataset
import boto3
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/extract_validation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def validate_dataset_structure(dataset):
    """Validate dataset structure and content."""
    validation_errors = []
    
    # Check if dataset has required splits
    required_splits = ['train', 'test', 'validation']
    for split in required_splits:
        if split not in dataset:
            validation_errors.append(f"Missing required split: {split}")
    
    # Check data content
    total_items = 0
    empty_items = 0
    
    for split_name, split_data in dataset.items():
        if 'text' not in split_data.column_names:
            validation_errors.append(f"Missing 'text' column in {split_name} split")
            continue
            
        split_items = len(split_data)
        total_items += split_items
        
        # Check for empty texts
        for item in split_data['text']:
            if not item or not item.strip():
                empty_items += 1
    
    # Log validation results
    logger.info(f"Dataset validation - Total items: {total_items}, Empty items: {empty_items}")
    
    if validation_errors:
        for error in validation_errors:
            logger.error(f"Dataset validation error: {error}")
        return False, validation_errors
    
    if total_items == 0:
        logger.error("Dataset contains no data")
        return False, ["Dataset is empty"]
    
    if empty_items / total_items > 0.5:
        logger.warning(f"High percentage of empty items: {empty_items/total_items:.2%}")
    
    logger.info("Dataset validation passed")
    return True, []

def download_wikitext(output_dir):
    """Télécharge les données WikiText-2 et les organise dans les sous-dossiers."""
    
    try:
        # Créer les répertoires nécessaires
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        for split in ['train', 'test', 'validation']:
            Path(os.path.join(output_dir, split)).mkdir(exist_ok=True)
        
        # Télécharger le dataset
        logger.info("Downloading WikiText-2 dataset...")
        dataset = load_dataset("wikitext", "wikitext-2-raw-v1")
        
        # Validate dataset
        is_valid, errors = validate_dataset_structure(dataset)
        if not is_valid:
            raise ValueError(f"Dataset validation failed: {errors}")
        
        # Sauvegarder chaque split dans son dossier respectif
        for split in dataset.keys():
            output_file = os.path.join(output_dir, split, f'wikitext-2-{split}.txt')
            
            valid_items = 0
            with open(output_file, 'w', encoding='utf-8') as f:
                for item in dataset[split]['text']:
                    if item and item.strip():  # Éviter les lignes vides
                        f.write(item + '\n')
                        valid_items += 1
            
            logger.info(f"Saved {valid_items} valid items to {split} split")
            
    except Exception as e:
        logger.error(f"Error downloading dataset: {e}")
        raise

def combine_and_upload(input_dir, endpoint_url):
    """Combine les fichiers et les téléverse dans le bucket raw."""
    try:
        # Initialiser le client S3
        s3_client = boto3.client('s3', endpoint_url=endpoint_url)
        
        # Validate S3 connection
        try:
            s3_client.list_buckets()
            logger.info("S3 connection validated")
        except Exception as e:
            logger.error(f"S3 connection failed: {e}")
            raise
        
        # Combiner tous les fichiers
        combined_content = []
        total_files = 0
        
        for split in ['train', 'test', 'validation']:
            split_dir = os.path.join(input_dir, split)
            if not os.path.exists(split_dir):
                logger.warning(f"Split directory not found: {split_dir}")
                continue
                
            for filename in os.listdir(split_dir):
                file_path = os.path.join(split_dir, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.readlines()
                        combined_content.extend(content)
                        total_files += 1
                        logger.info(f"Added {len(content)} lines from {filename}")
                except Exception as e:
                    logger.error(f"Error reading file {file_path}: {e}")
                    continue
        
        if not combined_content:
            raise ValueError("No content to upload - all files were empty or unreadable")
        
        # Sauvegarder le fichier combiné temporairement
        combined_file = os.path.join(input_dir, 'wikitext-2-combined.txt')
        with open(combined_file, 'w', encoding='utf-8') as f:
            f.writelines(combined_content)
        
        logger.info(f"Combined {len(combined_content)} lines from {total_files} files")
        
        # Téléverser vers S3
        try:
            s3_client.upload_file(
                combined_file,
                'raw',
                'wikitext-2-combined.txt'
            )
            logger.info(f"File uploaded successfully to s3://raw/wikitext-2-combined.txt")
        except Exception as e:
            logger.error(f"Upload error: {e}")
            raise
        
        # Nettoyer le fichier temporaire
        os.remove(combined_file)
        
    except Exception as e:
        logger.error(f"Error in combine_and_upload: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Télécharge et traite les données WikiText-2')
    parser.add_argument('--output-dir', type=str, default='data/raw',
                        help='Répertoire de sortie pour les données')
    parser.add_argument('--endpoint-url', type=str, default='http://localhost:4566',
                        help='URL du endpoint S3 (LocalStack)')
    
    args = parser.parse_args()
    
    try:
        logger.info("=== Starting data extraction pipeline ===")
        
        logger.info("Downloading and validating data...")
        download_wikitext(args.output_dir)
        logger.info("Data downloaded and organized successfully.")
        
        logger.info("Combining and uploading files...")
        combine_and_upload(args.output_dir, args.endpoint_url)
        logger.info("Processing completed successfully.")
        
        logger.info("=== Pipeline completed successfully ===")
        
    except Exception as e:
        logger.error(f"=== Pipeline failed: {e} ===")
        raise

if __name__ == "__main__":
    main()