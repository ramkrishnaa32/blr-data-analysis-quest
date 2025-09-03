"""
Part 1: BLS Data Synchronization
Downloads files from Bureau of Labor Statistics website and uploads to S3
Uses utils helper functions for add, delete, and update operations
"""

from utils.helperFunctions import (
    get_file_list_from_bls, 
    get_s3_metadata, 
    save_s3_metadata,
    should_update_file,
    download_and_upload_file,
    delete_from_s3
)

def main():
    """Main function that implements add, delete, and update logic"""
    print("Starting BLS data synchronization...")
    
    try:
        # Get current file list from BLS
        bls_files = get_file_list_from_bls()
        print(f"Found {len(bls_files)} files on BLS website")
        
        # Get existing metadata from S3
        existing_metadata = get_s3_metadata()
        existing_files = existing_metadata.get('files', {})
        
        # Track operations
        added_files = 0
        updated_files = 0
        deleted_files = 0
        
        # Process each file from BLS
        for filename, file_info in bls_files.items():
            if filename in existing_files:
                # File exists - check if it needs updating
                if should_update_file(filename, file_info, existing_files[filename]):
                    print(f"Updating file: {filename}")
                    new_hash = download_and_upload_file(filename, file_info)
                    if new_hash:
                        existing_files[filename]['file_hash'] = new_hash
                        updated_files += 1
                else:
                    print(f"File unchanged: {filename}")
            else:
                # New file - download and upload
                print(f"Adding new file: {filename}")
                new_hash = download_and_upload_file(filename, file_info)
                if new_hash:
                    existing_files[filename] = {
                        'url': file_info['url'],
                        'last_modified': file_info['last_modified'],
                        'file_hash': new_hash
                    }
                    added_files += 1
        
        # Check for deleted files (files that exist in S3 but not on BLS)
        for filename in list(existing_files.keys()):
            if filename not in bls_files:
                print(f"Deleting removed file: {filename}")
                delete_from_s3(filename)
                del existing_files[filename]
                deleted_files += 1
        
        # Update metadata
        existing_metadata['files'] = existing_files
        existing_metadata['last_sync'] = existing_metadata.get('last_sync', 'Never')
        save_s3_metadata(existing_metadata)
        
        print(f"BLS sync completed successfully!")
        print(f"Added: {added_files}, Updated: {updated_files}, Deleted: {deleted_files}")
        print(f"Total files: {len(existing_files)}")
        
    except Exception as e:
        print(f"Error in BLS sync: {e}")
        raise

if __name__ == "__main__":
    main()
