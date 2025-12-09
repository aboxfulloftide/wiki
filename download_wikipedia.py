import os
import requests
import json
import hashlib
from pathlib import Path
from tqdm import tqdm
import bz2
import shutil
from datetime import datetime
import argparse

class WikipediaDownloader:
    def __init__(self, base_url="https://dumps.wikimedia.org/enwiki/20251001/", 
                 download_dir="./wikipedia_data"):
        self.base_url = base_url
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.progress_file = self.download_dir / "download_progress.json"
        self.md5sums_file = self.download_dir / "enwiki-20251001-md5sums.txt"
        self.progress = self._load_progress()
        self.md5sums = {}
        
    def _load_progress(self):
        """Load progress from JSON file"""
        if self.progress_file.exists():
            try:
                # Check if file is not empty
                if self.progress_file.stat().st_size > 0:
                    with open(self.progress_file, 'r') as f:
                        return json.load(f)
                else:
                    print("Warning: Progress file is empty, creating new progress tracker")
            except json.JSONDecodeError as e:
                print(f"Warning: Progress file is corrupted ({e}), creating new progress tracker")
            except Exception as e:
                print(f"Warning: Could not load progress file ({e}), creating new progress tracker")
        
        return {
            "downloaded_files": {},
            "extracted_files": {},
            "verified_files": {},
            "last_updated": None
        }
    
    def _save_progress(self):
        """Save progress to JSON file"""
        self.progress["last_updated"] = datetime.now().isoformat()
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save progress: {e}")
    
    def _get_file_hash(self, filepath):
        """Calculate MD5 hash of a file"""
        print(f"Calculating MD5 hash for {filepath.name}...")
        hash_md5 = hashlib.md5()
        file_size = filepath.stat().st_size
        
        with open(filepath, "rb") as f, tqdm(
            desc=f"Hashing {filepath.name}",
            total=file_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024
        ) as pbar:
            for chunk in iter(lambda: f.read(4096 * 1024), b""):
                hash_md5.update(chunk)
                pbar.update(len(chunk))
        
        return hash_md5.hexdigest()
    
    def download_md5sums(self):
        """Download the MD5SUMS file"""
        md5sums_url = self.base_url + "enwiki-20251001-md5sums.txt"
        
        if self.md5sums_file.exists():
            print(f"✓ MD5SUMS file already exists")
        else:
            print(f"Downloading MD5SUMS file...")
            try:
                response = requests.get(md5sums_url)
                response.raise_for_status()
                
                with open(self.md5sums_file, 'w') as f:
                    f.write(response.text)
                
                print(f"✓ MD5SUMS file downloaded")
            except Exception as e:
                print(f"✗ Error downloading MD5SUMS: {e}")
                return False
        
        # Parse MD5SUMS file
        try:
            with open(self.md5sums_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split()
                        if len(parts) >= 2:
                            md5_hash = parts[0]
                            filename = parts[1]
                            self.md5sums[filename] = md5_hash
            
            print(f"✓ Loaded {len(self.md5sums)} MD5 checksums")
            return True
        except Exception as e:
            print(f"✗ Error parsing MD5SUMS: {e}")
            return False
    
    def verify_file(self, filepath):
        """Verify a file against its MD5 checksum"""
        filename = filepath.name
        
        # Check if we have the expected hash
        if filename not in self.md5sums:
            print(f"⚠ No MD5 checksum available for {filename}")
            return None
        
        expected_hash = self.md5sums[filename]
        
        # Check if already verified
        if filename in self.progress["verified_files"]:
            stored_hash = self.progress["verified_files"][filename].get("hash")
            if stored_hash == expected_hash:
                print(f"✓ {filename} already verified")
                return True
        
        # Calculate actual hash
        actual_hash = self._get_file_hash(filepath)
        
        # Compare
        if actual_hash == expected_hash:
            print(f"✓ {filename} verification PASSED")
            print(f"  Expected: {expected_hash}")
            print(f"  Actual:   {actual_hash}")
            
            self.progress["verified_files"][filename] = {
                "hash": actual_hash,
                "verified_at": datetime.now().isoformat()
            }
            self._save_progress()
            return True
        else:
            print(f"✗ {filename} verification FAILED")
            print(f"  Expected: {expected_hash}")
            print(f"  Actual:   {actual_hash}")
            return False
    
    def download_file(self, filename, verify=True):
        """Download a single file with progress bar"""
        url = self.base_url + filename
        filepath = self.download_dir / filename
        
        # Check if already downloaded and verified
        if filename in self.progress["downloaded_files"]:
            if filepath.exists():
                if verify and filename in self.progress["verified_files"]:
                    print(f"✓ {filename} already downloaded and verified, skipping...")
                    return filepath
                elif not verify:
                    print(f"✓ {filename} already downloaded, skipping...")
                    return filepath
        
        print(f"Downloading {filename}...")
        
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(filepath, 'wb') as f, tqdm(
                desc=filename,
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    pbar.update(size)
            
            # Store download info
            self.progress["downloaded_files"][filename] = {
                "path": str(filepath),
                "size": filepath.stat().st_size,
                "downloaded_at": datetime.now().isoformat()
            }
            self._save_progress()
            
            print(f"✓ {filename} downloaded successfully")
            
            # Verify if requested
            if verify:
                verification_result = self.verify_file(filepath)
                if verification_result is False:
                    print(f"⚠ File verification failed! Deleting corrupted file...")
                    filepath.unlink()
                    del self.progress["downloaded_files"][filename]
                    self._save_progress()
                    raise Exception(f"File verification failed for {filename}")
            
            return filepath
            
        except Exception as e:
            print(f"✗ Error downloading {filename}: {e}")
            if filepath.exists():
                filepath.unlink()
            raise
    
    def extract_bz2(self, filepath):
        """Extract a bz2 compressed file"""
        output_path = filepath.with_suffix('')
        
        # Check if already extracted
        if str(filepath) in self.progress["extracted_files"]:
            if output_path.exists():
                print(f"✓ {filepath.name} already extracted, skipping...")
                return output_path
        
        print(f"Extracting {filepath.name}...")
        
        try:
            file_size = filepath.stat().st_size
            
            with bz2.open(filepath, 'rb') as f_in, \
                 open(output_path, 'wb') as f_out, \
                 tqdm(desc=f"Extracting {filepath.name}", 
                      total=file_size, 
                      unit='iB',
                      unit_scale=True,
                      unit_divisor=1024) as pbar:
                
                while True:
                    chunk = f_in.read(8192)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    pbar.update(len(chunk))
            
            self.progress["extracted_files"][str(filepath)] = {
                "output_path": str(output_path),
                "extracted_at": datetime.now().isoformat(),
                "size": output_path.stat().st_size
            }
            self._save_progress()
            
            print(f"✓ {filepath.name} extracted successfully")
            return output_path
            
        except Exception as e:
            print(f"✗ Error extracting {filepath.name}: {e}")
            if output_path.exists():
                output_path.unlink()
            raise
    
    def download_wikipedia_dump(self, verify=True):
        """Download the main Wikipedia dump files"""
        # Download MD5SUMS first if verification is requested
        if verify:
            if not self.download_md5sums():
                print("⚠ Warning: Could not download MD5SUMS, proceeding without verification")
                verify = False
        
        # Main files to download
        files_to_download = [
            "enwiki-20251001-pages-articles-multistream.xml.bz2",
            "enwiki-20251001-pages-articles-multistream-index.txt.bz2"
        ]
        
        downloaded_files = []
        
        for filename in files_to_download:
            try:
                filepath = self.download_file(filename, verify=verify)
                downloaded_files.append(filepath)
            except Exception as e:
                print(f"Failed to download {filename}: {e}")
                continue
        
        return downloaded_files
    
    def verify_all_downloads(self):
        """Verify all downloaded files"""
        if not self.md5sums:
            if not self.download_md5sums():
                print("Cannot verify files without MD5SUMS")
                return
        
        print("\nVerifying all downloaded files...")
        
        for filename, info in self.progress["downloaded_files"].items():
            filepath = Path(info["path"])
            if filepath.exists():
                self.verify_file(filepath)
            else:
                print(f"⚠ File not found: {filename}")
    
    def extract_all_downloads(self):
        """Extract all downloaded bz2 files"""
        extracted_files = []
        
        for filename, info in self.progress["downloaded_files"].items():
            filepath = Path(info["path"])
            if filepath.suffix == ".bz2":
                try:
                    output_path = self.extract_bz2(filepath)
                    extracted_files.append(output_path)
                except Exception as e:
                    print(f"Failed to extract {filepath.name}: {e}")
                    continue
        
        return extracted_files
    
    def get_status(self):
        """Print current download and extraction status"""
        print("\n" + "="*60)
        print("Wikipedia Download Status")
        print("="*60)
        
        print(f"\nDownloaded Files: {len(self.progress['downloaded_files'])}")
        for filename, info in self.progress["downloaded_files"].items():
            size_mb = info["size"] / (1024 * 1024)
            print(f"  ✓ {filename} ({size_mb:.2f} MB)")
        
        print(f"\nVerified Files: {len(self.progress['verified_files'])}")
        for filename, info in self.progress["verified_files"].items():
            print(f"  ✓ {filename}")
        
        print(f"\nExtracted Files: {len(self.progress['extracted_files'])}")
        for filepath, info in self.progress["extracted_files"].items():
            size_mb = info["size"] / (1024 * 1024)
            output_name = Path(info["output_path"]).name
            print(f"  ✓ {output_name} ({size_mb:.2f} MB)")
        
        if self.progress["last_updated"]:
            print(f"\nLast Updated: {self.progress['last_updated']}")
        
        print("="*60 + "\n")
    
    def cleanup_compressed_files(self):
        """Remove compressed files after successful extraction"""
        for filename, info in self.progress["downloaded_files"].items():
            filepath = Path(info["path"])
            if filepath.suffix == ".bz2" and str(filepath) in self.progress["extracted_files"]:
                try:
                    filepath.unlink()
                    print(f"Removed compressed file: {filename}")
                except Exception as e:
                    print(f"Could not remove {filename}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Download and optionally extract Wikipedia dumps',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download only (default)
  python download_wikipedia.py
  
  # Download and extract
  python download_wikipedia.py --extract
  
  # Download, extract, and cleanup compressed files
  python download_wikipedia.py --extract --cleanup
  
  # Show status only
  python download_wikipedia.py --status
        """
    )
    
    parser.add_argument(
        '--extract',
        action='store_true',
        help='Extract downloaded files after download (default: False)'
    )
    
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Remove compressed files after successful extraction (requires --extract)'
    )
    
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show current download/extraction status and exit'
    )
    
    parser.add_argument(
        '--download-dir',
        type=str,
        default='./wikipedia_data',
        help='Directory to store downloaded files (default: ./wikipedia_data)'
    )
    
    parser.add_argument(
        '--base-url',
        type=str,
        default='https://dumps.wikimedia.org/enwiki/20251001/',
        help='Base URL for Wikipedia dumps (default: enwiki/20251001/)'
    )
    
    parser.add_argument(
        '--no-verify',
        action='store_true',
        help='Skip file verification (faster but less safe)'
    )
    
    args = parser.parse_args()
    
    # Initialize downloader
    downloader = WikipediaDownloader(
        base_url=args.base_url,
        download_dir=args.download_dir
    )
    
    # Show current status
    downloader.get_status()
    
    # If status only, exit
    if args.status:
        return
    
    # Download files
    print("Starting Wikipedia dump download...")
    downloaded_files = downloader.download_wikipedia_dump(verify=not args.no_verify)
    
    # Extract files if requested
    if args.extract:
        print("\nStarting extraction...")
        extracted_files = downloader.extract_all_downloads()
        
        # Cleanup if requested
        if args.cleanup:
            print("\nCleaning up compressed files...")
            downloader.cleanup_compressed_files()
    else:
        print("\nSkipping extraction (use --extract to extract files)")
    
    # Show final status
    downloader.get_status()
    
    print("\n✓ Download complete!")
    print(f"Files are located in: {downloader.download_dir.absolute()}")
    
    if not args.extract:
        print("\nTo extract files later, run:")
        print(f"  python download_wikipedia.py --extract")


if __name__ == "__main__":
    main()