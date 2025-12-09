
import mwxml
import bz2
from pathlib import Path
import argparse
from tqdm import tqdm
import json
import hashlib
import gc
import re

class WikipediaParser:
    def __init__(self, xml_file_path, index_file_path=None, checkpoint_dir='./wikipedia_checkpoints'):
        self.xml_file_path = Path(xml_file_path)
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        
        # Auto-detect index file if not provided
        if index_file_path is None:
            base_name = str(self.xml_file_path).replace('.xml.bz2', '')
            potential_index = f"{base_name}-index.txt.bz2"
            if Path(potential_index).exists():
                self.index_file_path = Path(potential_index)
            else:
                potential_index = f"{base_name}-index.txt"
                if Path(potential_index).exists():
                    self.index_file_path = Path(potential_index)
                else:
                    self.index_file_path = None
        else:
            self.index_file_path = Path(index_file_path)
        
        if self.index_file_path:
            print(f"Using index file: {self.index_file_path}")
        else:
            print("Warning: No index file found, will search entire dump (slower)")
    
    def _clean_text(self, text):
        """Clean Wikipedia markup from text"""
        if not text:
            return ""
        
        # Remove templates
        text = re.sub(r'\{\{[^}]+\}\}', '', text)
        # Remove file/image references
        text = re.sub(r'\[\[File:[^\]]+\]\]', '', text)
        text = re.sub(r'\[\[Image:[^\]]+\]\]', '', text)
        # Remove categories
        text = re.sub(r'\[\[Category:[^\]]+\]\]', '', text)
        # Remove internal links but keep text
        text = re.sub(r'\[\[([^|\]]+\|)?([^\]]+)\]\]', r'\2', text)
        # Remove external links
        text = re.sub(r'\[http[^\]]+\]', '', text)
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Remove references
        text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL)
        text = re.sub(r'<ref[^>]*\/>', '', text)
        
        return text.strip()
    
    def _search_in_text(self, text, search_term, case_sensitive=False):
        """
        Search for term in text, handling special characters properly.
        Returns (found, count) tuple.
        """
        if not text or not search_term:
            return False, 0
        
        # Use simple string search (not regex) to handle special characters
        if case_sensitive:
            found = search_term in text
            count = text.count(search_term)
        else:
            text_lower = text.lower()
            search_lower = search_term.lower()
            found = search_lower in text_lower
            count = text_lower.count(search_lower)
        
        return found, count
    
    def _get_checkpoint_path(self, search_term, case_sensitive=False):
        """Generate checkpoint file path based on search parameters"""
        search_id = hashlib.md5(
            f"{search_term}_{case_sensitive}_{self.xml_file_path}".encode()
        ).hexdigest()[:16]
        return self.checkpoint_dir / f"checkpoint_{search_id}.json"
    
    def _load_checkpoint(self, search_term, case_sensitive=False):
        """Load checkpoint data if it exists"""
        checkpoint_path = self._get_checkpoint_path(search_term, case_sensitive)
        
        if checkpoint_path.exists():
            try:
                with open(checkpoint_path, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                return checkpoint
            except Exception as e:
                print(f"Warning: Could not load checkpoint: {e}")
                return None
        return None
    
    def _save_checkpoint(self, search_term, case_sensitive, checkpoint_data):
        """Save checkpoint data"""
        checkpoint_path = self._get_checkpoint_path(search_term, case_sensitive)
        try:
            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2)
            print(f"✓ Checkpoint saved")
        except Exception as e:
            print(f"Warning: Could not save checkpoint: {e}")
    
    def _delete_checkpoint(self, search_term, case_sensitive=False):
        """Delete checkpoint file after successful completion"""
        checkpoint_path = self._get_checkpoint_path(search_term, case_sensitive)
        if checkpoint_path.exists():
            try:
                checkpoint_path.unlink()
                print(f"✓ Checkpoint deleted")
            except Exception as e:
                print(f"Warning: Could not delete checkpoint: {e}")
    
    def _write_page_to_file(self, page_data, output_handle, page_num):
        """Write a single page result to file immediately"""
        output_handle.write(f"\n{'='*60}\n")
        output_handle.write(f"Result #{page_num}\n")
        output_handle.write(f"{'='*60}\n")
        output_handle.write(f"Title: {page_data['title']}\n")
        output_handle.write(f"Page ID: {page_data['id']}\n")
        output_handle.write(f"Timestamp: {page_data['timestamp']}\n")
        output_handle.write(f"Occurrences: {page_data['occurrences']}\n")
        output_handle.write(f"Found in title: {'Yes' if page_data['found_in_title'] else 'No'}\n")
        output_handle.write(f"\n{'-'*60}\n")
        output_handle.write("Full Text (cleaned):\n")
        output_handle.write(f"{'-'*60}\n")
        output_handle.write(page_data['cleaned_text'])
        output_handle.write(f"\n{'='*60}\n\n")
        output_handle.flush()  # Ensure data is written to disk immediately
    
    def search_index(self, search_term, case_sensitive=False):
        """Search the index file for matching page titles"""
        if not self.index_file_path or not self.index_file_path.exists():
            print("Error: Index file not found")
            return []
        
        print(f"Searching index for: '{search_term}'")
        matching_entries = []
        
        # Determine if index is compressed
        if self.index_file_path.suffix == '.bz2':
            file_handle = bz2.open(self.index_file_path, 'rt', encoding='utf-8')
        else:
            file_handle = open(self.index_file_path, 'r', encoding='utf-8')
        
        try:
            for line in tqdm(file_handle, desc="Scanning index", unit=" lines"):
                parts = line.strip().split(':', 2)
                if len(parts) == 3:
                    offset, page_id, page_title = parts
                    
                    # Use simple string search to handle special characters
                    found, _ = self._search_in_text(page_title, search_term, case_sensitive)
                    
                    if found:
                        matching_entries.append({
                            'offset': int(offset),
                            'page_id': page_id,
                            'title': page_title
                        })
        finally:
            file_handle.close()
        
        print(f"Found {len(matching_entries)} matching entries in index")
        return matching_entries
    
    def extract_pages_streaming(self, target_page_ids, search_term, case_sensitive, output_file, checkpoint=None):
        """
        Extract specific pages using mwxml for efficient streaming.
        Writes directly to file, no memory accumulation.
        """
        print(f"Extracting pages from dump (streaming mode with mwxml)...")
        print(f"Target pages: {len(target_page_ids)}")
        
        target_ids_set = set(str(pid) for pid in target_page_ids)
        found_ids = set()
        
        # Load checkpoint data if available
        if checkpoint:
            found_ids = set(checkpoint.get('found_ids', []))
            pages_processed = checkpoint.get('pages_processed', 0)
            last_page_id = checkpoint.get('last_page_id', None)
            pages_found = checkpoint.get('pages_found', 0)
            print(f"Resuming from checkpoint: {pages_found} pages already found, {pages_processed} pages processed")
        else:
            pages_processed = 0
            last_page_id = None
            pages_found = 0
        
        # Open output file
        output_path = Path(output_file)
        mode = 'a' if checkpoint else 'w'
        output_handle = open(output_path, mode, encoding='utf-8', buffering=8192)
        
        if not checkpoint:
            output_handle.write(f"Wikipedia Search Results\n")
            output_handle.write(f"Search term: '{search_term}'\n")
            output_handle.write(f"Case sensitive: {case_sensitive}\n")
            output_handle.write(f"{'='*60}\n")
        
        try:
            # Open the compressed XML file with mwxml
            dump = mwxml.Dump.from_file(bz2.open(self.xml_file_path))
            
            skip_until_resume = last_page_id is not None
            current_page_id = None
            
            # Progress bar
            pbar = tqdm(desc="Processing pages", unit=" pages")
            
            for page in dump:
                pages_processed += 1
                pbar.update(1)
                
                current_page_id = str(page.id)
                
                # Skip pages until we reach the resume point
                if skip_until_resume:
                    if current_page_id == last_page_id:
                        skip_until_resume = False
                        print(f"\n✓ Resumed at page ID: {current_page_id}")
                    # Force garbage collection periodically
                    if pages_processed % 1000 == 0:
                        gc.collect()
                    continue
                
                # Check if this is one of our target pages
                if current_page_id in target_ids_set and current_page_id not in found_ids:
                    # Get the latest revision
                    for revision in page:
                        title = page.title
                        text = revision.text or ''
                        timestamp = revision.timestamp.strftime('%Y-%m-%dT%H:%M:%SZ') if revision.timestamp else 'N/A'
                        
                        # Count occurrences using safe string search
                        found_in_text, text_count = self._search_in_text(text, search_term, case_sensitive)
                        found_in_title, title_count = self._search_in_text(title, search_term, case_sensitive)
                        count = text_count + title_count
                        
                        # Clean the text
                        cleaned_text = self._clean_text(text)
                        
                        # Prepare page data
                        page_data = {
                            'id': current_page_id,
                            'title': title,
                            'cleaned_text': cleaned_text,
                            'timestamp': timestamp,
                            'occurrences': count,
                            'found_in_title': found_in_title
                        }
                        
                        # Write to file immediately
                        pages_found += 1
                        self._write_page_to_file(page_data, output_handle, pages_found)
                        
                        # Add to found_ids
                        found_ids.add(current_page_id)
                        print(f"\n✓ Found: {title} ({count} occurrences)")
                        
                        # Only process the latest revision
                        break
                    
                    # Force garbage collection to keep memory usage low
                    if pages_found % 100 == 0:
                        gc.collect()
                    
                    # Stop if we've found all target pages
                    if len(found_ids) == len(target_ids_set):
                        print("\n✓ All target pages found!")
                        break
        
        except Exception as e:
            print(f"\nError during parsing: {e}")
            print("Saving checkpoint before exit...")
            # Return partial results for checkpoint
            checkpoint_data = {
                'pages_processed': pages_processed,
                'found_ids': list(found_ids),
                'pages_found': pages_found,
                'last_page_id': current_page_id
            }
            self._save_checkpoint(search_term, case_sensitive, checkpoint_data)
            raise
        
        finally:
            pbar.close()
            output_handle.close()
        
        # Report any missing pages
        missing_ids = target_ids_set - found_ids
        if missing_ids:
            print(f"\n⚠ Warning: Could not find {len(missing_ids)} pages:")
            for page_id in missing_ids:
                print(f"  - Page ID: {page_id}")
        
        return list(found_ids), pages_processed, None, True
    
    def search_with_index(self, search_term, case_sensitive=False, search_content=False, output_file=None):
        """Search using index file for efficiency"""
        # First, search the index for matching titles
        index_matches = self.search_index(search_term, case_sensitive)
        
        if not index_matches:
            print("No matches found in index")
            return []
        
        # Extract the page IDs we need to find
        target_page_ids = [entry['page_id'] for entry in index_matches]
        
        # Check if we have a checkpoint for this search
        checkpoint = self._load_checkpoint(search_term, case_sensitive)
        
        # Extract pages from the stream
        found_ids, pages_processed, last_page_id, completed = self.extract_pages_streaming(target_page_ids, search_term, case_sensitive, output_file, checkpoint)
        
        # If we completed successfully, delete the checkpoint
        if completed:
            self._delete_checkpoint(search_term, case_sensitive)
        else:
            # Save checkpoint with current progress
            checkpoint_data = {
                'pages_processed': pages_processed,
                'found_ids': list(found_ids),
                'pages_found': len(found_ids),
                'last_page_id': last_page_id
            }
            self._save_checkpoint(search_term, case_sensitive, checkpoint_data)
        
        # Process results
        matching_pages = []
        for page_id in found_ids:
            # For now, just return the IDs since we're writing directly to file
            matching_pages.append({
                'id': page_id,
                'title': 'Not loaded (written to file)',
                'text': '',
                'cleaned_text': '',
                'timestamp': 'N/A',
                'occurrences': 0,
                'found_in_title': False
            })
        
        print(f"\n{'='*60}")
        print(f"Search complete!")
        print(f"Matching pages found: {len(matching_pages)}")
        print(f"{'='*60}\n")
        
        return matching_pages
    
    def parse_compressed_xml(self, search_term, case_sensitive=False, output_file=None):
        """Parse compressed XML file and search for pages containing the search term (fallback method)"""
        print(f"Parsing {self.xml_file_path.name}...")
        print(f"Searching for: '{search_term}' (case {'sensitive' if case_sensitive else 'insensitive'})")
        print("Warning: This method is slow. Consider using --use-index for faster searches.")
        
        matching_pages = []
        total_pages = 0
        
        # Check if we have a checkpoint for this search
        checkpoint = self._load_checkpoint(search_term, case_sensitive)
        
        # Load checkpoint data if available
        if checkpoint:
            matching_pages = checkpoint.get('results', [])
            pages_processed = checkpoint.get('pages_processed', 0)
            last_page_id = checkpoint.get('last_page_id', None)
            print(f"Resuming from checkpoint: {len(matching_pages)} pages already found")
        else:
            pages_processed = 0
            last_page_id = None
        
        # Determine if file is compressed
        if self.xml_file_path.suffix == '.bz2':
            file_handle = bz2.open(self.xml_file_path, 'rt', encoding='utf-8')
        else:
            file_handle = open(self.xml_file_path, 'r', encoding='utf-8')
        
        try:
            # Use iterparse for memory efficiency
            context = ET.iterparse(file_handle, events=('start', 'end'))
            context = iter(context)
            
            # Get root element
            event, root = next(context)
            
            current_page = {}
            in_page = False
            skip_until_resume = last_page_id is not None
            
            for event, elem in tqdm(context, desc="Processing pages", unit=" elements"):
                tag = elem.tag.replace(self.namespace, '')
                
                if event == 'start':
                    if tag == 'page':
                        in_page = True
                        current_page = {}
                
                elif event == 'end':
                    if tag == 'title' and in_page:
                        current_page['title'] = elem.text or ''
                    
                    elif tag == 'id' and in_page and 'id' not in current_page:
                        current_page['id'] = elem.text or ''
                    
                    elif tag == 'text' and in_page:
                        current_page['text'] = elem.text or ''
                    
                    elif tag == 'timestamp' and in_page:
                        current_page['timestamp'] = elem.text or ''
                    
                    elif tag == 'page':
                        total_pages += 1
                        pages_processed += 1
                        in_page = False
                        
                        page_id = current_page.get('id', '')
                        
                        # Skip pages until we reach the resume point
                        if skip_until_resume:
                            if page_id == last_page_id:
                                skip_until_resume = False
                                print(f"\n✓ Resumed at page ID: {page_id}")
                            elem.clear()
                            root.clear()
                            continue
                        
                        # Check if page contains search term
                        text = current_page.get('text', '')
                        title = current_page.get('title', '')
                        
                        # Use safe string search for special characters
                        found_in_text, text_count = self._search_in_text(text, search_term, case_sensitive)
                        found_in_title, title_count = self._search_in_text(title, search_term, case_sensitive)
                        count = text_count + title_count
                        
                        if found_in_text or found_in_title:
                            # Clean the text
                            cleaned_text = self._clean_text(text)
                            
                            # Prepare page data
                            page_data = {
                                'id': current_page.get('id', 'N/A'),
                                'title': title,
                                'text': text,
                                'cleaned_text': cleaned_text,
                                'timestamp': current_page.get('timestamp', 'N/A'),
                                'occurrences': count,
                                'found_in_title': found_in_title
                            }
                            
                            matching_pages.append(page_data)
                            
                            # Write to file immediately if specified
                            if output_file:
                                self._write_page_to_file(page_data, output_handle, len(matching_pages), total_pages)
                            
                            print(f"\n✓ Found match in: {title} ({count} occurrences)")
                        
                        # Clear element to save memory
                        elem.clear()
                        root.clear()
            
        except Exception as e:
            print(f"\nError during parsing: {e}")
            print("Saving checkpoint before exit...")
            # Save checkpoint with current progress
            checkpoint_data = {
                'pages_processed': pages_processed,
                'results': matching_pages,
                'last_page_id': page_id if current_page else last_page_id
            }
            self._save_checkpoint(search_term, case_sensitive, checkpoint_data)
            raise
        
        finally:
            file_handle.close()
        
        # If we completed successfully, delete the checkpoint
        self._delete_checkpoint(search_term, case_sensitive)
        
        print(f"\n{'='*60}")
        print(f"Search complete!")
        print(f"Total pages processed: {total_pages}")
        print(f"Matching pages found: {len(matching_pages)}")
        print(f"{'='*60}\n")
        
        return matching_pages
    
    def display_results(self, results, show_full_text=False, max_text_length=500):
        """Display search results"""
        if not results:
            print("No matching pages found.")
            return
        
        for i, page in enumerate(results, 1):
            print(f"\n{'='*60}")
            print(f"Result {i}/{len(results)}")
            print(f"{'='*60}")
            print(f"Title: {page['title']}")
            print(f"Page ID: {page['id']}")
            print(f"Timestamp: {page['timestamp']}")
            print(f"Occurrences: {page['occurrences']}")
            print(f"Found in title: {'Yes' if page['found_in_title'] else 'No'}")
            print(f"\n{'-'*60}")
            
            if show_full_text:
                print("Full Text (cleaned):")
                print(page['cleaned_text'])
            else:
                print("Text Preview (cleaned):")
                preview = page['cleaned_text'][:max_text_length]
                if len(page['cleaned_text']) > max_text_length:
                    preview += "..."
                print(preview)
            
            print(f"{'='*60}\n")
    
    def save_results_to_file(self, results, output_file):
        """Save results to a text file"""
        output_path = Path(output_file)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(f"Wikipedia Search Results\n")
            f.write(f"{'='*60}\n\n")
            
            for i, page in enumerate(results, 1):
                f.write(f"Result {i}/{len(results)}\n")
                f.write(f"{'='*60}\n")
                f.write(f"Title: {page['title']}\n")
                f.write(f"Page ID: {page['id']}\n")
                f.write(f"Timestamp: {page['timestamp']}\n")
                f.write(f"Occurrences: {page['occurrences']}\n")
                f.write(f"Found in title: {'Yes' if page['found_in_title'] else 'No'}\n")
                f.write(f"\n{'-'*60}\n")
                f.write("Full Text (cleaned):\n")
                f.write(page['cleaned_text'])
                f.write(f"\n\n{'='*60}\n\n")
        
        print(f"✓ Results saved to: {output_path.absolute()}")


def main():
    parser = argparse.ArgumentParser(
        description='Parse Wikipedia XML dump and search for specific terms',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Search for "sumeria" in the XML dump
  python parse_wikipedia.py --search sumeria
  
  # Search with case sensitivity
  python parse_wikipedia.py --search Sumeria --case-sensitive
  
  # Show full text of matching pages
  python parse_wikipedia.py --search sumeria --full-text
  
  # Save results to file
  python parse_wikipedia.py --search sumeria --output results.txt
  
  # Specify custom XML file path
  python parse_wikipedia.py --xml-file ./custom_path/enwiki.xml.bz2 --search sumeria
  
  # Use index file for faster search
  python parse_wikipedia.py --search sumeria --use-index
  
  # Search both title and content
  python parse_wikipedia.py --search sumeria --use-index --search-content
        """
    )
    
    parser.add_argument(
        '--xml-file',
        type=str,
        default='./wikipedia_data/enwiki-20251001-pages-articles-multistream.xml.bz2',
        help='Path to Wikipedia XML dump file (default: ./wikipedia_data/enwiki-20251001-pages-articles-multistream.xml.bz2)'
    )
    
    parser.add_argument(
        '--search',
        type=str,
        required=True,
        help='Search term to find in Wikipedia pages'
    )
    
    parser.add_argument(
        '--case-sensitive',
        action='store_true',
        help='Make search case-sensitive (default: False)'
    )
    
    parser.add_argument(
        '--full-text',
        action='store_true',
        help='Display full text of matching pages (default: show preview only)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        help='Save results to output file'
    )
    
    parser.add_argument(
        '--max-preview',
        type=int,
        default=500,
        help='Maximum characters to show in preview mode (default: 500)'
    )
    
    parser.add_argument(
        '--use-index',
        action='store_true',
        help='Use index file for faster searching (default: False)'
    )
    
    parser.add_argument(
        '--search-content',
        action='store_true',
        help='Search in page content in addition to titles (default: False)'
    )
    
    args = parser.parse_args()
    
    # Check if XML file exists
    xml_path = Path(args.xml_file)
    if not xml_path.exists():
        print(f"Error: XML file not found: {xml_path}")
        print("\nMake sure you've downloaded the Wikipedia dump first:")
        print("  python download_wikipedia.py")
        return
    
    # Initialize parser
    wiki_parser = WikipediaParser(args.xml_file)
    
    # Search for pages
    if args.use_index:
        results = wiki_parser.search_with_index(
            search_term=args.search,
            case_sensitive=args.case_sensitive,
            search_content=args.search_content,
            output_file=args.output
        )
    else:
        results = wiki_parser.parse_compressed_xml(
            search_term=args.search,
            case_sensitive=args.case_sensitive,
            output_file=args.output
        )
    
    # Display results
    wiki_parser.display_results(
        results,
        show_full_text=args.full_text,
        max_text_length=args.max_preview
    )
    
    # Summary
    if results:
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"Found {len(results)} pages containing '{args.search}'")
        for result in results:
            print(f"- {result['title']} ({result['occurrences']} occurrences)")
        print(f"{'='*60}")

if __name__ == "__main__":
    main()
