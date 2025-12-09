Wiki download and parser, written with claude 4.5

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
  
  # Use regex pattern matching
  python parse_wikipedia.py --search "Sumeria|Mesopotamia" --use-index --regex

  # Search for any number(s) followed by 's' (e.g., "1s", "20s", "1990s")
python parse_wikipedia.py --search "\d+s" --regex --use-index

# Search for 2-digit numbers followed by 's' (e.g., "20s", "30s", "90s")
python parse_wikipedia.py --search "\d{2}s" --regex --use-index

# Search for 4-digit years followed by 's' (e.g., "1920s", "1990s", "2000s")
python parse_wikipedia.py --search "\d{4}s" --regex --use-index

# Search for specific decade patterns (e.g., "1920s", "1930s", etc.)
python parse_wikipedia.py --search "19\d{2}s" --regex --use-index

# Search for numbers 1-9 followed by 's' (e.g., "1s", "2s", "9s")
python parse_wikipedia.py --search "[1-9]s" --regex --use-index

# Search with word boundaries to avoid matching within words
python parse_wikipedia.py --search "\b\d+s\b" --regex --use-index

# Case-sensitive search for numbers followed by 's' or 'S'
python parse_wikipedia.py --search "\d+[sS]" --regex --use-index --case-sensitive

# Search and save results to file
python parse_wikipedia.py --search "\d{4}s" --regex --use-index --output decades_results.txt
l
#Search only years followed by 's' that can end with 'BC' or 'AD' (e.g., "1920s BC", "1990s AD")
python parse_wikipedia.py --search "^\d+s(?: (?:BC|AD))?$" --regex --use-index --output decades.txt
