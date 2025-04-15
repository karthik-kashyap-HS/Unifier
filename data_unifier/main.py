from pathlib import Path
from App.file_processor import FileProcessor
from tabulate import tabulate
import pandas as pd

def main():
    # Configuration
    input_path = "data/"  # Can be file or folder
    output_dir = "processed/"
    
    # Ensure output directory exists
    Path(output_dir).mkdir(exist_ok=True)
    
    # Process files
    print(" Starting file processing...")
    processor = FileProcessor()
    results = processor.process(input_path, output_dir)
    
    # Print summary
    success = sum(1 for r in results.values() if r['status'] == 'success')
    print(f"\n Processed {len(results)} files ({success} successful)")
    
    # Detailed results
    print("\n File Details:")
    for filename, result in results.items():
        status = "processed" if result['status'] == 'success' else "Failed"
        print(f"\n{status} {filename}")
        print(f"Type: {result.get('file_type', 'unknown')}")
        print(f"Status: {result['status'].upper()}")
        
        if result['status'] == 'success':
            print(f"Rows: {result['rows']}")
            if result['output_path']:
                print(f"Saved to: {result['output_path']}")
            
            # Show sample if successful
            if result['rows'] > 0:
                try:
                    df = pd.read_csv(result['output_path']) if result['output_path'] else None
                    if df is not None and not df.empty:
                        print("\nSample data:")
                        print(tabulate(df.head(3), headers='keys', tablefmt='psql'))
                except Exception as e:
                    print(f"Couldn't display sample: {str(e)}")
        else:
            print(f"Error: {result['error']}")

if __name__ == "__main__":
    main()