import pandas as pd
from pathlib import Path
from typing import Dict, Union
from App.utils.logger import get_logger
from App.utils.file_validators import FileValidator

logger = get_logger(__name__)

class FileProcessor:
    def __init__(self):
        self.validator = FileValidator()
    
    def process(self, input_path: Union[str, Path], output_dir: str = None) -> Dict[str, Dict]:
        """Process files and return results dictionary"""
        input_path = Path(input_path)
        results = {}
        
        if input_path.is_file():
            results[input_path.name] = self._process_single(input_path, output_dir)
        elif input_path.is_dir():
            for file_path in input_path.iterdir():
                if file_path.is_file():
                    results[file_path.name] = self._process_single(file_path, output_dir)
        else:
            raise ValueError(f"Path does not exist: {input_path}")
        
        return results
    
    def _process_single(self, file_path: Path, output_dir: str = None) -> Dict:
        """Process a single file"""
        result = {
            'status': 'failed',
            'file_type': None,
            'output_path': None,
            'rows': 0,
            'error': None
        }
        
        try:
            file_type = self.validator.validate_file(file_path)
            if not file_type:
                raise ValueError(f"Unsupported file type: {file_path.suffix}")
            
            result['file_type'] = file_type
            logger.info(f"Processing {file_type} file: {file_path.name}")
            
            # Load the file
            df = self._load_file(file_path, file_type)
            result['rows'] = len(df)
            
            # Save output if requested
            if output_dir:
                output_path = Path(output_dir) / f"{file_path.stem}.csv"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(output_path, index=False)
                result['output_path'] = str(output_path)
                logger.info(f"Saved processed data to: {output_path}")
            
            result['status'] = 'success'
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Failed to process {file_path.name}: {str(e)}")
        
        return result
    
    def _load_file(self, file_path: Path, file_type: str) -> pd.DataFrame:
        """Load file based on type"""
        if file_type == 'csv':
            return pd.read_csv(file_path)
        elif file_type == 'excel':
            return self._load_excel(file_path)
        elif file_type == 'pdf':
            return self._load_pdf(file_path)
        raise ValueError(f"No loader for file type: {file_type}")
    
    def _load_excel(self, file_path: Path) -> pd.DataFrame:
        """Load Excel file, combining all sheets"""
        xls = pd.ExcelFile(file_path)
        dfs = []
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            df['_sheet'] = sheet_name  # Add sheet name as column
            dfs.append(df)
        return pd.concat(dfs, ignore_index=True)
    
    def _load_pdf(self, file_path: Path) -> pd.DataFrame:
        """Extract tables from PDF"""
        import camelot
        tables = camelot.read_pdf(str(file_path), flavor='lattice')
        if not tables:
            raise ValueError("No tables found in PDF")
        return pd.concat([table.df for table in tables])