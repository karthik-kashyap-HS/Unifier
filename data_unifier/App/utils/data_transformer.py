import pandas as pd
import numpy as np
import re
from typing import List, Tuple, Optional

class DataTransformer:
    """Transforms data from various formats to standardized formats for processing"""
    
    @staticmethod
    def detect_location_columns(df: pd.DataFrame) -> Tuple[List[str], List[str], Optional[str]]:
        """
        Detects columns that might represent locations or categories.
        
        Returns:
        - id_columns: columns to keep as identifiers
        - location_columns: detected location columns
        - value_column: the column that contains the values (e.g. 'Inventory Units')
        """
        # Common patterns for location-type columns
        loc_patterns = [
            # Single letters or single letter with digit
            r'^[A-Z]$', r'^[A-Z]\d+$',
            # Words that suggest locations
            r'(?i)^location.*$', r'(?i)^loc.*$', r'(?i)^store.*$',
            r'(?i)^warehouse.*$', r'(?i)^branch.*$', r'(?i)^region.*$',
            r'(?i)^site.*$', r'(?i)^facility.*$'
        ]
        
        # Common item identifier columns
        id_patterns = [
            r'(?i).*code.*', r'(?i).*id.*', r'(?i).*descr.*', 
            r'(?i).*item.*', r'(?i).*product.*', r'(?i).*sku.*'
        ]
        
        # Common total column patterns
        total_pattern = r'(?i)^total.*$'
        
        # Detect potential location columns and id columns
        loc_cols = []
        id_cols = []
        total_col = None
        
        for col in df.columns:
            col_str = str(col)
            # Check if it's a total column
            if re.match(total_pattern, col_str):
                total_col = col_str
                continue
                
            # Check if it's a location column
            is_loc = any(re.match(pattern, col_str) for pattern in loc_patterns)
            
            # Check if it's an ID column
            is_id = any(re.match(pattern, col_str) for pattern in id_patterns)
            
            if is_loc:
                loc_cols.append(col_str)
            elif is_id:
                id_cols.append(col_str)
            elif col_str not in ['Total', 'Sum', 'Grand Total']:
                # Default: treat as ID column unless it's a summary column
                id_cols.append(col_str)
        
        # If we have location columns and no value column name specified,
        # create a generic name based on column header context
        value_col = None
        if loc_cols:
            if total_col:
                # Use total column name to infer value name (e.g., "Total Inventory" -> "Inventory")
                value_match = re.match(r'(?i)Total\s+(.*)', total_col)
                if value_match:
                    value_col = value_match.group(1)
                else:
                    value_col = "Value"
            else:
                # Default
                value_col = "Value"
        
        return id_cols, loc_cols, value_col
    
    @staticmethod
    def transform_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform a dataframe from wide format (with location columns) to long format.
        Automatically detects the column structure.
        """
        # Skip if DataFrame is empty or has only 1-2 columns
        if df.empty or len(df.columns) < 3:
            return df
            
        # First, detect the column types
        id_cols, loc_cols, value_col = DataTransformer.detect_location_columns(df)
        
        # If no location columns detected, return original
        if not loc_cols:
            return df
            
        # If we have "_sheet" column (from original processing), preserve it
        sheet_col = None
        if '_sheet' in df.columns:
            sheet_col = df['_sheet'].copy()
            df = df.drop('_sheet', axis=1)
            id_cols = [c for c in id_cols if c != '_sheet']
        
        # Remove Total column if present
        total_pattern = r'(?i)^total.*$'
        total_cols = [col for col in df.columns if re.match(total_pattern, str(col))]
        if total_cols:
            df = df.drop(total_cols, axis=1)
        
        # Perform the melt operation
        result = pd.melt(
            df,
            id_vars=id_cols,
            value_vars=loc_cols,
            var_name='Locations',
            value_name=value_col or 'Inventory Units'
        )
        
        # Filter out rows with NaN or 0 values if desired
        result = result.dropna(subset=[value_col or 'Inventory Units'])
        result = result[result[value_col or 'Inventory Units'] != 0]
        
        # Add sheet column back if it existed
        if sheet_col is not None:
            # Need to repeat sheet values to match melted data
            # Each row in original becomes len(loc_cols) rows in result
            sheet_values = np.repeat(sheet_col.values, len(loc_cols))
            # Truncate to correct length in case of NA filtering
            sheet_values = sheet_values[:len(result)]
            result['_sheet'] = sheet_values
            
        return result
