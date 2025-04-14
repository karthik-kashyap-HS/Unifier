import magic
from pathlib import Path

class FileValidator:
    def __init__(self):
        # Initialize magic with mime type detection
        self.mime = magic.Magic(mime=True)
        
        # MIME type to our internal type mapping
        self.mime_map = {
            'text/csv': 'csv',
            'application/vnd.ms-excel': 'excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'excel',
            'application/pdf': 'pdf'
        }
        
        # Fallback extension mapping
        self.ext_map = {
            '.csv': 'csv',
            '.xls': 'excel',
            '.xlsx': 'excel',
            '.pdf': 'pdf'
        }
    
    def validate_file(self, file_path: Path) -> str:
        """Detect file type using magic bytes with extension fallback"""
        try:
            # First try magic bytes
            mime_type = self.mime.from_file(file_path)
            if mime_type in self.mime_map:
                return self.mime_map[mime_type]
        except Exception as e:
            pass
        
        # Fallback to file extension
        ext = file_path.suffix.lower()
        return self.ext_map.get(ext)