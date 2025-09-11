import re
from typing import Dict, List, Optional


class FieldTracker:
    """Track field lineage and data sources"""
    
    def __init__(self):
        self.lineage_db: Dict[str, Dict] = {}
    
    def add_lineage(self, target_table: str, target_field: str, 
                   source_tables: List[str], source_fields: List[str], 
                   join_condition: str = "") -> None:
        """Add field lineage information"""
        key = f"{target_table}.{target_field}"
        self.lineage_db[key] = {
            "source_tables": source_tables,
            "source_fields": source_fields,
            "join_condition": join_condition,
            "target_table": target_table,
            "target_field": target_field
        }
    
    def get_lineage(self, table: str, field: str) -> Optional[Dict]:
        """Get lineage information for a field"""
        key = f"{table}.{field}"
        return self.lineage_db.get(key)
    
    def analyze_query_lineage(self, query: str) -> Dict[str, List[str]]:
        """Analyze a query to extract potential lineage"""
        # Simple regex-based analysis (can be enhanced with proper SQL parser)
        tables = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)', query.upper())
        table_list = [t[0] or t[1] for t in tables]
        
        fields = re.findall(r'SELECT\s+(.*?)\s+FROM', query.upper(), re.DOTALL)
        if fields:
            field_list = [f.strip() for f in fields[0].split(',')]
        else:
            field_list = []
        
        return {
            "tables": table_list,
            "fields": field_list
        }
    
    def get_all_lineage(self) -> Dict[str, Dict]:
        """Get all lineage information"""
        return self.lineage_db.copy()
    
    def clear_lineage(self) -> None:
        """Clear all lineage information"""
        self.lineage_db.clear()