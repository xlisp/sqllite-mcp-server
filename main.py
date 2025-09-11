from typing import List, Optional
from mcp.server.fastmcp import FastMCP

from database.connection_manager import ConnectionManager
from database.operations import DatabaseOperations
from lineage.field_tracker import FieldTracker
from utils.import_export import ImportExportUtils
from utils.data_generator import DataGenerator


# Initialize FastMCP server
mcp = FastMCP("sqlite-db")

# Initialize components
connection_manager = ConnectionManager()
db_operations = DatabaseOperations(connection_manager)
field_tracker = FieldTracker()
import_export = ImportExportUtils(connection_manager)
data_generator = DataGenerator(connection_manager)


@mcp.tool()
async def connect_database(db_path: str) -> str:
    """Connect to a SQLite database file.
    
    Args:
        db_path: Path to the SQLite database file
    """
    return db_operations.connect_database(db_path)


@mcp.tool()
async def execute_query(db_path: str, query: str, params: Optional[List] = None) -> str:
    """Execute a SQL query on the database.
    
    Args:
        db_path: Path to the SQLite database file
        query: SQL query to execute
        params: Optional parameters for the query
    """
    return db_operations.execute_query(db_path, query, params)


@mcp.tool()
async def describe_table(db_path: str, table_name: str) -> str:
    """Get detailed information about a table structure.
    
    Args:
        db_path: Path to the SQLite database file
        table_name: Name of the table to describe
    """
    return db_operations.describe_table(db_path, table_name)


@mcp.tool()
async def add_field_lineage(target_table: str, target_field: str, 
                          source_tables: List[str], source_fields: List[str], 
                          join_condition: str = "") -> str:
    """Add field lineage information to track data sources.
    
    Args:
        target_table: Target table name
        target_field: Target field name
        source_tables: List of source table names
        source_fields: List of source field names
        join_condition: Optional join condition description
    """
    try:
        field_tracker.add_lineage(
            target_table, target_field, 
            source_tables, source_fields, 
            join_condition
        )
        return f"Lineage added for {target_table}.{target_field} from {', '.join([f'{t}.{f}' for t, f in zip(source_tables, source_fields)])}"
    except Exception as e:
        return f"Error adding lineage: {str(e)}"


@mcp.tool()
async def trace_field_lineage(table: str, field: str) -> str:
    """Trace the lineage of a specific field to understand its data sources.
    
    Args:
        table: Table name
        field: Field name to trace
    """
    try:
        lineage = field_tracker.get_lineage(table, field)
        
        if not lineage:
            return f"No lineage information found for {table}.{field}"
        
        result = f"Field Lineage for {table}.{field}:\n"
        result += f"Source Tables: {', '.join(lineage['source_tables'])}\n"
        result += f"Source Fields: {', '.join(lineage['source_fields'])}\n"
        
        if lineage['join_condition']:
            result += f"Join Condition: {lineage['join_condition']}\n"
        
        # Try to provide sample data mapping
        result += f"\nData Flow:\n"
        for i, (src_table, src_field) in enumerate(zip(lineage['source_tables'], lineage['source_fields'])):
            result += f"  {src_table}.{src_field} -> {table}.{field}\n"
        
        return result
    except Exception as e:
        return f"Error tracing lineage: {str(e)}"


@mcp.tool()
async def analyze_query_lineage(query: str) -> str:
    """Analyze a SQL query to identify potential field lineage.
    
    Args:
        query: SQL query to analyze
    """
    try:
        analysis = field_tracker.analyze_query_lineage(query)
        
        result = "Query Analysis:\n"
        result += f"Tables involved: {', '.join(analysis['tables'])}\n"
        result += f"Fields selected: {', '.join(analysis['fields'])}\n"
        
        # Check if we have lineage information for any of the fields
        lineage_found = []
        for table in analysis['tables']:
            for field in analysis['fields']:
                if field != '*':  # Skip wildcard
                    clean_field = field.split('.')[-1].strip()  # Get field name without table prefix
                    lineage = field_tracker.get_lineage(table, clean_field)
                    if lineage:
                        lineage_found.append(f"{table}.{clean_field}")
        
        if lineage_found:
            result += f"\nFields with tracked lineage: {', '.join(lineage_found)}"
        else:
            result += "\nNo tracked lineage found for fields in this query."
        
        return result
    except Exception as e:
        return f"Error analyzing query: {str(e)}"


@mcp.tool()
async def generate_sample_data(db_path: str, table_name: str, num_rows: int = 10) -> str:
    """Generate and insert sample data into a table based on column types.
    
    Args:
        db_path: Path to the SQLite database file
        table_name: Name of the table to populate
        num_rows: Number of rows to generate (default: 10)
    """
    return data_generator.generate_sample_data(db_path, table_name, num_rows)


@mcp.tool()
async def import_csv(db_path: str, csv_path: str, table_name: str, create_table: bool = True) -> str:
    """Import data from a CSV file into a SQLite table.
    
    Args:
        db_path: Path to the SQLite database file
        csv_path: Path to the CSV file
        table_name: Name of the target table
        create_table: Whether to create the table if it doesn't exist
    """
    return import_export.import_csv(db_path, csv_path, table_name, create_table)


@mcp.tool()
async def export_table_to_csv(db_path: str, table_name: str, output_path: str) -> str:
    """Export a table to a CSV file.
    
    Args:
        db_path: Path to the SQLite database file
        table_name: Name of the table to export
        output_path: Path for the output CSV file
    """
    return import_export.export_table_to_csv(db_path, table_name, output_path)


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')