import random
from typing import List, Tuple, Dict, Any
from faker import Faker
from database.connection_manager import ConnectionManager


class DataGenerator:
    """Generate sample data for database tables"""
    
    def __init__(self, connection_manager: ConnectionManager):
        self.conn_manager = connection_manager
        self.fake = Faker()
    
    def generate_sample_data(self, db_path: str, table_name: str, num_rows: int = 10) -> str:
        """Generate and insert sample data into a table based on column types"""
        try:
            conn = self.conn_manager.get_connection(db_path)
            cursor = conn.cursor()
            
            # Get table schema
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            if not columns:
                return f"Table '{table_name}' not found."
            
            # Generate data based on column types
            insert_data = []
            
            for _ in range(num_rows):
                row_data = []
                for col in columns:
                    col_dict = dict(col)
                    column_name = col_dict["name"]
                    column_type = col_dict["type"].upper()
                    is_pk = bool(col_dict["pk"])
                    
                    # Generate data based on type
                    if is_pk and 'INTEGER' in column_type:
                        # Skip auto-increment primary keys
                        continue
                    else:
                        value = self._generate_value_by_type(column_name, column_type)
                        row_data.append(value)
                
                insert_data.append(tuple(row_data))
            
            # Filter out auto-increment columns from column names
            filtered_columns = []
            for col in columns:
                col_dict = dict(col)
                is_pk = bool(col_dict["pk"])
                column_type = col_dict["type"].upper()
                if not (is_pk and 'INTEGER' in column_type):
                    filtered_columns.append(col_dict["name"])
            
            # Insert data
            placeholders = ','.join(['?' for _ in filtered_columns])
            insert_query = f"INSERT INTO {table_name} ({','.join(filtered_columns)}) VALUES ({placeholders})"
            
            cursor.executemany(insert_query, insert_data)
            conn.commit()
            
            return f"Successfully generated and inserted {num_rows} rows into {table_name}"
        except Exception as e:
            return f"Error generating data: {str(e)}"
    
    def _generate_value_by_type(self, column_name: str, column_type: str) -> Any:
        """Generate a value based on column name and type"""
        column_name_lower = column_name.lower()
        
        if 'INT' in column_type:
            return random.randint(1, 1000)
        elif 'REAL' in column_type or 'FLOAT' in column_type or 'DOUBLE' in column_type:
            return round(random.uniform(1.0, 1000.0), 2)
        elif 'TEXT' in column_type or 'VARCHAR' in column_type or 'CHAR' in column_type:
            return self._generate_text_value(column_name_lower)
        elif 'DATE' in column_type or 'DATETIME' in column_type:
            return self.fake.date_time().isoformat()
        elif 'BOOL' in column_type:
            return random.choice([0, 1])
        else:
            return self.fake.text(max_nb_chars=20)
    
    def _generate_text_value(self, column_name: str) -> str:
        """Generate text value based on column name patterns"""
        if 'name' in column_name:
            return self.fake.name()
        elif 'email' in column_name:
            return self.fake.email()
        elif 'phone' in column_name:
            return self.fake.phone_number()
        elif 'address' in column_name:
            return self.fake.address()
        elif 'company' in column_name:
            return self.fake.company()
        elif 'city' in column_name:
            return self.fake.city()
        elif 'country' in column_name:
            return self.fake.country()
        elif 'title' in column_name:
            return self.fake.job()
        elif 'description' in column_name:
            return self.fake.text(max_nb_chars=100)
        else:
            return self.fake.text(max_nb_chars=50)
    
    def get_table_schema_for_generation(self, db_path: str, table_name: str) -> List[Dict[str, Any]]:
        """Get table schema information for data generation"""
        try:
            conn = self.conn_manager.get_connection(db_path)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            schema = []
            for col in columns:
                col_dict = dict(col)
                schema.append({
                    "name": col_dict["name"],
                    "type": col_dict["type"],
                    "nullable": not col_dict["notnull"],
                    "default": col_dict["dflt_value"],
                    "primary_key": bool(col_dict["pk"])
                })
            return schema
        except Exception:
            return []