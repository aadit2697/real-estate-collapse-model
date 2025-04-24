#### 5. src/utils/snowflake_connector.py

import yaml
import snowflake.connector 
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import os

class SnowflakeConnector:
    def __init__(self, config_path='config/snowflake_config.yaml'):
        self._conn = None
        self._engine = None
        self.config = self._load_config(config_path)

    def _load_config(self, config_path):
        """Load Snowflake configuration from YAML file"""
        if os.path.exists(config_path):
            with open(config_path) as f:
                return yaml.safe_load(f)['snowflake']
        else:
            # Fallback to environment variables if config file doesn't exist
            return {
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': os.getenv('SNOWFLAKE_PASSWORD'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': os.getenv('SNOWFLAKE_DATABASE'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
                'role': os.getenv('SNOWFLAKE_ROLE')
            }
        def _load_config(self, config_path):
            if os.path.exists(config_path):
                with open(config_path) as f:
                    config_data = yaml.safe_load(f)
                    print("✅ Loaded config:", config_data)  # ADD THIS LINE
                    return config_data['snowflake']

    def get_connection(self):
        """Get Snowflake connection using snowflake-connector-python"""
        if not self._conn:
            self._conn = snowflake.connector.connect(
                account=self.config['account'],
                user=self.config['user'],
                password=self.config['password'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema'],
                role=self.config['role']
            )
        return self._conn

    def get_engine(self):
        """Get SQLAlchemy engine for Snowflake"""
        if not self._engine:
            self._engine = create_engine(URL(
                account=self.config['account'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                schema=self.config['schema'],
                warehouse=self.config['warehouse'],
                role=self.config['role']
            ))
        return self._engine

    def execute_query(self, query, params=None):
        """Execute a SQL query and return results"""
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(query, params or {})
            return cur.fetchall()
        finally:
            cur.close()

    def close(self):
        """Close all connections"""
        if self._conn:
            self._conn.close()
            self._conn = None
        if self._engine:
            self._engine.dispose()
            self._engine = None