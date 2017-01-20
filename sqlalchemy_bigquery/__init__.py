__version__ = '0.0.8'

from sqlalchemy.dialects import registry

registry.register("bigquery", "sqlalchemy_bigquery.base", "BigQueryDialect")
