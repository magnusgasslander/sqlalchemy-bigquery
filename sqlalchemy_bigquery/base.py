"""
Support for Google BigQuery.

Does not support actually connecting to BQ.
Directly derived from the mssql dialect with minor modifications

"""

from sqlalchemy.sql import compiler
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql import sqltypes
from sqlalchemy.engine import default
from sqlalchemy.types import TypeEngine


import sqlalchemy.dialects.mssql.base as mssql_base

from sqlalchemy.dialects.mssql.base import MSDialect

class BQSQLCompiler(SQLCompiler):

    def visit_column(self, column, add_to_result_map=None, **kwargs):
        # TODO: figure out how to do this immutably
        # force column rendering to not use quotes by declaring every col literal
        column.is_literal = True
        return super(BQSQLCompiler, self).visit_column(
            column,
            add_to_result_map=add_to_result_map,
            include_table = False,
            **kwargs
        )


    def visit_match_op_binary(self, binary, operator, **kw):
        return "%s CONTAINS %s" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw))


class BQString(sqltypes.String):
    def __init__(
            self,
            length=None,
            collation=None,
            convert_unicode=False,
            unicode_error=None,
            _warn_on_bytestring=False
    ):
        return super(BQString, self).__init__(
            length=length,
            collation=collation,
            convert_unicode=convert_unicode,
            unicode_error=unicode_error,
            _warn_on_bytestring=_warn_on_bytestring
        )

    def literal_processor(self, dialect):
        def process(value):
            value = value.replace("'", "\\'")
            return "'%s'" % value
        return process

class BQIdentifierPreparer(compiler.IdentifierPreparer):
    def __init__(self, dialect):
        super(BQIdentifierPreparer, self).__init__(
            dialect,
            initial_quote='[',
            final_quote=']',
        )

    def format_label(self, label, name=None):
        """ bq can't handle quoting labels and not labels with period """
        return name.replace(".", "_") or label.name.replace(".", "_")

    def quote_schema(self, schema, force=None):
        return schema

    def quote(self, ident, force=None):
        return ident


class BigQueryDialect(default.DefaultDialect):

    def __init__(self, **kwargs):
        super(BigQueryDialect, self).__init__(**kwargs)
    print "Initializing Bigquery dialect"
    name = 'bigquery'
    statement_compiler = BQSQLCompiler
    preparer = BQIdentifierPreparer
    #colspecs = {
    #sqltypes.String: BQString
    #}
    legacy_schema_aliasing = False

    @classmethod
    def dbapi(cls):
        from gcp import bq_dbapi as api
        return api

    def _check_unicode_returns(self, connection, additional_tests=None):
        return 'conditional'

    def _check_unicode_description(self, connection):
        return True

    def do_rollback(self, dbapi_connection):
        pass

    def get_table_names(self, connection, schema=None, **kw):
        """Return a list of table names for `schema`."""
        print "BigQueryDialect::get_table_names conn: {} schema: {}".format(connection, schema)
        #return connection.get_table_names(schema, **kw)
        return ["babynames.names2010"]


    def get_columns(self, connection, table_name, schema=None, **kw):
        """Return information about columns in `table_name`.

        Given a :class:`.Connection`, a string
        `table_name`, and an optional string `schema`, return column
        information as a list of dictionaries with these keys:

        name
          the column's name

        type
          [sqlalchemy.types#TypeEngine]

        nullable
          boolean

        default
          the column's default value

        autoincrement
          boolean

        sequence
          a dictionary of the form
              {'name' : str, 'start' :int, 'increment': int, 'minvalue': int,
               'maxvalue': int, 'nominvalue': bool, 'nomaxvalue': bool,
               'cycle': bool}

        Additional column attributes may be present.
        """
        print "BigQueryDialect::get_columns conn: {} table_name: {} schema: {}".format(connection, table_name, schema)
        return [{'name':'name', 'type': STRING, 'nullable':True, 'default':'not_set', 'autoincrement':False}]

supports_native_boolean = True

dialect = BigQueryDialect
