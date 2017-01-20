#
from bq_dbapi import BigQueryConnection
apilevel = "2.0"
threadsafety = 2
paramstyle = 'pyformat'
import sys, traceback

def connect(user=None, host='localhost', unix_sock=None, port=5432, database=None,
            password=None, ssl=False, timeout=None,
            application_name=None, **kwargs):
    print "connect host: {}, port: {}, user: {}, password: {}, database:{}, ssl: {}, \
        application_name:{} ".format(host,port,user,password, database, ssl, application_name)
    conn = BigQueryConnection(host)
    #traceback.print_stack()
    return conn




class Warning(Exception):
    """Generic exception raised for important database warnings like data
    truncations.  This exception is not currently used by pg8000.
    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    def __init__(self):
        print "Warning"
    pass


class Error(Exception):
    """Generic exception that is the base exception of all other error
    exceptions.
    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self):
        print "Error"
    pass


class InterfaceError(Error):
    """Generic exception raised for errors that are related to the database
    interface rather than the database itself.  For example, if the interface
    attempts to use an SSL connection but the server refuses, an InterfaceError
    will be raised.
    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self):
        print "InterfaceError"
    pass


class DatabaseError(Error):
    """Generic exception raised for errors that are related to the database.
    This exception is currently never raised by pg8000.
    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self):
        print "DatabaseError"
    pass


class DataError(DatabaseError):
    """Generic exception raised for errors that are due to problems with the
    processed data.  This exception is not currently raised by pg8000.
    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self):
        print "DataError"
    pass


class OperationalError(DatabaseError):
    """
    Generic exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer. This
    exception is currently never raised by pg8000.
    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self):
        print "OperationalError"
    pass


class IntegrityError(DatabaseError):
    """
    Generic exception raised when the relational integrity of the database is
    affected.  This exception is not currently raised by pg8000.
    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self):
        print "IntegrityError"
    pass


class InternalError(DatabaseError):
    """Generic exception raised when the database encounters an internal error.
    This is currently only raised when unexpected state occurs in the pg8000
    interface itself, and is typically the result of a interface bug.
    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self):
        print "InternalError"
    pass


class ProgrammingError(DatabaseError):
    """Generic exception raised for programming errors.  For example, this
    exception is raised if more parameter fields are in a query string than
    there are available parameters.
    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self):
        print "ProgrammingError"
    pass


class NotSupportedError(DatabaseError):
    """Generic exception raised in case a method or database API was used which
    is not supported by the database.
    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self):
        print "NotSupportedError"
    pass


class ArrayContentNotSupportedError(NotSupportedError):
    """
    Raised when attempting to transmit an array where the base type is not
    supported for binary data transfer by the interface.
    """

    def __init__(self):
        print "ArrayContentNotSupportedError"
    pass


class ArrayContentNotHomogenousError(ProgrammingError):
    """
    Raised when attempting to transmit an array that doesn't contain only a
    single type of object.
    """

    def __init__(self):
        print "ArrayContentNotHomogenousError"
    pass


class ArrayContentEmptyError(ProgrammingError):
    """Raised when attempting to transmit an empty array. The type oid of an
    empty array cannot be determined, and so sending them is not permitted.
    """

    def __init__(self):
        print "ArrayContentEmptyError"
    pass


class ArrayDimensionsNotConsistentError(ProgrammingError):
    """
    Raised when attempting to transmit an array that has inconsistent
    multi-dimension sizes.
    """

    def __init__(self):
        print "ArrayDimentionNotConsistentError"
    pass