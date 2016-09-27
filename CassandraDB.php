<?php

namespace Yee\Libraries\Database;

use Cassandra;

class CassandraDB
{

    /**
     * Static instance of self
     *
     * @var CassandraDB
     */
    protected static $_instance;
    protected $_cluster;
    protected $_session;
    protected $_keyspace;
    protected $isConnected = false;

    /**
     * Boolean - Toggle which defines if the select results will be converted
     * from Cassandra object to appropriate data type.
     *
     * Default: true
     *
     * @var boolean
     */
    public $autoConvert = true;

    /**
     * Uuid of the last insert
     * 
     * @var string
     */
    protected $_uuid;

    /**
     * The CQL query to be prepared and executed
     *
     * @var string
     */
    protected $_query;

    /**
     * An array that holds where conditions 'fieldname' => 'value'
     *
     * @var array
     */
    protected $_where = array();

    /**
     * An Array that holds all parameters which will be binded.
     *
     * @var array
     */
    protected $_argumentsToBind = array();

    /**
     * An Array that holds table information as 'column name' => 'column type'
     *
     * @var array 
     */
    protected $_table = array();

    /**
     * Array with all query options which will be used in the query itself
     *
     * @var array
     */
    protected $_queryOptions = array();

    /**
     * Database credentials
     *
     * @var string
     */
    protected $seeds;
    protected $username;
    protected $password;
    protected $port;
    protected $keyspace;

    /**
     * @param string $seed_nodes
     * @param string $cas_username
     * @param string $cas_pass
     * @param integer $port
     * @param string $keyspace
     */
    public function __construct( $seed_nodes, $cas_username, $cas_pass, $port, $keyspace )
    {
        $this->seeds = $seed_nodes;
        $this->username = $cas_username;
        $this->password = $cas_pass;
        $this->port = $port;
        $this->keyspace = $keyspace;

        self::$_instance = $this;
    }

    /**
     * A method to connect to the DatabaseManager
     */
    public function connect()
    {

        $this->_cluster = Cassandra::cluster()
                ->withContactPoints( implode( ',', $this->seeds ) );

        if ( $this->username != '' && $this->password != '' ) {
            $this->_cluster = $this->_cluster->withCredentials( $this->username, $this->password );
        }

        if ( $this->port != '' ) {
            $this->_cluster = $this->_cluster->withPort( $this->port );
        }

        $this->_cluster = $this->_cluster->build();

        $this->_session = $this->_cluster->connect( $this->keyspace );

        $this->_keyspace = $this->_session->schema()->keyspace( $this->keyspace );

        $this->isConnected = true;
    }

    /**
     * A method of returning the static instance to allow access to the
     * instantiated object from within another class.
     * Inheriting this class would require reloading connection info.
     *
     * @return object Returns the current instance.
     */
    public function getInstance()
    {
        return self::$_instance;
    }

    /**
     * Reset states after an execution
     *
     * @return object Returns the current instance.
     */
    protected function reset()
    {
        $this->_where = array();
        $this->_query = null;
        $this->_queryOptions = array();
        $this->_argumentsToBind = array();
        $this->_table = array();
    }

    public function uuid()
    {
        return $this->_uuid->uuid();
    }

    /**
     * Pass in a raw query and execute it.
     *
     * @param string $query      Contains a user-provided query.
     *
     * @return array | boolean   Contains the returned rows from the query OR the state of the executed query.
     */
    public function rawQuery( $query )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        try {
            $stmt = new Cassandra\SimpleStatement( $query );
            $result = $this->_session->execute( $stmt );
        } catch ( Cassandra\Exception $e ) {

            $this->_getErrors( $e, "RAW QUERY" );
            return false;
        }

        if ( $result->count() > 0 ) {
            return $this->_extractRows( $result );
        }

        return true;
    }

    /**
     * A convenient SELECT * function.
     *
     * @param string  $tableName The name of the database table to work with.
     * @param integer $numRows   The number of rows total to return.
     *
     * @return array Contains the returned rows from the select query.
     */
    public function get( $tableName, $numRows = null, $columns = '*', $itemsPerPage = 0 )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( empty( $columns ) ) {
            $columns = '*';
        }
        
        $this->_getTableInfo( $this->_keyspace->table( $tableName ) );

        $columns = is_array( $columns ) ? implode( ', ', $columns ) : $columns;
        $this->_query = "SELECT $columns FROM " . $tableName;

        $stmt = $this->_buildQuery( $numRows );

        //Check if query has failed
        if ( $stmt == false ) {
            return false;
        }

        $executionOptions = $this->_buildExecutionOptions( null, $itemsPerPage );

        try {
            $result = $this->_session->execute( $stmt, $executionOptions );
        } catch ( Exception $e ) {
            $this->_getErrors( $e, 'SELECT EXECUTION' );
            return false;
        }

        $this->reset();

        if ( $itemsPerPage > 0 ) {
            return $this->_extractRowsInPages( $result );
        }

        return $this->_extractRows( $result );
    }

    /**
     * A convenient SELECT * function to get one record.
     *
     * @param string  $tableName The name of the database table to work with.
     *
     * @return array Contains the returned rows from the select query.
     */
    public function getOne( $tableName, $columns = '*' )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        return $this->get( $tableName, 1, $columns, 0 );
    }

    /**
     *
     * @param <string $tableName The name of the table.
     * @param array $insertData Data containing information for inserting into the DB.
     *
     * @return boolean Boolean indicating whether the insert query was completed succesfully.
     */
    public function insert( $tableName, $insertData, $options = null )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        $this->_getTableInfo( $this->_keyspace->table( $tableName ) );

        $this->_prepareQueryOptions( $options );

        $this->_query = "INSERT INTO " . $tableName;

        $stmt = $this->_buildBindQuery( $insertData, true );

        //Check if query has failed
        if ( $stmt == false ) {
            return false;
        }

        $arguments = $this->_buildExecutionOptions( $this->_argumentsToBind );

        try {
            $result = $this->_session->execute( $stmt, $arguments );
        } catch ( Cassandra\Exception $e ) {
            $this->_getErrors( $e, "INSERT EXECUTION" );
            return false;
        }

        $this->reset();
        return true;
    }

    /**
     * Update query. Be sure to first call the "where" method.
     *
     * @param string $tableName The name of the database table to work with.
     * @param array  $tableData Array of data to update the desired row.
     *
     * @return boolean
     */
    public function update( $tableName, $tableData, $options = null )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        $this->_getTableInfo( $this->_keyspace->table( $tableName ) );

        $this->_prepareQueryOptions( $options );

        $this->_query = "UPDATE " . $tableName;

        $stmt = $this->_buildBindQuery( $tableData, false );

        //Check if query has failed
        if ( $stmt == false ) {
            return false;
        }

        $arguments = $this->_buildExecutionOptions( $this->_argumentsToBind );

        try {
            $result = $this->_session->execute( $stmt, $arguments );
        } catch ( Cassandra\Exception $e ) {
            $this->_getErrors( $e, "UPDATE EXECUTION" );
            return false;
        }

        $this->reset();
        return true;
    }

    /**
     * Delete query. Call the "where" method first.
     *
     * @param string  $tableName The name of the database table to work with.
     * @param integer $numRows   The number of rows to delete.
     *
     * @return boolean 
     */
    public function delete( $tableName, $columns = null, $options = null )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }
        
        $this->_getTableInfo( $this->_keyspace->table( $tableName ) );

        $columns = is_array( $columns ) ? implode( ', ', $columns ) : $columns;

        $this->_query = "DELETE $columns FROM " . $tableName;

        $this->_prepareQueryOptions( $options );

        $stmt = $this->_buildQuery( null );

        //Check if query has failed
        if ( $stmt == false ) {
            return false;
        }

        try {
            $this->_session->execute( $stmt );
        } catch ( Cassandra\Exception $e ) {
            $this->_getErrors( $e, "DELETE EXECUTION" );
            return false;
        }

        $this->reset();
        return true;
    }

    /**
     * This method allows you to specify multiple (method chaining optional) AND WHERE statements for CQL queries.
     *
     * @param string $whereProp  The name of the database field.
     * @param mixed  $whereValue The value of the database field.
     *
     * @return CassandraDb
     */
    public function where( $whereColumns, $whereValues, $operator = "=", $options = null )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        $this->_prepareQueryOptions( $options );

        $this->_where[] = Array( $whereColumns, $whereValues, $operator );

        return $this;
    }

    /**
     * Method which will build the query statement.
     * This method will create placeholders (?) for binding arguments.
     * Build INSERT and UPDATE clause.
     *
     * @param array $tableData Should contain an array of data for updating the database.
     *
     * @return object Returns Cassandra\PreparedStatement object.
     */
    protected function _buildBindQuery( $tableData = null, $isInsert )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( $isInsert == false ) {
            //Handle UPADTE option TTL or TIMESTAMP
            $this->_buildOptionTtlAndTimestamp( $isInsert );
        }

        $this->_buildTableData( $tableData, $isInsert );
        $this->_buildWhere();

        //Handle UPDATE option IF
        if ( $isInsert === false ) {
            $this->_buildOptionIf();
        }

        //Handle INSERT|UPDATE option IF EXISTS
        $this->_buildOptionExists( $isInsert );

        //Handle INSERT option TTL and TIMESTAMP
        if ( $isInsert === true ) {
            $this->_buildOptionTtlAndTimestamp( $isInsert );
        }

        //Handle WHERE option ALLOW FILTERING
        $this->_buildOptionAllowFiltering();

        $stmt = $this->_buildQueryStatement();

        return $stmt;
    }

    /**
     * Method which will build the query statement.
     * This method will NOT create any placeholders (?) for binding arguments.
     * Build WHERE and LIMIT clause.
     *
     * @param int   $numRows   The number of rows total to return.
     *
     * @return object Returns Cassandra\PreparedStatement object.
     */
    protected function _buildQuery( $numRows )
    {
        //Handle DELETE option TIMESTAMP
        $this->_buildOptionTtlAndTimestamp( false );

        $this->_buildWhere();
        $this->_buildLimit( $numRows );

        //Handle DELETE option IF
        $this->_buildOptionIf();

        //Handle DELETE option IF EXISTS
        $this->_buildOptionExists( false );

        //Handle WHERE option ALLOW FILTERING
        $this->_buildOptionAllowFiltering();

        return $this->_buildQueryStatement( true );
    }

    /**
     * Abstraction method that will build an INSERT or UPDATE part of the query
     */
    protected function _buildTableData( $tableData, $isInsert )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( $isInsert === true ) {
            $this->_query .= ' (' . implode( array_keys( $tableData ), ', ' ) . ')';
            $this->_query .= ' VALUES (';
        } else {
            $this->_query .= ' SET';
        }

        foreach ( $tableData as $column => $value ) {

            //Insert arguments for binding
            $this->_argumentsToBind[$column] = $value;

            if ( $isInsert === false ) {
                $this->_query .= " " . $column . " = ";
            }

            if ( !is_array( $value ) ) {
                $this->_query .= '?, ';
                continue;
            }

            $this->_query .= " {" . str_repeat( "?,", count( $value ) );
            $this->_query = rtrim( $this->_query, ',' );
            $this->_query .= '}, ';
        }
        $this->_query = rtrim( $this->_query, ', ' );

        if ( $isInsert === true )
            $this->_query .= ')';
    }

    /**
     * Abstraction method that will build the LIMIT part of the WHERE statement
     *
     * @param int   $numRows   The number of rows total to return.
     */
    protected function _buildLimit( $numRows )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( isset( $numRows ) ) {
            $this->_query .= ' LIMIT ' . (int) $numRows;
        }
    }

    /**
     * Abstraction method that will build the part of the WHERE conditions
     */
    protected function _buildWhere()
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( empty( $this->_where ) ) {
            return;
        }
        
        //Filter function variable
        $filter = function($v){
            return is_string($v)? "'$v'" : $v;
        };

        for ( $row = 0; $row < count( $this->_where ); $row++ ) {

            if ( $row == 0 ) {
                $this->_query .= " WHERE";
            } else {
                $this->_query .= " AND";
            }

            list ( $col, $val, $operator ) = $this->_where[$row];

            if ( is_array( $col ) ) {
                $this->_query .= " (" . implode( ", ", $col ) . ") ";
            } else {
                $this->_query .= " $col ";
            }

            $this->_query .= $operator;

            if ( is_array( $val ) ) {
                //check if the values are arrays
                if ( is_array( $val[0] ) ) {
                    //filter the values
                    for ( $i = 0; $i < count( $val ); $i++ ) {
                        $val[$i] = array_map($filter, $val[$i]);
                        $val[$i] = "(" . implode( ", ", $val[$i] ) . ")";
                    }
                }
                //Filter all $val
                $val = array_map( $filter, $val);
                $this->_query .= " (" . implode( ", ", $val ) . ") ";
            } else {
                if($this->_table[$col]->name() != 'uuid'){
                    $this->_query .= " " . ( is_string( $val ) ? "'$val'" : $val ) . " ";
                } else {
                    $this->_query .= " " .  $val . " ";
                }
            }
        }
    }

    /**
     * Builds the 'ALLOW FILTERING' option in the query
     */
    protected function _buildOptionAllowFiltering()
    {
        if ( array_key_exists( 'ALLOW FILTERING', $this->_queryOptions ) == false ) {
            return;
        }

        $this->_query .= " ALLOW FILTERING";
    }

    /**
     * Builds the 'IF NOT EXISTS' or 'IF EXISTS' option in the query
     * 
     * @param boolean $isInsert TRUE -> builds the proper option for insert
     */
    protected function _buildOptionExists( $isInsert )
    {
        if ( array_key_exists( 'IF EXISTS', $this->_queryOptions ) == false && array_key_exists( 'IF NOT EXISTS', $this->_queryOptions ) == false ) {
            return;
        }

        if ( $isInsert ) {
            $this->_query .= " IF NOT EXISTS";
        } else {
            $this->_query .= " IF EXISTS";
        }
    }

    /**
     * Builds the 'IF' ... 'AND' option in the query
     */
    protected function _buildOptionIf()
    {
        if ( array_key_exists( "IF", $this->_queryOptions ) == false ) {
            return;
        }

        $parameters = $this->_queryOptions['IF'];

        for ( $item = 0; $item < count( $parameters ); $item++ ) {
            if ( $item == 0 ) {
                $this->_query .= ' IF ' . key( $parameters[$item] ) . ' = ' . $parameters[$item][key( $parameters[$item] )];
            } else {
                $this->_query .= ' AND ' . key( $parameters[$item] ) . ' = ' . $parameters[$item][key( $parameters[$item] )];
            }
        }
    }

    protected function _buildOptionTtlAndTimestamp( $isInsert )
    {
        $hasOptionTTL = array_key_exists( 'TTL', $this->_queryOptions );
        $hasOptionTimestamp = array_key_exists( 'TIMESTAMP', $this->_queryOptions );
        if ( $hasOptionTTL == false && $hasOptionTimestamp == false ) {
            return;
        }

        if ( $isInsert == true ) {
            if ( $hasOptionTTL && $hasOptionTimestamp ) {
                $this->_query .= ' USING TTL ' . $this->_queryOptions['TTL'] . ' AND ' . $this->_queryOptions['TIMESTAMP'];
                return;
            }

            if ( $hasOptionTTL ) {
                $this->_query .= ' USING TTL ' . $this->_queryOptions['TTL'];
                return;
            }
            if ( $hasOptionTimestamp ) {
                $this->_query .= ' USING TIMESTAMP ' . $this->_queryOptions['TIMESTAMP'];
                return;
            }
        } else {
            if ( $hasOptionTTL == true ) {
                $this->_query .= ' USING TTL ' . $this->_queryOptions['TTL'];
            } else {
                $this->_query .= ' USING TIMESTAMP ' . $this->_queryOptions['TIMESTAMP'];
            }
        }
    }

    /**
     * Sorts and prepares all options which have been passed from the main query functions
     * 
     * @param string|array $options All options which have been passed from the query
     */
    protected function _prepareQueryOptions( $options )
    {
        if ( $options == null ) {
            return;
        }

        if ( is_array( $options ) ) {

            foreach ( $options as $option => $parameters ) {

                /* Handle options without parameters */
                if ( false === is_string( $option ) ) {
                    $this->_queryOptions[strtoupper( $parameters )] = null;
                    continue;
                }

                /* Handle options with single parameter */
                if ( false === is_array( $parameters ) ) {
                    $this->_queryOptions[strtoupper( $option )] = $parameters;
                    continue;
                }

                /* Handle options with multiple parameters */
                $fixedParameters = array();
                foreach ( $parameters as $param ) {
                    $col = key( $param );
                    $fixedParameters[] = [ $col => is_string( $param[$col] ) ? "'" . $param[$col] . "'" : $param[$col] ];
                }
                $this->_queryOptions[strtoupper( $option )] = $fixedParameters;
                unset( $fixedParameters );
            }
        } else {
            $this->_queryOptions[strtoupper( $options )] = null;
        }
    }

    /**
     * Method creates a Cassandra Statement object from the CQL query
     * 
     * @param boolean $simpleStatement TRUE -> The method will return Cassandra\SimpleStatement
     *
     * @return object   Cassandra\PreparedStatement | Cassandra\SimpleStatement
     */
    protected function _buildQueryStatement( $simpleStatement = false )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        try {
            if ( $simpleStatement ) {
                return new Cassandra\SimpleStatement( $this->_query );
            }
            return $this->_session->prepare( $this->_query );
        } catch ( Cassandra\Exception $e ) {
            $this->_getErrors( $e, "BUILD QUERY STATEMENT" );
            return false;
        }
    }

    /**
     * Creates a Cassandra Execution Options object.
     * 
     * @param array Arguments which will be prepared for binding.
     * @param int   Number of items which will be dislayed in a single page.
     *
     * @return object Cassandra ExecutionOptions
     */
    protected function _buildExecutionOptions( $arguments = array(), $itemsPerPage = 0 )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        //array holding the execution options
        $executionOptions = array();

        if ( !empty( $arguments ) ) {
            $executionOptions['arguments'] = $this->_convertArgumentsToCassandra( $arguments );
        }

        if ( $itemsPerPage > 0 ) {
            $executionOptions['page_size'] = $itemsPerPage;
        }

        try {
            return new Cassandra\ExecutionOptions( $executionOptions );
        } catch ( Cassandra\Exception $e ) {
            $this->_getErrors( $e, "BUILD EXECUTION OPTIONS" );
            return false;
        }
    }

    /**
     * Extracts all rows in pages from the executed Cassandra statement
     *
     * @param object Cassandra executed statement
     *
     * @return array|null All rows in pages from the executed statement OR 'null' if there are no results.
     */
    protected function _extractRowsInPages( $executionResult )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( $executionResult->count() == 0 ) {
            return null;
        }

        $convertedResults = array();

        //Get column names
        $cols = array_keys( $executionResult[0] );

        //Current page
        $page = 0;

        while ( true ) {

            $convertedResults[$page] = array();

            for ( $row = 0; $row < $executionResult->count(); $row++ ) {

                array_push( $convertedResults[$page], $executionResult[$row] );

                if ( $this->autoConvert ) {

                    foreach ( $cols as $col ) {

                        if ( gettype( $convertedResults[$page][$row][$col] ) == 'object' ) {
                            $convertedResults[$page][$row][$col] = $this->_convertFromCassandraObject( $convertedResults[$page][$row][$col] );
                        }
                    }
                }
            }

            if ( $executionResult->isLastPage() ) {
                break;
            }

            $executionResult = $executionResult->nextPage();
            $page++;
        }

        return $convertedResults;
    }

    /**
     * Extracts all rows from the executed Cassandra statement
     *
     * @param object Cassandra executed statement
     *
     * @return array|null All rows from the executed statement OR 'null' if there are no results.
     */
    protected function _extractRows( $executionResult )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( $executionResult->count() == 0 ) {
            return null;
        }

        $convertedResults = array();

        //Get column names
        $columnNames = array_keys( $executionResult[0] );

        for ( $row = 0; $row < $executionResult->count(); $row++ ) {

            array_push( $convertedResults, $executionResult[$row] );
            //Default set to true
            if ( $this->autoConvert ) {

                foreach ( $columnNames as $col ) {

                    if ( gettype( $convertedResults[$row][$col] ) == 'object' ) {
                        $convertedResults[$row][$col] = $this->_convertFromCassandraObject( $convertedResults[$row][$col] );
                    }
                }
            }
        }

        return $convertedResults;
    }

    /**
     * Converts from cassandra object to appropriate data format
     *
     * @param Cassandra Object
     *
     * @return Appropriate data format
     */
    protected function _convertFromCassandraObject( $col )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        switch ( $col->type() ) {
            case 'timestamp':
                return $col->toDateTime();
            case 'bigint':
                return $col->toInt();
            case 'decimal':
            case 'float':
                return $col->toDouble();
            case 'varint':
                return $col->value();
            case 'blob':
                return $col->bytes();
            case 'uuid':
            case 'timeuuid':
                return $col->uuid();
            case 'inet':
                return $col->address();
        }
    }

    /**
     * Gets the table information from the database
     * 'column name' => 'column type'
     *
     * @param object Cassandra\DefaultTable
     */
    protected function _getTableInfo( $table )
    {
        foreach ( $table->columns() as $column ) {
            $this->_table[$column->name()] = $column->type();
        }
    }

    /**
     * Loops through all parameters which will be converted
     * to appropriate Cassandra object type
     *
     * @param array     Arguments prior conversion
     *
     * @return array    Arguments after conversion to Cassandra Objects
     */
    protected function _convertArgumentsToCassandra( $arguments )
    {
        foreach ( array_keys( $arguments ) as $argumentColumn ) {
            $arguments[$argumentColumn] = $this->_convertToCassandraObject( $arguments[$argumentColumn], $argumentColumn );
        }
        return $arguments;
    }

    /**
     * Converts an argument value from the arguments passes through the query
     * to the appropriate Cassandra object type based on the type of the column in the table.
     *
     * Called upon insert, update query
     *
     * @param object Value of the argument from the query
     * @param string Column name of the argument
     *
     * @return object New cassandra object OR the value itself
     */
    protected function _convertToCassandraObject( $paramValue, $paramKey )
    {

        switch ( $this->_table[$paramKey] ) {
            case 'timestamp':
                return new Cassandra\Timestamp( strtotime( $paramValue ) );
            case 'bigint':
                return new Cassandra\Bigint( $paramValue );
            case 'decimal':
                return new Cassandra\Decimal( strval( $paramValue ) );
            case 'float':
                return new Cassandra\Float( strval( $paramValue ) );
            case 'varint':
                return new Cassandra\Varint( $paramValue );
            case 'blob':
                return new Cassandra\Blob( $paramValue );
            case 'uuid':
                if ( $paramValue != null ) {
                    $this->_uuid = new Cassandra\Uuid( $paramValue );
                } else {
                    $this->_uuid = new Cassandra\Uuid();
                }
                return $this->_uuid;
            case 'timeuuid':
                if ( $paramValue != null ) {
                    $this->_uuid = new Cassandra\Timeuuid( $paramValue );
                } else {
                    $this->_uuid = new Cassandra\Timeuuid();
                }
                return $this->_uuid;
            case 'inet':
                return new Cassandra\Inet( $paramValue );
            default:
                return $paramValue;
        }
    }

    /**
     * Function which prints the errors if one occurs
     * 
     * @param type $e   Cassandra Exception
     * @param type $from Where the error is coming from
     */
    protected function _getErrors( $e, $from )
    {
        echo "<p style='font-size: 16pt'><strong>ERROR - $from</strong>: " . $e->getMessage() . "</p>";
        echo "<h4>Error occured in CassandraDB line: " . $e->getLine() . "</h4> ";
        echo "<h1>Error backtrace from CassandraDB: </h1>";
        echo implode( "<br>", explode( "#", $e->getTraceAsString() ) );
    }
}