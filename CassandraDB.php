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
    protected $_bindParams = array();

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
     * An Array that holds table information as 'column name' => 'column type'
     *
     * @var array 
     */
    protected $_table = array();

    /**
     * String that holds the name of the table which is used
     *
     * @var string Name of the table
     */
    protected $_tableName;

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
     * @param integer $port
     * @param string $cas_username
     * @param string $cas_pass
     * @param string $keyspace
     */
    public function __construct($seed_nodes, $cas_username, $cas_pass, $port, $keyspace)
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
     *
     */
    public function connect()
    {

        $this->_cluster = Cassandra::cluster()
                ->withContactPoints(implode(',', $this->seeds));

        if ($this->username != '' && $this->password != '') {
            $this->_cluster = $this->_cluster->withCredentials($this->username, $this->password);
        }

        if ($this->port != '') {
            $this->_cluster = $this->_cluster->withPort($this->port);
        }

        $this->_cluster = $this->_cluster->build();

        $this->_session = $this->_cluster->connect($this->keyspace);

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
        $this->_groupBy = array();
        $this->_query = null;
        $this->_bindParams = array();
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

            echo $e->getMessage();
            return false;
        }

        if ( $result->count() > 0 ) {
            $arrayResults = array();
            foreach ( $result as $row ) {
                array_push( $arrayResults, $row );
            }
            return $arrayResults;
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
    public function get( $tableName, $numRows = null, $columns = '*', $options = 0 )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( empty( $columns ))
            $columns = '*';

        if ( empty( $this->_tableName ) || $this->_tableName != $tableName ) {
                $this->_tableName = $tableName;
                $this->_initTable( $this->_keyspace->table( $tableName ) );
        }

        $column = is_array( $columns ) ? implode( ', ', $columns ) : $columns;
        $this->_query = "SELECT $column FROM " . $tableName;

        $stmt = $this->_buildQuery( $numRows );

        if ( $stmt == false ) {
            return false;
        }

        $arguments = $this->_buildExecutionOptions( $this->_bindParams, $options );

        try {

            $result = $this->_session->execute( $stmt, $arguments );
            
        } catch ( Exception $e ) {

            echo $e->getMessage();
            return false;
        }


        $this->reset();

        return $this->_extractResult( $result, $options > 0 );
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

        $res = $this->get( $tableName, 1, $columns );

        if ( is_object( $res ))
            return $res;

        if ( isset( $res[0] ))
            return $res[0];

        return null;
    }

    /**
     *
     * @param <string $tableName The name of the table.
     * @param array $insertData Data containing information for inserting into the DB.
     *
     * @return boolean Boolean indicating whether the insert query was completed succesfully.
     */
    public function insert( $tableName, $insertData )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }
        var_dump( $this->_tableName );
        if ( empty( $this->_tableName ) || $this->_tableName != $tableName ) {
            $this->_tableName = $tableName;
            $this->_initTable( $this->_keyspace->table( $tableName ) );
        }

        $this->_query = "INSERT INTO " . $tableName;

        $stmt = $this->_buildQuery( null, $insertData );

        if ( $stmt == false ) {
            return false;
        }

        $arguments = $this->_buildExecutionOptions( $insertData );

        try {

            $result = $this->_session->execute( $stmt, $arguments );
        } catch ( Cassandra\Exception $e ) {
            echo $e->getMessage();
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
    public function update( $tableName, $tableData )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( empty( $this->_tableName ) || $this->_tableName != $tableName ) {
            $this->_tableName = $tableName;
            $this->_initTable( $this->_keyspace->table( $tableName ) );
        }

        $this->_query = "UPDATE " . $tableName . " SET ";

        $stmt = $this->_buildQuery( null, $tableData );

        if ( $stmt == false ) {
            return false;
        }

        foreach ( $tableData as $key => $value ) {
            $this->_bindParams[$key] = $value;
        }

        $arguments = $this->_buildExecutionOptions( $this->_bindParams );

        try {

            $result = $this->_session->execute( $stmt, $arguments );
        } catch ( Cassandra\Exception $e ) {
            echo $e->getMessage();
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
    public function delete( $tableName, $numRows = null )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( empty( $this->_tableName ) || $this->_tableName != $tableName ) {
            $this->_tableName = $tableName;
            $this->_initTable( $this->_keyspace->table( $tableName ) );
        }

        $this->_query = "DELETE FROM " . $tableName;

        $stmt = $this->_buildQuery( $numRows );

        if ( $stmt == false ) {
            return false;
        }

        $arguments = $this->_buildExecutionOptions( $this->_bindParams );

        try {

            $this->_session->execute( $stmt, $arguments );
        } catch ( Cassandra\Exception $e ) {
            echo $e->getMessage();
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
    public function where( $whereProp, $whereValue = null, $operator = null )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        $this->_bindParams[$whereProp] = $whereValue;

        if ( $operator ) {
            $whereValue = Array( $operator => $whereValue );

        $this->_where[] = Array( "AND", $whereValue, $whereProp );

        return $this;
    }

    /**
     * Creates Cassandra bind on parameters
     *
     * @param array
     *
     * @return object 
     */
    protected function _buildBindParams( $items )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        try {

            return new Cassandra\ExecutionOptions( array( 'arguments' => $items ));
        } catch ( Cassandra\Exception $e ) {

            echo $e->getMessage();
            return false;
        }
    }

    /**
     * Helper function to add variables into bind parameters array and will return
     * its CQL part of the query according to operator in ' $operator ?'
     *
     * @param Array Variable with values
     */
    protected function _buildPair($operator, $value)
    {
        if (!$this->isConnected) {
            $this->connect();
        }

        if (!is_object($value)) {
            return ' ' . $operator . ' ? ';
        }

        return " " . $operator . " (" . $subQuery['query'] . ")";
    }

    /**
     * Abstraction method that will compile the WHERE statement,
     * any passed update data, and the desired rows.
     * It then builds the CQL query.
     *
     * @param int   $numRows   The number of rows total to return.
     * @param array $tableData Should contain an array of data for updating the database.
     *
     * @return cassandra_stmt Returns the $stmt object.
     */
    protected function _buildQuery( $numRows = null, $tableData = null )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        $this->_buildTableData( $tableData );
        $this->_buildWhere();
        $this->_buildLimit( $numRows );

        // Prepare query
        $stmt = $this->_prepareQuery();

        return $stmt;
    }

    /**
     * Abstraction method that will build an INSERT or UPDATE part of the query
     */
    protected function _buildTableData( $tableData )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( !is_array( $tableData ))
            return;

        $isInsert = strpos( $this->_query, 'INSERT' );
        $isUpdate = strpos( $this->_query, 'UPDATE' );

        if ($isInsert !== false) {
            $this->_query .= ' (' . implode( array_keys( $tableData ), ', ') . ')';
            $this->_query .= ' VALUES (';
        }

        foreach ( $tableData as $column => $value ) {
            if ( $isUpdate !== false )
                $this->_query .= " " . $column . " = ";

            // Simple value - extract parameters to be binded
            if ( !is_array( $value )) {
                $this->_query .= '?, ';
                continue;
            }

            // Function value
            $key = key( $value );
            $val = $value[$key];
            switch ( $key ) {
                case '[I]':
                    $this->_query .= $column . $val . ", ";
                    break;
                case '[F]':
                    $this->_query .= $val[0] . ", ";
                    if (!empty($val[1]))
                        break;
                case '[N]':
                    if ($val == null)
                        $this->_query .= "!" . $column . ", ";
                    else
                        $this->_query .= "!" . $val . ", ";
                    break;
                default:
                    die("Wrong operation");
            }
        }
        $this->_query = rtrim( $this->_query, ', ' );

        if ( $isInsert !== false )
            $this->_query .= ')';
    }

    /**
     * Abstraction method that will build the part of the WHERE conditions
     */
    protected function _buildWhere()
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        if ( empty( $this->_where ))
            return;

        //Prepair the where portion of the query
        $this->_query .= ' WHERE ';

        // Remove first AND/OR concatenator
        $this->_where[0][0] = '';
        foreach ( $this->_where as $cond ) {
            list ( $concat, $wValue, $wKey ) = $cond;

            $this->_query .= " " . $concat . " " . $wKey;

            // Empty value (raw where condition in wKey)
            if ( $wValue === null )
                continue;

            // Simple = comparison
            if ( !is_array( $wValue ))
                $wValue = Array('=' => $wValue );

            $key = key( $wValue );
            $val = $wValue[$key];

            switch ( strtolower( $key )) {
                case '0':
                    break;
                case 'not in':
                case 'in':
                    $comparison = ' ' . $key . ' (';
                    foreach ( $val as $v ) {
                        $comparison .= ' ?,';
                    }
                    $this->_query .= rtrim( $comparison, ',') . ' ) ';
                    break;
                case 'not between':
                case 'between':
                    $this->_query .= " $key ? AND ? ";
                    break;
                default:
                    $this->_query .= $this->_buildPair( $key, $val );
            }
        }
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

        if ( !isset( $numRows ))
            return;

        if ( is_array($numRows ))
            $this->_query .= ' LIMIT ' . (int) $numRows[0] . ', ' . (int) $numRows[1];
        else
            $this->_query .= ' LIMIT ' . (int) $numRows;
    }

    /**
     * Method attempts to prepare the CQL query
     *
     * @return cassandra_stmt
     */
    protected function _prepareQuery()
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        try {

            $stmt = $this->_session->prepare( $this->_query );
            return $stmt;
        } catch ( Cassandra\Exception $e ) {

            echo '<strong>ERROR - PREPARE STATEMENT</strong>: ' . $e->getMessage();
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
    protected function _buildExecutionOptions( $arguments = array(), $paging = 0 )
    {
        if ( !$this->isConnected ) {
            $this->connect();
        }

        $executionOptionsArray = array ();

        if ( !empty( $arguments ) ){
            $executionOptionsArray[ 'arguments' ] = $this->_convertArgumentsToCassandra( $arguments );
        }

        if($paging > 0){
            $executionOptionsArray[ 'page_size' ] = $paging;
        }


        try {

            return new Cassandra\ExecutionOptions( $executionOptionsArray );
        } catch ( Cassandra\Exception $e ) {

            echo '<strong>ERROR - BIND</strong>: ' . $e->getMessage();
            return false;
        }
    }

    /**
     * Returns the result from the Cassandra executed query.
     * Either in rows or in pages.
     *
     * @param object    Cassandra\Rows
     * @param boolean   Pages are set or not set
     *
     * @return array|null All rows from the executed statement OR 'null' if there are no results.
     *
     */
    protected function _extractResult( $excutionResult, $hasPages = false )
    {
        if ( $hasPages ){
            return $this->_extractRowsInPages( $excutionResult );
        }

        return $this->_extractRows( $excutionResult );
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

        while ( true ){

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

            if ($executionResult->isLastPage()) {
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
        $cols = array_keys( $executionResult[0] );

        for ( $row = 0; $row < $executionResult->count(); $row++ ) {

            array_push( $convertedResults, $executionResult[$row] );

            if ( $this->autoConvert ) {

                foreach ( $cols as $col ) {
                    
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
     * Initializes the table information
     * 'column name' => 'column type'
     *
     * @param object Cassandra\DefaultTable
     *
     */
    protected function _initTable( $table )
    {
        $this->_table = array();

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

            $arguments[ $argumentColumn ] = $this->_convertToCassandraObject( $arguments[ $argumentColumn ], $argumentColumn );
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
                return new Cassandra\Uuid();
            case 'timeuuid':
                return new Cassandra\Timeuuid();
            case 'inet':
                return new Cassandra\Inet( $paramValue );
            default:
                return $paramValue;
        }
    }
}