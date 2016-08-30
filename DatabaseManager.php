<?php

namespace Yee\Managers;

use Yee\Yee;
use Yee\Libraries\Database\MysqliDB;
use Yee\Libraries\Database\CassandraDB;

class DatabaseManager
{
    protected $config;
    
    /**
     * @param array $options
     */
    function __construct( $db_type = "mysql" )
    {
    	$app = \Yee\Yee::getInstance();
    	$this->config = $app->config('database');
    	
    	foreach( $this->config as $key => $database )
    	{
    	     if( $database['database.type'] == "cassandra" )
    	     {
    	         $app->db[$key] = new CassandraDB( $database['database.seeds'], $database['database.user'], $database['database.pass'], $database['database.port'], $database['database.keyspace'] );
    	     } else {
    	         $app->db[$key] = new MysqliDB( $database['database.host'], $database['database.user'], $database['database.pass'], $database['database.name'], $database['database.port'] );
    	     }
    	}	
    }
}