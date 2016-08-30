<?php
    return array(
                'templates.path'    => __DIR__ . '/templates/',
                'debug' => false,
                'log.enabled' => true,
                'log.writer' => new \Yee\Log\FileLogger(
                    array(
                        'path' => __DIR__.'/logs',
                        'name_format' => 'Y-m-d',
                        'message_format' => '%label% - %date% - %message%'
                    )
                ),
                'database' => array(
                    'cassandra' => array(
                            'database.type'       => 'cassandra', 
					   		'database.seeds'      => array( '192.168.100.200' ),
					   		'database.port'       => 9042,
					   		'database.keyspace'   => 'hrsystem',
					   		'database.user'       => '',
					   		'database.pass'       => ''
					),
                ),
                'session' => 'php',   // php, database or memcached
                'cache.path'=> __DIR__ . '/cache',
                'cache.timeout'=> 1800,
    );
