class Config:
    exec_path   = '/mnt/SG/marenato/kafka_2.11-0.10.1.0'
    #exec_path   = ''
    num_parts   = 16
    sf          = 1
    tuples_sf   = 5
    bid_ratio   = 2
    zk_dir      = '/tmp/zookeeper'
    zk_config   = 'zookeeper.properties'
    kafkadir    = '/tmp/kafka-logs'
    server      = 'localhost'
