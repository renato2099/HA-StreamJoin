class Config:
    exec_path_l = '/mnt/SG/marenato/kafka_2.11-0.10.1.0'
    exec_path_h = '/Users/renatomarroquin/Documents/Apache/Kafka/kafka_2.11-0.10.1.0'
    exec_path   = exec_path_l
    partitions  = 16
    sf          = 1
    tuples      = 5
    bid_ratio   = 2
    zk_dir      = '/tmp/zookeeper'
    zk_config   = 'zookeeper.properties'
    k_dir       = '/tmp/kafka-logs'
    k_config    = 'server.properties'
    server      = 'localhost'
    missing     = 0
    pcompletion = 0.9
    psuccess    = 0.1
    aproducer   = 'auction-producer'
    bproducer   = 'bid-producer'
    pjoin       = 'pjoin'
    hajoin      = 'hajoin'
    jarpath     = ''

