魅族游戏中心日志数据统计：

    离线：
        日志格式：19.167.29.40   [2018-03-04 21:10:16]   (Android o,Meizu note 7)    三国志 104
        
        需求：
            按月统计topN访问的游戏
            按城市统计的topN访问的游戏
            打开游戏中心的topN魅族手机的型号
            按照每天时段统计游戏中心的访问量

        流程：
            generate log by generate_offline_log.py -->
            read data from hdfs/local(text) by RDD(no schema)  -->
            transform RDD to DF(with schema) in two ways(reflection or programming) or by 'ctxt' external datasource -->
            no hbase:
                resave data in hdfs/local with parquet format -->
                read data from hdfs/local(parquet) by DF -->
            with hbase:
                save data in hbase(PairRddFunction.saveAsNewAPIHadoopFile()) -->
                read data from hbase(sc.newAPIHadoopRDD()) -->
            analyse data by DF/SQL -->
            save result in MYSQL

    实时：
        下载日志：{"time":"20191106093559",
                 "user_id":958,
                 "game_id":220}

        付费日志：
                {"flag":"1",
                "order_id":"2b7ce0c4-9d4e-4ac9-8c28-62fdda71a032",
                "fee":193,
                "time":"20191106093559",
                "user_id":958,
                "game_id":220}
        需求：
            统计每天/每小时/每分钟游戏中心分发游戏的数量、通过游戏中心分发的游戏内购的消费金额
            每隔5s统计前10s的数据
            统计排除黑名单用户后的数据

        流程：
            generate streaming log by LogGenerator -->
                - write download_log to hadoop001:44444 by Log4j
                - write consume_log to hadoop001:55555 by Log4j
            read streaming download_log/consume_log from hadoop001:44444/55555(avro source) by Flume -->
            write streaming log into topics stat_download_topic and stat_consumer_topic(Kafka sink) by Flume -->
            consume and analyse data from two topics by integrating SparkStreaming with Kafka



附录：

    建表语句：
    
        create table game_by_month_topn_stat (
        month int,
        game_name varchar(20),
        game_id int,
        times int,
        primary key(month,game_id)
        );

        create table game_by_city_topn_stat (
        city varchar(20),
        game_id int,
        game_name varchar(20),
        times int,
        ranks int,
        primary key(city,game_id)
        );

        create table game_by_phonemodel_times_stat (
        phonemodel varchar(20),
        times int,
        primary key(phonemodel)
        );

        create table game_by_hour_times_stat (
        hour varchar(20),
        times int,
        primary key(hour)
        );

    flume配置
        avro-memory-kafka.sources = avro-source
        avro-memory-kafka.sinks = kafka-sink
        avro-memory-kafka.channels = memory-channel
        avro-memory-kafka.sources.avro-source.type = avro
        avro-memory-kafka.sources.avro-source.bind = hadoop001
        avro-memory-kafka.sources.avro-source.port = 44444(55555)
        avro-memory-kafka.sinks.kafka-sink.type = org.apache.flume.sink.kafka.KafkaSink
        avro-memory-kafka.sinks.kafka-sink.brokerList = hadoop001:9092
        avro-memory-kafka.sinks.kafka-sink.topic = stat_download_topic(stat_cousume_topic)
        avro-memory-kafka.sinks.kafka-sink.batchSize = 3
        avro-memory-kafka.channels.memory-channel.type = memory
        avro-memory-kafka.sources.avro-source.channels = memory-channel
        avro-memory-kafka.sinks.kafka-sink.channel = memory-channel


