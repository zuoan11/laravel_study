<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;

class Rdkafka extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'rdkafka';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        // $this -> producer();
        $this -> consumer();
    }

    public function consumer()
    {
        $conf = new \RdKafka\Conf();

        // Set a rebalance callback to log partition assignments (optional)
        $conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo "Assign: ";
                    var_dump($partitions);
                    $kafka->assign($partitions);
                    break;

                 case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                     echo "Revoke: ";
                     var_dump($partitions);
                     $kafka->assign(NULL);
                     break;

                 default:
                    throw new \Exception($err);
            }
        });

        // Configure the group.id. All consumer with the same group.id will consume
        // different partitions.
        $conf->set('group.id', 'test5');

        // Initial list of Kafka brokers
        $conf->set('metadata.broker.list', '127.0.0.1');

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'smallest': start from the beginning
        $conf->set('auto.offset.reset', 'smallest');

        $consumer = new \RdKafka\KafkaConsumer($conf);

        // Subscribe to topic 'test'
        $consumer->subscribe(['kafka-test-topic']);

        echo "Waiting for partition assignment... (make take some time when\n";
        echo "quickly re-joining the group after leaving it.)\n";

        while (true) {
            $message = $consumer->consume(120*1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    var_dump($message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }

    public function _consumer()
    {

        $conf = new \RdKafka\Conf();
        $conf->set('group.id', 'test3');
        $rk = new \RdKafka\Consumer($conf);
        $rk->addBrokers("127.0.0.1:9092");
        $topicConf = new \RdKafka\TopicConf();
        $topicConf->set('auto.commit.interval.ms', 100);
        $topicConf->set('offset.store.method', 'file');
        $topicConf->set('offset.store.path', sys_get_temp_dir());
        $topicConf->set('auto.offset.reset', 'earliest');
        $topic = $rk->newTopic("kafka-test-topic", $topicConf);
        $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);

        while(true) {
            $message = $topic->consume(0, 5000);
            if ($message) {
                echo "读取到消息\n\r";
                var_dump($message);
                switch($message->err)
                {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        echo "读取消息成功:\n\r";
                        var_dump($message->payload);
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        echo "读取消息失败\n\r";
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        echo "请求超时\n\r";
                        break;
                    default:
                        throw new \Exception($message->errstr(), $message->err);
                    break;
                }
            } else {
                echo "未读取到消息\n\r";
            }
        }
    }

    public function producer()
    {
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', 'localhost:9092');

        //If you need to produce exactly once and want to keep the original produce order, uncomment the line below
        //$conf->set('enable.idempotence', 'true');

        $producer = new \RdKafka\Producer($conf);

        $topic = $producer->newTopic("kafka-test-topic");

        for ($i = 0; $i < 10; $i++) {
            $topic->produce(RD_KAFKA_PARTITION_UA, 0, "Message $i");
            $producer->poll(0);
        }

        for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
            $result = $producer->flush(10000);
            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }
    }

    public function _producer()
    {
        $conf = new RdKafka\Conf();
        $conf->setDrMsgCb(function ($kafka, $message) {
            file_put_contents("./dr_cb.log", var_export($message, true).PHP_EOL, FILE_APPEND);
        });
        $conf->setErrorCb(function ($kafka, $err, $reason) {
            file_put_contents("./err_cb.log", sprintf("Kafka error: %s (reason: %s)", rd_kafka_err2str($err), $reason).PHP_EOL, FILE_APPEND);
        });

        $rk = new RdKafka\Producer($conf);
        $rk->setLogLevel(LOG_DEBUG);
        $rk->addBrokers("127.0.0.1");

        $cf = new RdKafka\TopicConf();
        // -1必须等所有brokers同步完成的确认 1当前服务器确认 0不确认，这里如果是0回调里的offset无返回，如果是1和-1会返回offset
        // 我们可以利用该机制做消息生产的确认，不过还不是100%，因为有可能会中途kafka服务器挂掉
        $cf->set('request.required.acks', 0);
        $topic = $rk->newTopic("kafka-test-topic", $cf);

        $option = 'qkl';
        for ($i = 0; $i < 20; $i++) {
            //RD_KAFKA_PARTITION_UA自动选择分区
            //$option可选
            $topic->produce(RD_KAFKA_PARTITION_UA, 0, "qkl . $i", $option);
        }


        $len = $rk->getOutQLen();
        while ($len > 0) {
            $len = $rk->getOutQLen();
            var_dump($len);
            $rk->poll(50);
        }
    }
}
