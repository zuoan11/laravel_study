<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;

class KafkaCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'kafka';

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


    protected function consumer()
    {
        $config = \Kafka\ConsumerConfig::getInstance();
        $config->setMetadataRefreshIntervalMs(500);
        $config->setMetadataBrokerList('127.0.0.1:9092');
        $config->setGroupId('test1');
        $config->setBrokerVersion('1.0.0');
        $config->setTopics(array(
            'kafka-test-topic'
        ));
        $config->setOffsetReset("earliest");
        $consumer = new \Kafka\Consumer();
        // $this->info("Kafka Consumer is listening: ");
        $consumer->start(function ($topic, $part, $message) {
            // 新格式数据转化成旧格式
            // event(new TranslateKafkaMessage($message['message']['value']));
            dump($message['message']['value']);
            \Log::info("TranslateKafkaMessageListener", [
                "message" => $message,
                "topic" => $topic,
                "part" => $part,
                "unique_id" => md5($message['message']['value']),
                "version" => "v4"
            ]);
        });
    }

    protected function producer()
    {
        $object = $this;
        $topic = 'kafka-test-topic';
        $value = json_encode(array(
            'aaa',
            'bbb',
            'ccc'
        ));
        $urls = '127.0.0.1:9092';
        $config = \Kafka\ProducerConfig::getInstance();
        $config->setMetadataRefreshIntervalMs(10000);
        $config->setMetadataBrokerList($urls);
        $config->setBrokerVersion('1.0.0');
        $config->setRequiredAck(1);
        $config->setIsAsyn(false);
        $config->setProduceInterval(500);

        $producer = new \Kafka\Producer(function () use ($value, $topic) {
            return [
                [
                    'topic' => $topic,
                    'value' => $value,
                    'key' => '',
                ],
            ];
        });
        $producer->success(function ($result) use ($object) {
            $object->info(json_encode($result));
            return "success";
        });
        $producer->error(function ($errorCode) use ($object) {
            $object->error($errorCode);
        });
        $producer->send(true);
    }

}
