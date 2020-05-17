<?php
declare(strict_types=1);

use Lcobucci\Kafka\Client\Channel;
use Lcobucci\Kafka\Cluster;
use Lcobucci\Kafka\Protocol\Message\ApiVersionsRequest;
use Lcobucci\Kafka\Protocol\Message\Response;
use Lcobucci\Kafka\Protocol\Schema\Parser;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use React\EventLoop\Factory;
use React\Socket\Connector;

require 'vendor/autoload.php';

$loop = Factory::create();
$logger = new Logger('test-channel', [new StreamHandler('php://stderr')]);

$cluster = Cluster::bootstrap('192.168.122.1:9092,192.168.122.1:9093,192.168.122.1:9094');
$channel = new Channel(
    new Connector($loop),
    $loop,
    $logger,
    new Parser(),
    $cluster->brokers[0]
);

$channel->send(new ApiVersionsRequest(), 0, 'producer-test')->then(
    static function (Response $response) use ($logger): void {
        $logger->debug('Response received', ['response' => $response]);
    }
);

$loop->addSignal(
    SIGINT,
    $listener = static function () use (&$listener, $loop, $channel) {
        $channel->disconnect();

        $loop->removeSignal(SIGINT, $listener);
    }
);

$loop->run();
