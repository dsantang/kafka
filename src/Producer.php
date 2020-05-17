<?php
declare(strict_types=1);

namespace Lcobucci\Kafka;

use Lcobucci\Kafka\Producer\Record;
use Psr\Log\LoggerInterface;
use React\Promise\Promise;

final class Producer
{
    public function __construct(Client $client, LoggerInterface $logger)
    {
    }

    public function send(Record $record): Promise
    {
    }

    public function flush(): void
    {
    }

    public function close(int $timeout = 0): void
    {
    }
}
