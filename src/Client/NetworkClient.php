<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Client;

use Lcobucci\Kafka\Client;
use React\Promise\PromiseInterface;

final class NetworkClient implements Client
{
    public function __construct(string $clientId, ConnectionManager $connectionManager)
    {
    }

    public function send(Request $request): PromiseInterface
    {
    }
}
