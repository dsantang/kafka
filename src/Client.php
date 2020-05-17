<?php
declare(strict_types=1);

namespace Lcobucci\Kafka;

use Lcobucci\Kafka\Client\Request;
use React\Promise\PromiseInterface;

interface Client
{
    public function send(Request $request): PromiseInterface;
}
