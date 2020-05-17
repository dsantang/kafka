<?php
declare(strict_types=1);

namespace Lcobucci\Kafka;

final class Node
{
    /** @var string */
    public $id;

    /** @var string */
    public $host;

    /** @var int */
    public $port;

    /** @var string|null */
    public $rack;

    public function __construct(string $nodeId, string $host, int $port, ?string $rack)
    {
        $this->id   = $nodeId;
        $this->host = $host;
        $this->port = $port;
        $this->rack = $rack;
    }
}
