<?php
declare(strict_types=1);

namespace Lcobucci\Kafka;

use function array_map;
use function explode;

final class Cluster
{
    /** @var string|null */
    public $id;

    /** @var Node[] */
    public $brokers;

    public function __construct(?string $id, array $brokers)
    {
        $this->id      = $id;
        $this->brokers = $brokers;
    }

    public static function bootstrap(string $servers): self
    {
        $id      = 0;
        $brokers = array_map(
            static function (string $server) use (&$id): Node {
                [$host, $port] = explode(':', $server);

                return new Node((string) --$id, $host, (int) $port, null);
            },
            explode(',', $servers)
        );

        return new self(null, $brokers);
    }
}
