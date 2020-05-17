<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Client;

final class InFlightRequest
{
    public function __construct(
        $header,
        string $destination,
        bool $isInternalRequest,
    ) {
    }
}
