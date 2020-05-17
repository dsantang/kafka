<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Protocol\Message;

use Lcobucci\Kafka\Protocol\Buffer;
use Lcobucci\Kafka\Protocol\Schema\Parser;

abstract class Response
{
    public static function parse(Buffer $buffer, Parser $schemaParser, int $version): self
    {
        $schema = $schemaParser->parse(static::schemaDefinition($version));

        return static::fromArray($schema->read($buffer));
    }

    abstract public static function fromArray(array $data): self;

    abstract public static function schemaDefinition(int $version): array;
}
