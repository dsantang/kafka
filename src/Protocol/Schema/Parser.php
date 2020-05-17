<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Protocol\Schema;

use Lcobucci\Kafka\Protocol\Schema;
use Lcobucci\Kafka\Protocol\Type;
use function array_key_exists;
use function is_array;
use function is_string;

final class Parser
{
    public function parse(array $definition): Schema
    {
        return new Schema(...$this->parseFields($definition));
    }

    private function parseFields(array $definition): iterable
    {
        foreach ($definition as $name => $fieldDefinition) {
            yield new Field($name, $this->parseFieldType($fieldDefinition));
        }
    }

    /**
     * @param string|array<string, mixed> $fieldDefinition
     */
    private function parseFieldType($fieldDefinition): Type
    {
        if (is_string($fieldDefinition)) {
            return new $fieldDefinition;
        }

        assert(is_array($fieldDefinition));

        if (array_key_exists('_items', $fieldDefinition)) {
            return new Type\ArrayOf(
                $this->parseFieldType($fieldDefinition['_items']),
                $fieldDefinition['_nullable'] ?? false
            );
        }

        return $this->parse($fieldDefinition);
    }
}
