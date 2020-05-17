<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Serialization;

interface Serializer
{
    /**
     * @param mixed $data
     */
    public function serialize($data): string;

    /**
     * @return mixed
     */
    public function deserialize(string $data);
}
