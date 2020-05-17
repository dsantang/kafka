<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Producer;

final class Record
{
    /**
     * @var string
     */
    private $topic;

    /**
     * @var mixed
     */
    private $value;

    /**
     * @var mixed|null
     */
    private $key;

    /**
     * @var string[]|null
     */
    private $headers;

    /**
     * @var int|null
     */
    private $partition;

    /**
     * @var int|null
     */
    private $timestamp;

    /**
     * @param mixed         $value
     * @param mixed|null    $key
     * @param string[]|null $headers
     */
    public function __construct(string $topic, $value, $key = null, ?array $headers = null, ?int $partition = null, ?int $timestamp = null)
    {
        $this->topic     = $topic;
        $this->key       = $key;
        $this->value     = $value;
        $this->headers   = $headers;
        $this->partition = $partition;
        $this->timestamp = $timestamp;
    }

    public function topic(): string
    {
        return $this->topic;
    }

    /**
     * @return mixed
     */
    public function value()
    {
        return $this->value;
    }

    /**
     * @return mixed|null
     */
    public function key()
    {
        return $this->key;
    }

    /**
     * @return string[]|null
     */
    public function headers(): ?array
    {
        return $this->headers;
    }

    public function partition(): ?int
    {
        return $this->partition;
    }

    public function timestamp(): ?int
    {
        return $this->timestamp;
    }
}
