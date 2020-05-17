<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Protocol\Message;

use Lcobucci\Kafka\Protocol\Buffer;
use Lcobucci\Kafka\Protocol\Message;
use Lcobucci\Kafka\Protocol\Schema\Parser;
use Lcobucci\Kafka\Protocol\Type;
use function assert;

final class RequestHeaders implements Message
{
    private const SCHEMA = [
        'api_key' => Type\Int16::class,
        'api_version' => Type\Int16::class,
        'correlation_id' => Type\Int32::class,
        'client_id' => Type\NullableString::class,
    ];

    /**
     * @var int
     */
    public $apiKey;

    /**
     * @var int
     */
    public $apiVersion;

    /**
     * @var int
     */
    public $correlationId;

    /**
     * @var string
     */
    public $client;

    /**
     * @var callable
     */
    private $responseFactory;

    public function __construct(int $apiKey, int $apiVersion, int $correlationId, string $client, callable $responseFactory)
    {
        $this->apiKey = $apiKey;
        $this->apiVersion = $apiVersion;
        $this->correlationId = $correlationId;
        $this->client = $client;
        $this->responseFactory = $responseFactory;
    }

    public function toBuffer(Parser $schemaParser): Buffer
    {
        $schema = $schemaParser->parse(self::SCHEMA);
        $data = $this->asArray();

        $buffer = Buffer::allocate($schema->sizeOf($data));
        $schema->write($data, $buffer);

        return $buffer;
    }

    public function parseResponse(Buffer $buffer, Parser $schemaParser): Response
    {
        $correlationId = $buffer->readInt();
        assert($this->correlationId === $correlationId);

        return ($this->responseFactory)($buffer, $schemaParser, $this->apiVersion);
    }

    private function asArray(): array
    {
        return [
            'api_key' => $this->apiKey,
            'api_version' => $this->apiVersion,
            'correlation_id' => $this->correlationId,
            'client_id' => $this->client,
        ];
    }
}
