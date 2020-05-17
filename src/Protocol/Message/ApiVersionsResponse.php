<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Protocol\Message;

use Lcobucci\Kafka\Protocol\Type;

final class ApiVersionsResponse extends Response
{
    private const SCHEMA_VERSIONS = [
        self::SCHEMA_V0,
        self::SCHEMA_V1,
        self::SCHEMA_V1,
    ];

    private const SCHEMA_V0 = [
        'error_code' => Type\Int16::class,
        'api_versions' => [
            '_items' => [
                'api_key' => Type\Int16::class,
                'min_version' => Type\Int16::class,
                'max_version' => Type\Int16::class,
            ],
        ],
    ];
    private const SCHEMA_V1 = self::SCHEMA_V0 + ['throttle_time_ms' => Type\Int32::class];

    /**
     * @var int
     */
    public $errorCode;

    /**
     * @var array
     */
    public $apiVersions;

    /**
     * @var int
     */
    public $throttleTime;

    public function __construct(int $errorCode, array $apiVersions, int $throttleTime)
    {
        $this->errorCode = $errorCode;
        $this->apiVersions = $apiVersions;
        $this->throttleTime = $throttleTime;
    }

    public static function fromArray(array $data): Response
    {
        return new self(
            $data['error_code'],
            $data['api_versions'],
            $data['throttle_time_ms'] ?? 0
        );
    }

    public static function schemaDefinition(int $version): array
    {
        return self::SCHEMA_VERSIONS[$version];
    }
}
