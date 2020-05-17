<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Protocol\Message;

use Lcobucci\Kafka\Protocol\Type;
use function count;

final class MetadataRequest extends Request
{
    private const SCHEMA_VERSIONS = [
        ['topics' => ['_items' => Type\NonNullableString::class]],
        self::SCHEMA_V1,
        self::SCHEMA_V1,
        self::SCHEMA_V1,
        self::SCHEMA_V4,
        self::SCHEMA_V4,
        self::SCHEMA_V4,
        self::SCHEMA_V4,
    ];

    private const SCHEMA_V1 = [
        'topics' => ['_nullable' => true, '_items' => Type\NonNullableString::class]
    ];

    private const SCHEMA_V4 = self::SCHEMA_V1 + ['allow_auto_topic_creation' => Type\Boolean::class];

    /**
     * @var array|null
     */
    public $topics;

    /**
     * @var bool
     */
    public $allowTopicCreation;

    public function __construct(?array $topics, bool $allowTopicCreation)
    {
        $this->topics = $topics;
        $this->allowTopicCreation = $allowTopicCreation;
    }

    public function apiKey(): int
    {
        return 3;
    }

    public function highestSupportedVersion(): int
    {
        return count(self::SCHEMA_VERSIONS) - 1;
    }

    public static function schemaDefinition(int $version): array
    {
        return self::SCHEMA_VERSIONS[$version];
    }

    public function asArray(int $version): array
    {
        return [
            'topics' => $version === 0 && $this->topics === null ? [] : $this->topics,
            'allow_auto_topic_creation' => $this->allowTopicCreation,
        ];
    }

    public function responseClass(): string
    {
        return '';
    }
}
