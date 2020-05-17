<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Protocol\Message;

final class ApiVersionsRequest extends Request
{
    public function apiKey(): int
    {
        return 18;
    }

    public function highestSupportedVersion(): int
    {
        return 2;
    }

    public static function schemaDefinition(int $version): array
    {
        return [];
    }

    public function asArray(int $version): array
    {
        return [];
    }

    public function responseClass(): string
    {
        return ApiVersionsResponse::class;
    }
}
