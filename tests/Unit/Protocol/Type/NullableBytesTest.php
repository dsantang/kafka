<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Test\Unit\Protocol\Type;

use Lcobucci\Kafka\Protocol\Message;
use Lcobucci\Kafka\Protocol\NotEnoughBytesAllocated;
use Lcobucci\Kafka\Protocol\SchemaValidationFailure;
use Lcobucci\Kafka\Protocol\Type\NullableBytes;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use function pack;

/**
 * @coversDefaultClass \Lcobucci\Kafka\Protocol\Type\NullableBytes
 */
final class NullableBytesTest extends TestCase
{
    /**
     * @test
     *
     * @covers ::write
     *
     * @uses \Lcobucci\Kafka\Protocol\Message
     */
    public function writeShouldAppendTheLengthUsingFourBytesAndTheContent(): void
    {
        $message = Message::allocate(8);

        $type = new NullableBytes();
        $type->write(Message::fromContent('1234'), $message);

        $message->reset();

        self::assertSame(4, $message->readInt());
        self::assertSame('1234', $message->read(4));
    }

    /**
     * @test
     *
     * @covers ::write
     *
     * @uses \Lcobucci\Kafka\Protocol\Message
     */
    public function writeShouldUseMinusOneForLengthWhenContentIsNull(): void
    {
        $message = Message::allocate(4);

        $type = new NullableBytes();
        $type->write(null, $message);

        $message->reset();

        self::assertSame(-1, $message->readInt());
    }

    /**
     * @test
     *
     * @covers ::write
     *
     * @uses \Lcobucci\Kafka\Protocol\Message
     */
    public function writeShouldNotModifyContentsPosition(): void
    {
        $message = Message::allocate(8);
        $content = Message::fromContent('1234');

        $type = new NullableBytes();
        $type->write($content, $message);

        self::assertSame(0, $content->position());
    }

    /**
     * @test
     *
     * @covers ::write
     *
     * @uses \Lcobucci\Kafka\Protocol\Message
     * @uses \Lcobucci\Kafka\Protocol\NotEnoughBytesAllocated
     */
    public function writeShouldNotHandleExceptionsFromMessage(): void
    {
        $type = new NullableBytes();

        $this->expectException(NotEnoughBytesAllocated::class);
        $type->write(Message::fromContent('1234'), Message::allocate(4));
    }

    /**
     * @test
     *
     * @covers ::read
     *
     * @uses \Lcobucci\Kafka\Protocol\Message
     */
    public function readShouldReturnTheContentBasedOnLength(): void
    {
        $message = Message::fromContent(pack('N', 4) . 'test' . pack('N', 0) . pack('N', -1));
        $type    = new NullableBytes();

        self::assertEquals(Message::fromContent('test'), $type->read($message));
        self::assertEquals(Message::fromContent(''), $type->read($message));
        self::assertNull($type->read($message));
    }

    /**
     * @test
     *
     * @covers ::read
     *
     * @uses \Lcobucci\Kafka\Protocol\Message
     * @uses \Lcobucci\Kafka\Protocol\NotEnoughBytesAllocated
     */
    public function readShouldNotHandleExceptionsFromMessage(): void
    {
        $type = new NullableBytes();

        $this->expectException(NotEnoughBytesAllocated::class);
        $type->read(Message::allocate(0));
    }

    /**
     * @test
     *
     * @covers ::sizeOf
     *
     * @uses \Lcobucci\Kafka\Protocol\Message
     */
    public function sizeOfShouldReturnFourPlusRemainingBytes(): void
    {
        $type = new NullableBytes();

        self::assertSame(6, $type->sizeOf(Message::allocate(2)));
        self::assertSame(9, $type->sizeOf(Message::allocate(5)));
    }

    /**
     * @test
     *
     * @covers ::sizeOf
     */
    public function sizeOfShouldReturnFourWhenContentIsNull(): void
    {
        $type = new NullableBytes();

        self::assertSame(4, $type->sizeOf(null));
    }

    /**
     * @test
     *
     * @covers ::isNullable
     */
    public function isNullableShouldAlwaysReturnTrue(): void
    {
        $type = new NullableBytes();

        self::assertTrue($type->isNullable());
    }

    /**
     * @test
     *
     * @covers ::validate
     *
     * @uses \Lcobucci\Kafka\Protocol\Type
     * @uses \Lcobucci\Kafka\Protocol\Message
     */
    public function validateShouldNotRaiseExceptionWhenValueIsAMessageOrNull(): void
    {
        $type = new NullableBytes();

        $type->validate(Message::fromContent('test'));
        $type->validate(Message::fromContent('testing'));
        $type->validate(null);

        $this->addToAssertionCount(1);
    }

    /**
     * @test
     *
     * @covers ::validate
     *
     * @uses \Lcobucci\Kafka\Protocol\Type
     * @uses \Lcobucci\Kafka\Protocol\SchemaValidationFailure
     */
    public function validateShouldRaiseExceptionWhenValueIsNotAnObject(): void
    {
        $type = new NullableBytes();

        $this->expectException(SchemaValidationFailure::class);
        $this->expectExceptionMessage('object');

        $type->validate(false);
    }

    /**
     * @test
     *
     * @covers ::validate
     *
     * @uses \Lcobucci\Kafka\Protocol\Type
     * @uses \Lcobucci\Kafka\Protocol\SchemaValidationFailure
     */
    public function validateShouldRaiseExceptionWhenValueIsNotAnInstanceOfMessage(): void
    {
        $type = new NullableBytes();

        $this->expectException(SchemaValidationFailure::class);
        $this->expectExceptionMessage(Message::class);

        $type->validate(new RuntimeException());
    }
}