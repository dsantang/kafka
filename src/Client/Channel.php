<?php
declare(strict_types=1);

namespace Lcobucci\Kafka\Client;

use Lcobucci\Kafka\Node;
use Lcobucci\Kafka\Protocol\Buffer;
use Lcobucci\Kafka\Protocol\Message\Request;
use Lcobucci\Kafka\Protocol\Message\RequestHeaders;
use Lcobucci\Kafka\Protocol\Schema\Parser;
use Psr\Log\LoggerInterface;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\Socket\ConnectionInterface;
use React\Socket\ConnectorInterface;
use SplPriorityQueue;
use SplQueue;
use Throwable;
use function assert;

final class Channel
{
    /**
     * @var ConnectorInterface
     */
    private $connector;

    /**
     * @var LoopInterface
     */
    private $loop;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Node
     */
    private $node;

    /**
     * @var SplPriorityQueue
     */
    private $processingQueue;

    /**
     * @var ConnectionInterface|null
     */
    private $connection;

    /**
     * @var SplQueue
     */
    private $inFlightQueue;
    /**
     * @var Parser
     */
    private $schemaParser;

    public function __construct(ConnectorInterface $connector, LoopInterface $loop, LoggerInterface $logger, Parser $schemaParser, Node $node)
    {
        $this->connector = $connector;
        $this->loop      = $loop;
        $this->logger    = $logger;
        $this->node      = $node;
        $this->schemaParser = $schemaParser;
        $this->processingQueue  = new SplPriorityQueue();
        $this->inFlightQueue = new SplQueue();
    }

    public function send(Request $request, int $correlation, string $client): PromiseInterface
    {
        $this->ensureConnected();

        $deferred = new Deferred();
        $this->processingQueue->insert([[$request, $correlation, $client], $deferred], 0);

        return $deferred->promise();
    }

    private function ensureConnected(): void
    {
        if ($this->connection !== null) {
            return;
        }

        $this->connect();
    }

    private function connect(): void
    {
        $this->logger->info('Opening connection to node', ['node' => $this->node]);

        $this->connector->connect($this->node->host . ':' . $this->node->port)->then(
            [$this, 'initializeConnection'],
            function (Throwable $throwable): void {
                $this->logger->error(
                    'Error while connecting to node #' . $this->node->id,
                    ['node' => $this->node, 'exception' => $throwable]
                );

                $this->loop->addTimer(
                    1,
                    function () {
                        $this->connect();
                    }
                );
            }
        );
    }

    public function initializeConnection(ConnectionInterface $connection): void
    {
        $this->logger->info('Connection to node established', ['node' => $this->node]);

        $this->connection = $connection;
        $this->connection->on('data', [$this, 'onData']);
        $this->connection->on('error', [$this, 'cleanUpConnection']);
        $this->connection->on('close', [$this, 'cleanUpConnection']);

        $this->loop->futureTick([$this, 'processQueue']);
    }

    public function processQueue(): void
    {
        if (! $this->processingQueue->valid()) {
            return;
        }

        $this->logger->debug('Processing message queue of node', ['node' => $this->node]);

        for ($i = 0, $max = min(15, $this->processingQueue->count()); $i < $max; ++$i) {
            [$data, $deferred] = $this->processingQueue->current();

            $this->processingQueue->next();
            $headers = $this->sendMessage($data);
            $this->inFlightQueue->enqueue([$headers, $deferred]);
        }

        $this->loop->futureTick([$this, 'processQueue']);
    }

    private function sendMessage(array $message): RequestHeaders
    {
        [$request, $correlation, $client] = $message;
        assert($request instanceof Request);

        $headers = new RequestHeaders(
            $request->apiKey(),
            $request->highestSupportedVersion(),
            $correlation,
            $client,
            [$request->responseClass(), 'parse']
        );

        $header = $headers->toBuffer($this->schemaParser);
        $body = $request->toBuffer($this->schemaParser, $headers->apiVersion);

        $length = Buffer::allocate(4);
        $length->writeInt($header->length() + $body->length());

        $this->connection->write($length->bytes());
        $this->connection->write($header->bytes());
        $this->connection->write($body->bytes());

        return $headers;
    }

    public function onData(string $data): void
    {
        $this->logger->debug('Message received', ['node' => $this->node]);

        [$headers, $deferred] = $this->inFlightQueue->dequeue();
        assert($headers instanceof RequestHeaders);
        assert($deferred instanceof Deferred);

        $buffer = Buffer::fromContent($data);
        $length = $buffer->readInt();

        $deferred->resolve($headers->parseResponse(Buffer::fromContent($buffer->read($length)), $this->schemaParser));
    }

    public function disconnect(): void
    {
        if ($this->connection === null) {
            return;
        }

        $this->connection->end();
    }

    public function cleanUpConnection(): void
    {
        $this->logger->info('Closing connection to node', ['node' => $this->node]);

        $this->connection = null;
    }
}
