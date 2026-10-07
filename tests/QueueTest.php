<?php

declare(strict_types=1);

namespace Ph20sr\JobQueue\Tests;

use PDO;
use Ph20sr\JobQueue\Queue;
use PHPUnit\Framework\TestCase;

final class QueueTest extends TestCase
{
    private PDO $pdo;
    private Queue $queue;
    private int $now = 1_800_000_000;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->queue = new Queue($this->pdo, clock: fn (): int => $this->now);
        $this->queue->install();
    }

    public function testFifoAndPayload(): void
    {
        $this->queue->push('emails', ['to' => 'a@x.com']);
        $this->queue->push('emails', ['to' => 'b@x.com', 'nome' => 'João']);
        $this->queue->push('whatsapp', ['to' => '5511987654321']);

        $first = $this->queue->reserve('emails');
        $second = $this->queue->reserve('emails');
        $this->assertSame('a@x.com', $first->payload['to']);
        $this->assertSame('João', $second->payload['nome']);
        $this->assertSame(1, $first->attempts);
        $this->assertNull($this->queue->reserve('emails'), 'as duas estão reservadas');
        $this->assertSame('5511987654321', $this->queue->reserve('whatsapp')->payload['to'], 'filas separadas');
    }

    public function testDelay(): void
    {
        $this->queue->push('emails', ['n' => 1], delaySeconds: 300);
        $this->assertNull($this->queue->reserve('emails'));
        $this->now += 300;
        $this->assertNotNull($this->queue->reserve('emails'));
    }

    public function testCrashedWorkerTaskComesBackAfterTimeout(): void
    {
        $this->queue->push('emails', ['n' => 1]);
        $crashed = $this->queue->reserve('emails', timeout: 60);
        // worker morreu sem concluir

        $this->now += 30;
        $this->assertNull($this->queue->reserve('emails'), 'ainda reservada');
        $this->now += 31;
        $retry = $this->queue->reserve('emails');
        $this->assertSame($crashed->id, $retry->id);
        $this->assertSame(2, $retry->attempts, 'a queda conta como tentativa');

        // O worker "zumbi" que voltou não pode concluir: a reserva agora é de outro
        $this->assertFalse($this->queue->complete($crashed));
        $this->assertTrue($this->queue->complete($retry));
        $this->assertSame(1, $this->queue->stats('emails')['done']);
    }

    public function testFailureBackoffAndDead(): void
    {
        $this->queue->push('webhooks', ['url' => 'https://cliente.exemplo/hook'], maxAttempts: 3);

        $job = $this->queue->reserve('webhooks');
        $this->assertTrue($this->queue->fail($job, new \RuntimeException('HTTP 500')));
        $this->assertNull($this->queue->reserve('webhooks'), 'espera o backoff');
        $this->now += 30;
        $job = $this->queue->reserve('webhooks');
        $this->assertSame(2, $job->attempts);
        $this->queue->fail($job, 'timeout');

        $this->now += 59;
        $this->assertNull($this->queue->reserve('webhooks'), '2ª espera = 60 s');
        $this->now += 1;
        $job = $this->queue->reserve('webhooks');
        $this->assertTrue($job->isLastAttempt());
        $this->queue->fail($job, 'HTTP 500 de novo');

        $this->now += 86400;
        $this->assertNull($this->queue->reserve('webhooks'));
        $stats = $this->queue->stats('webhooks');
        $this->assertSame(1, $stats['dead']);
        $error = $this->pdo->query('SELECT last_error FROM jobs')->fetchColumn();
        $this->assertSame('HTTP 500 de novo', $error);

        $this->assertTrue($this->queue->retry($job->id));
        $this->assertSame(1, $this->queue->reserve('webhooks')->attempts, 'retry zera as tentativas');
    }

    public function testBackoffSchedule(): void
    {
        $this->assertSame([30, 60, 120, 240, 3600], [Queue::backoff(1), Queue::backoff(2), Queue::backoff(3), Queue::backoff(4), Queue::backoff(20)]);
    }

    public function testUniqueKeyDeduplicates(): void
    {
        $a = $this->queue->push('emails', ['fatura' => 123], uniqueKey: 'boleto-email:123');
        $b = $this->queue->push('emails', ['fatura' => 123], uniqueKey: 'boleto-email:123');
        $this->assertSame($a, $b);
        $this->assertSame(1, $this->queue->stats('emails')['pending']);
    }

    public function testWorkProcessesAndIsolatesFailures(): void
    {
        foreach ([1, 2, 3, 4] as $n) {
            $this->queue->push('emails', ['n' => $n]);
        }
        $sent = [];
        $stats = $this->queue->work('emails', function (array $payload) use (&$sent): void {
            if ($payload['n'] === 3) {
                throw new \RuntimeException('caixa cheia');
            }
            $sent[] = $payload['n'];
        });
        $this->assertSame(['done' => 3, 'failed' => 1], $stats);
        $this->assertSame([1, 2, 4], $sent);
        $this->assertSame(['pending' => 1, 'ready' => 0, 'reserved' => 0, 'done' => 3, 'dead' => 0], $this->queue->stats('emails'));
    }

    public function testWorkRespectsMaxJobs(): void
    {
        for ($n = 0; $n < 5; $n++) {
            $this->queue->push('q', ['n' => $n]);
        }
        $this->assertSame(['done' => 2, 'failed' => 0], $this->queue->work('q', fn () => null, maxJobs: 2));
        $this->assertSame(3, $this->queue->stats('q')['ready']);
    }

    public function testPurgeOldFinishedJobs(): void
    {
        $this->queue->push('q', ['n' => 1]);
        $this->queue->work('q', fn () => null);
        $this->now += 8 * 86400;
        $this->assertSame(1, $this->queue->purge());
    }
}
