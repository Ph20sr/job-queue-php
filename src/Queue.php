<?php

declare(strict_types=1);

namespace Ph20sr\JobQueue;

use PDO;

/**
 *     $queue = new Queue($pdo);
 *     $queue->push('emails', ['to' => 'maria@x.com', 'template' => 'boas-vindas']);
 *
 *     // worker (cron a cada minuto ou processo contínuo)
 *     $queue->work('emails', fn (array $p) => $mailer->send($p), maxJobs: 50);
 */
final class Queue
{
    private readonly string $driver;
    /** @var \Closure(): int */
    private \Closure $clock;

    public function __construct(private readonly PDO $pdo, private readonly string $table = 'jobs', ?callable $clock = null)
    {
        if (!preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $table)) {
            throw new \InvalidArgumentException("Nome de tabela inválido: {$table}");
        }
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->driver = (string) $this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $this->clock = $clock !== null ? \Closure::fromCallable($clock) : static fn (): int => time();
    }

    public function install(): void
    {
        $mysql = $this->driver === 'mysql';
        $id = $mysql ? 'BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY' : 'INTEGER PRIMARY KEY AUTOINCREMENT';
        $index = $mysql ? ",\n            INDEX idx_{$this->table}_ready (queue, status, available_at)" : '';
        $this->pdo->exec("CREATE TABLE IF NOT EXISTS {$this->table} (
            id {$id},
            queue VARCHAR(64) NOT NULL,
            payload TEXT NOT NULL,
            status VARCHAR(16) NOT NULL DEFAULT 'pending',
            attempts INT NOT NULL DEFAULT 0,
            max_attempts INT NOT NULL,
            available_at BIGINT NOT NULL,
            reserved_token VARCHAR(64) NULL,
            reserved_until BIGINT NULL,
            last_error TEXT NULL,
            unique_key VARCHAR(190) NULL UNIQUE,
            created_at BIGINT NOT NULL,
            finished_at BIGINT NULL{$index}
        )");
        if (!$mysql) {
            $this->pdo->exec("CREATE INDEX IF NOT EXISTS idx_{$this->table}_ready ON {$this->table} (queue, status, available_at)");
        }
    }

    /**
     * Enfileira uma tarefa. Com `uniqueKey`, enfileirar de novo a mesma chave
     * devolve o id existente em vez de duplicar (ex.: "boleto-email:fat_123").
     *
     * @param array<string, mixed> $payload
     */
    public function push(string $queue, array $payload, int $delaySeconds = 0, int $maxAttempts = 5, ?string $uniqueKey = null): int
    {
        if ($maxAttempts < 1) {
            throw new \InvalidArgumentException('maxAttempts deve ser >= 1');
        }
        $now = ($this->clock)();
        $sql = ($this->driver === 'mysql' ? 'INSERT IGNORE' : 'INSERT OR IGNORE')
            . " INTO {$this->table} (queue, payload, max_attempts, available_at, unique_key, created_at) VALUES (?, ?, ?, ?, ?, ?)";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([$queue, json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE), $maxAttempts, $now + max(0, $delaySeconds), $uniqueKey, $now]);
        if ($stmt->rowCount() === 0 && $uniqueKey !== null) {
            $find = $this->pdo->prepare("SELECT id FROM {$this->table} WHERE unique_key = ?");
            $find->execute([$uniqueKey]);
            return (int) $find->fetchColumn();
        }
        return (int) $this->pdo->lastInsertId();
    }

    /**
     * Reserva a próxima tarefa disponível por `timeout` segundos. Se o worker
     * morrer, a tarefa volta para a fila quando a reserva vence.
     */
    public function reserve(string $queue, int $timeout = 60): ?Job
    {
        $now = ($this->clock)();
        $token = bin2hex(random_bytes(16));
        $ready = "queue = ? AND status = 'pending' AND available_at <= ? AND (reserved_until IS NULL OR reserved_until < ?)";

        if ($this->driver === 'mysql') {
            // SKIP LOCKED: workers simultâneos pegam tarefas diferentes, sem esperar uns pelos outros
            $this->pdo->beginTransaction();
            try {
                $stmt = $this->pdo->prepare("SELECT id FROM {$this->table} WHERE {$ready} ORDER BY available_at, id LIMIT 1 FOR UPDATE SKIP LOCKED");
                $stmt->execute([$queue, $now, $now]);
                $id = $stmt->fetchColumn();
                if ($id !== false) {
                    $this->pdo->prepare("UPDATE {$this->table} SET reserved_token = ?, reserved_until = ?, attempts = attempts + 1 WHERE id = ?")
                        ->execute([$token, $now + $timeout, $id]);
                }
                $this->pdo->commit();
            } catch (\Throwable $e) {
                $this->pdo->rollBack();
                throw $e;
            }
        } else {
            // SQLite: um único UPDATE é atômico; a condição é repetida no WHERE externo
            $this->pdo->prepare(
                "UPDATE {$this->table} SET reserved_token = ?, reserved_until = ?, attempts = attempts + 1
                 WHERE id = (SELECT id FROM {$this->table} WHERE {$ready} ORDER BY available_at, id LIMIT 1)
                   AND (reserved_until IS NULL OR reserved_until < ?)",
            )->execute([$token, $now + $timeout, $queue, $now, $now, $now]);
        }

        $stmt = $this->pdo->prepare("SELECT * FROM {$this->table} WHERE reserved_token = ?");
        $stmt->execute([$token]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        if (!$row) {
            return null;
        }
        return new Job((int) $row['id'], (string) $row['queue'], json_decode((string) $row['payload'], true), (int) $row['attempts'], (int) $row['max_attempts'], $token);
    }

    /** Marca como concluída. false se a reserva já tinha vencido e outro worker pegou a tarefa. */
    public function complete(Job $job): bool
    {
        $stmt = $this->pdo->prepare(
            "UPDATE {$this->table} SET status = 'done', finished_at = ?, reserved_token = NULL, reserved_until = NULL, last_error = NULL
             WHERE id = ? AND reserved_token = ?",
        );
        $stmt->execute([($this->clock)(), $job->id, $job->token]);
        return $stmt->rowCount() > 0;
    }

    /**
     * Registra a falha: volta para a fila com backoff exponencial (30 s, 1 min,
     * 2 min... até 1 h) ou, na última tentativa, vai para "dead".
     */
    public function fail(Job $job, \Throwable|string $error): bool
    {
        $message = $error instanceof \Throwable ? get_class($error) . ': ' . $error->getMessage() : $error;
        $now = ($this->clock)();
        $dead = $job->isLastAttempt();
        $stmt = $this->pdo->prepare(
            "UPDATE {$this->table} SET status = ?, available_at = ?, last_error = ?, reserved_token = NULL, reserved_until = NULL, finished_at = ?
             WHERE id = ? AND reserved_token = ?",
        );
        $stmt->execute([
            $dead ? 'dead' : 'pending',
            $now + self::backoff($job->attempts),
            mb_substr($message, 0, 2000),
            $dead ? $now : null,
            $job->id,
            $job->token,
        ]);
        return $stmt->rowCount() > 0;
    }

    public static function backoff(int $attempt): int
    {
        return min(3600, 30 * 2 ** max(0, $attempt - 1));
    }

    /**
     * Processa tarefas até a fila esvaziar ou atingir `maxJobs`. Ideal para um
     * cron a cada minuto em hospedagem compartilhada.
     *
     * @param callable(array<string, mixed>, Job): void $handler
     * @return array{done: int, failed: int}
     */
    public function work(string $queue, callable $handler, int $maxJobs = 100, int $timeout = 60): array
    {
        $stats = ['done' => 0, 'failed' => 0];
        for ($i = 0; $i < $maxJobs; $i++) {
            $job = $this->reserve($queue, $timeout);
            if ($job === null) {
                break;
            }
            try {
                $handler($job->payload, $job);
                $this->complete($job);
                $stats['done']++;
            } catch (\Throwable $e) {
                $this->fail($job, $e);
                $stats['failed']++;
            }
        }
        return $stats;
    }

    /** @return array{pending: int, ready: int, reserved: int, done: int, dead: int} */
    public function stats(string $queue): array
    {
        $now = ($this->clock)();
        $stmt = $this->pdo->prepare(
            "SELECT
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'pending' AND available_at <= ? AND (reserved_until IS NULL OR reserved_until < ?) THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'pending' AND reserved_until >= ? THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'dead' THEN 1 ELSE 0 END)
             FROM {$this->table} WHERE queue = ?",
        );
        $stmt->execute([$now, $now, $now, $queue]);
        $r = array_map('intval', $stmt->fetch(PDO::FETCH_NUM));
        return ['pending' => $r[0], 'ready' => $r[1], 'reserved' => $r[2], 'done' => $r[3], 'dead' => $r[4]];
    }

    /** Devolve uma tarefa "dead" para a fila (depois de corrigir o problema). */
    public function retry(int $id): bool
    {
        $stmt = $this->pdo->prepare(
            "UPDATE {$this->table} SET status = 'pending', attempts = 0, available_at = ?, finished_at = NULL WHERE id = ? AND status = 'dead'",
        );
        $stmt->execute([($this->clock)(), $id]);
        return $stmt->rowCount() > 0;
    }

    /** Apaga tarefas concluídas há mais de `olderThanSeconds`. */
    public function purge(int $olderThanSeconds = 7 * 86400): int
    {
        $stmt = $this->pdo->prepare("DELETE FROM {$this->table} WHERE status = 'done' AND finished_at < ?");
        $stmt->execute([($this->clock)() - $olderThanSeconds]);
        return $stmt->rowCount();
    }
}
