<?php

declare(strict_types=1);

namespace Ph20sr\JobQueue;

final class Job
{
    /** @param array<string, mixed> $payload */
    public function __construct(
        public readonly int $id,
        public readonly string $queue,
        public readonly array $payload,
        /** Tentativas, contando a atual. */
        public readonly int $attempts,
        public readonly int $maxAttempts,
        /** Identifica ESTA reserva: só quem reservou pode concluir ou falhar a tarefa. */
        public readonly string $token,
    ) {
    }

    public function isLastAttempt(): bool
    {
        return $this->attempts >= $this->maxAttempts;
    }
}
