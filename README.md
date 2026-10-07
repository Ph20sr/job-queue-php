# job-queue-php

[![CI](https://github.com/Ph20sr/job-queue-php/actions/workflows/ci.yml/badge.svg)](https://github.com/Ph20sr/job-queue-php/actions/workflows/ci.yml)
![PHP](https://img.shields.io/badge/php-%3E%3D8.1-777bb4)
![zero dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)

**Fila de tarefas em segundo plano** para sistemas PHP, usando o banco que você já tem (MySQL/MariaDB ou SQLite). Não precisa de Redis, RabbitMQ nem servidor extra, e funciona até em **hospedagem compartilhada** com um cron por minuto.

Use para tirar do caminho do usuário o que é lento ou pode falhar: **enviar e-mail e WhatsApp, chamar webhooks, gerar PDF, sincronizar com ERP**.

## O que ela garante

| situação | comportamento |
| --- | --- |
| dois workers ao mesmo tempo | nunca pegam a mesma tarefa (`SKIP LOCKED` no MySQL, `UPDATE` atômico no SQLite) |
| worker morre no meio | a reserva vence e a tarefa **volta para a fila** (contando como tentativa) |
| worker "zumbi" volta depois | não consegue concluir uma tarefa que já é de outro (token de reserva) |
| a tarefa falha | **retentativa com backoff**: 30 s, 1 min, 2 min, 4 min... até 1 h |
| falhou todas as vezes | vai para `dead` com o último erro, e `retry($id)` devolve à fila depois da correção |
| o mesmo e-mail enfileirado duas vezes | `uniqueKey` deduplica |
| enviar só daqui a 1 hora | `delaySeconds` |

## Uso

```php
use Ph20sr\JobQueue\Queue;

$queue = new Queue($pdo);
$queue->install();   // cria a tabela jobs (uma vez)

// No request: enfileira e responde na hora
$queue->push('emails', ['template' => 'boleto', 'invoice' => 123], uniqueKey: 'email-boleto:123');
$queue->push('whatsapp', ['to' => '5511987654321', 'text' => 'Seu pedido saiu!'], delaySeconds: 60);
```

```php
// worker.php: cron a cada minuto (* * * * * php worker.php)
$queue->work('emails', function (array $payload, Job $job) use ($mailer): void {
    $mailer->sendInvoice($payload['invoice']);   // exceção = falha → retentativa automática
}, maxJobs: 50);
```

Ou um processo contínuo (VPS, Docker, supervisor):

```php
while (true) {
    $stats = $queue->work('webhooks', $handler, maxJobs: 100);
    if ($stats['done'] + $stats['failed'] === 0) {
        sleep(2);
    }
}
```

### Monitoramento

```php
$queue->stats('emails');   // ['pending' => 12, 'ready' => 3, 'reserved' => 2, 'done' => 1530, 'dead' => 1]
$queue->retry($id);        // reprocessa uma tarefa morta
$queue->purge();           // apaga concluídas com mais de 7 dias (cron diário)
```

## Requisitos de banco

- **MySQL 8.0+ ou MariaDB 10.6+**, pelo `SKIP LOCKED`
- **SQLite 3.x** (testes e projetos pequenos)

Os testes automatizados rodam em SQLite. O caminho do MySQL usa a mesma interface, com `SELECT ... FOR UPDATE SKIP LOCKED` numa transação.

## Testes

```bash
composer install
vendor/bin/phpunit
```

## Licença

MIT
