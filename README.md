O sistema que vamos construir

Vamos chamar provisoriamente de:

PX Trading Lab

Não vamos tentar copiar um home broker. O objetivo é estudar algumas techs.

Nossa sequência

Vamos fazer o projeto em etapas, porque jogar Docker + Kafka + Kubernetes + Dynatrace ao mesmo tempo esconderia o aprendizado.

Etapas:

ETAPA 1 ✅
.NET 10
Orders API
YARP Gateway

        ↓

ETAPA 2 ✅
Dockerfile
Docker network
Docker Compose

        ↓

ETAPA 3 ✅
PostgreSQL
EF Core
Migrations

        ↓

ETAPA 4 ✅
Transactional Outbox

        ↓

ETAPA 5 ✅
Kafka + Kafka UI

        ↓

ETAPA 6 ✅
Trade Processor

        ↓

ETAPA 7
Idempotência
Retry
DLQ

        ↓

ETAPA 8
OpenTelemetry
Dynatrace

        ↓

ETAPA 9
Kubernetes

        ↓

ETAPA 10
Azure AKS
