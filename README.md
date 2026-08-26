O sistema que vamos construir

Vamos chamar provisoriamente de:

PX Trading Lab

Não vamos tentar copiar um home broker. O objetivo é estudar algumas techs.

Nossa sequência

Vamos fazer o projeto em etapas, porque jogar Docker + Kafka + Kubernetes + Dynatrace ao mesmo tempo esconderia o aprendizado.

Criar a Solution .NET 10 e as primeiras APIs.
Criar o YARP Gateway.
Criar PostgreSQL + EF Core.
Implementar criação de ordens.
Dockerizar cada aplicação.
Criar docker-compose.yml.
Adicionar Kafka.
Adicionar Kafka UI.
Criar TradeProcessor.
Implementar Transactional Outbox.
Implementar idempotência.
Observabilidade + Dynatrace.
Kubernetes local.
Escalar Orders API para vários Pods.
Derrubar Pods propositalmente e estudar resiliência.
Levar para Azure/AKS.
