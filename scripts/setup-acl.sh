#!/bin/bash
# Setup Redpanda SASL users + ACL rules
# Chạy trong container: (Get-Content scripts/setup-acl.sh -Raw) -replace "`r`n", "`n" | docker exec -i appointment-backend-redpanda-0-1 bash

set -e

BROKER="localhost:9092"
ADMIN_API="localhost:9644"
SASL_FLAGS="--user admin --password admin-secret --sasl-mechanism SCRAM-SHA-256"

echo "=== Creating SASL/SCRAM users ==="

# Superuser (admin) — dùng cho rpk và Redpanda Console
rpk acl user create admin -p admin-secret --mechanism SCRAM-SHA-256 --api-urls $ADMIN_API

# API service — producer ghi events
rpk acl user create api-service -p api-service-secret --mechanism SCRAM-SHA-256 --api-urls $ADMIN_API

# Telegram consumer — chỉ đọc appointment-events
rpk acl user create telegram-consumer -p telegram-secret --mechanism SCRAM-SHA-256 --api-urls $ADMIN_API

# Chat consumer — chỉ đọc chat-messages
rpk acl user create chat-consumer -p chat-secret --mechanism SCRAM-SHA-256 --api-urls $ADMIN_API

# Kafka Connect — đọc appointment-events, ghi MongoDB, quản lý internal topics
rpk acl user create kafka-connect -p connect-secret --mechanism SCRAM-SHA-256 --api-urls $ADMIN_API

echo ""
echo "=== Setting ACL rules ==="

# --- api-service: producer cho appointment-events và chat-messages ---
rpk acl create --allow-principal User:api-service \
  --operation write --operation describe --operation create \
  --topic appointment-events --topic chat-messages \
  --brokers $BROKER $SASL_FLAGS

# --- telegram-consumer: chỉ đọc appointment-events ---
rpk acl create --allow-principal User:telegram-consumer \
  --operation read --operation describe \
  --topic appointment-events \
  --brokers $BROKER $SASL_FLAGS

rpk acl create --allow-principal User:telegram-consumer \
  --operation read \
  --group appointment-telegram \
  --brokers $BROKER $SASL_FLAGS

# --- chat-consumer: chỉ đọc chat-messages ---
rpk acl create --allow-principal User:chat-consumer \
  --operation read --operation describe \
  --topic chat-messages \
  --brokers $BROKER $SASL_FLAGS

rpk acl create --allow-principal User:chat-consumer \
  --operation read \
  --group chat-storage \
  --brokers $BROKER $SASL_FLAGS

# --- kafka-connect: đọc appointment-events, quản lý internal topics ---
rpk acl create --allow-principal User:kafka-connect \
  --operation read --operation describe \
  --topic appointment-events \
  --brokers $BROKER $SASL_FLAGS

rpk acl create --allow-principal User:kafka-connect \
  --operation all \
  --topic _connect-configs --topic _connect-offsets --topic _connect-status \
  --topic appointment-events-dlq \
  --brokers $BROKER $SASL_FLAGS

rpk acl create --allow-principal User:kafka-connect \
  --operation read \
  --group appointment-connect \
  --group connect-mongodb-appointment-events-sink \
  --brokers $BROKER $SASL_FLAGS

echo ""
echo "=== ACL summary ==="
rpk acl list --brokers $BROKER $SASL_FLAGS

echo ""
echo "=== Done! ==="
echo "Users created: admin, api-service, telegram-consumer, chat-consumer, kafka-connect"
echo ""
echo "Next steps:"
echo "  1. Update KAFKA_USERNAME/KAFKA_PASSWORD in .env for each service"
echo "  2. Restart all services"
