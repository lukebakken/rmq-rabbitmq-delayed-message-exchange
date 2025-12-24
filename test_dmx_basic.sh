#!/bin/bash
#
# Test script for RabbitMQ Delayed Message Exchange plugin
# Tests basic functionality: publish with delay, verify delivery
#

set -o errexit
set -o nounset
set -o pipefail

# Configuration
declare -r rabbitmq_host="localhost"
declare -r rabbitmq_port="15672"
declare -r rabbitmq_user="guest"
declare -r rabbitmq_pass="guest"
declare -r vhost="%2F"
declare -r base_url="http://${rabbitmq_host}:${rabbitmq_port}/api"

declare -r exchange_name="test-delayed-exchange"
declare -r queue_name="test-delayed-queue"
declare -r routing_key="test-key"
declare -ri delay_seconds=5

# Colors for output (no emoji)
declare -r green='\033[0;32m'
declare -r yellow='\033[1;33m'
declare -r red='\033[0;31m'
declare -r nc='\033[0m'

echo "=========================================="
echo "RabbitMQ Delayed Message Exchange Test"
echo "=========================================="
echo ""

# Step 1: Create delayed message exchange
echo -e "${yellow}[1/6]${nc} Creating delayed message exchange: ${exchange_name}"
if curl -s -u "${rabbitmq_user}:${rabbitmq_pass}" \
  -X PUT \
  -H "content-type:application/json" \
  -d '{
    "type": "x-delayed-message",
    "durable": true,
    "auto_delete": false,
    "arguments": {
      "x-delayed-type": "direct"
    }
  }' \
  "${base_url}/exchanges/${vhost}/${exchange_name}" >/dev/null 2>&1
then
  echo -e "${green}SUCCESS${nc} Exchange created successfully"
else
  echo -e "${red}FAILED${nc} Failed to create exchange" >&2
  exit 1
fi
echo ""

# Step 2: Create quorum queue
echo -e "${yellow}[2/6]${nc} Creating quorum queue: ${queue_name}"
if curl -s -u "${rabbitmq_user}:${rabbitmq_pass}" \
  -X PUT \
  -H "content-type:application/json" \
  -d '{
    "durable": true,
    "auto_delete": false,
    "arguments": {
      "x-queue-type": "quorum"
    }
  }' \
  "${base_url}/queues/${vhost}/${queue_name}" >/dev/null 2>&1
then
  echo -e "${green}SUCCESS${nc} Queue created successfully"
else
  echo -e "${red}FAILED${nc} Failed to create queue" >&2
  exit 1
fi
echo ""

# Step 3: Bind queue to exchange
echo -e "${yellow}[3/6]${nc} Binding queue to exchange with routing key: ${routing_key}"
if curl -s -u "${rabbitmq_user}:${rabbitmq_pass}" \
  -X POST \
  -H "content-type:application/json" \
  -d "{
    \"routing_key\": \"${routing_key}\"
  }" \
  "${base_url}/bindings/${vhost}/e/${exchange_name}/q/${queue_name}" >/dev/null 2>&1
then
  echo -e "${green}SUCCESS${nc} Binding created successfully"
else
  echo -e "${red}FAILED${nc} Failed to create binding" >&2
  exit 1
fi
echo ""

# Step 4: Publish message with delay
declare publish_time
publish_time=$(date '+%Y-%m-%d %H:%M:%S')
declare -r message_body="Message published at: ${publish_time}"
declare -ri delay_ms=$((delay_seconds * 1000))

echo -e "${yellow}[4/6]${nc} Publishing message with ${delay_seconds}s delay"
echo "       Message: ${message_body}"
echo "       Delay: ${delay_ms}ms"

if curl -s -u "${rabbitmq_user}:${rabbitmq_pass}" \
  -X POST \
  -H "content-type:application/json" \
  -d "{
    \"properties\": {
      \"delivery_mode\": 2,
      \"headers\": {
        \"x-delay\": ${delay_ms}
      }
    },
    \"routing_key\": \"${routing_key}\",
    \"payload\": \"${message_body}\",
    \"payload_encoding\": \"string\"
  }" \
  "${base_url}/exchanges/${vhost}/${exchange_name}/publish" >/dev/null 2>&1
then
  echo -e "${green}SUCCESS${nc} Message published successfully"
else
  echo -e "${red}FAILED${nc} Failed to publish message" >&2
  exit 1
fi
echo ""

# Step 5: Wait for delay + buffer
declare -ri wait_time=10
echo -e "${yellow}[5/6]${nc} Waiting ${wait_time} seconds for message delivery..."

declare -i i
for (( i=1; i<=wait_time; i++ ))
do
  echo -n "."
  sleep 1
done
echo ""
echo -e "${green}SUCCESS${nc} Wait complete"
echo ""

# Step 6: Fetch message from queue
echo -e "${yellow}[6/6]${nc} Fetching message from queue"
declare response
response=$(curl -s -u "${rabbitmq_user}:${rabbitmq_pass}" \
  -X POST \
  -H "content-type:application/json" \
  -d '{
    "count": 1,
    "ackmode": "ack_requeue_false",
    "encoding": "auto"
  }' \
  "${base_url}/queues/${vhost}/${queue_name}/get")

# Check if we got a message
declare -i message_count
message_count=$(echo "${response}" | grep -c "payload" || true)

if (( message_count > 0 ))
then
  echo -e "${green}SUCCESS${nc} Message received successfully!"
  echo ""
  echo "Message details:"
  if command -v python3 >/dev/null 2>&1
  then
    echo "${response}" | python3 -m json.tool
  else
    echo "${response}"
  fi
  echo ""
  echo "=========================================="
  echo "TEST PASSED"
  echo "=========================================="
else
  echo -e "${red}FAILED${nc} No message received" >&2
  echo ""
  echo "Response:"
  echo "${response}"
  echo ""
  echo "=========================================="
  echo "TEST FAILED"
  echo "=========================================="
  exit 1
fi
