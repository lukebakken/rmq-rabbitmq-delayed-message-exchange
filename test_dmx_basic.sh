#!/bin/bash
# vim: set ts=2 sw=2 et:
#
# Test script for RabbitMQ Delayed Message Exchange plugin
# Tests basic functionality: publish with delay, verify delivery
#
# Usage: ./test_dmx_basic.sh [num_messages] [min_delay_seconds] [max_delay_seconds]
#   num_messages: Number of messages to publish (default: 1)
#   min_delay_seconds: Minimum delay in seconds (default: 3)
#   max_delay_seconds: Maximum delay in seconds (default: 3)
#

set -o errexit
set -o nounset
set -o pipefail

# Show help
if [[ "${1:-}" == "--help" ]] || [[ "${1:-}" == "-h" ]]
then
  cat <<EOF
Usage: $0 [num_messages] [min_delay_seconds] [max_delay_seconds]

Test RabbitMQ Delayed Message Exchange plugin by publishing messages with
random delays and verifying they are delivered correctly.

Arguments:
  num_messages        Number of messages to publish (default: 1)
  min_delay_seconds   Minimum delay in seconds (default: 3)
  max_delay_seconds   Maximum delay in seconds (default: 3)

Examples:
  $0                  # Publish 1 message with 3s delay
  $0 5                # Publish 5 messages with 3s delay each
  $0 10 1 5           # Publish 10 messages with random delays 1-5s
  $0 20 0 10          # Publish 20 messages with random delays 0-10s

EOF
  exit 0
fi

# Parse arguments
declare -ri num_messages="${1:-1}"
declare -ri min_delay="${2:-3}"
declare -ri max_delay="${3:-3}"

# Validate arguments
if (( num_messages < 1 ))
then
  printf "Error: num_messages must be >= 1\n" >&2
  exit 1
fi

if (( min_delay < 0 ))
then
  printf "Error: min_delay must be >= 0\n" >&2
  exit 1
fi

if (( max_delay < min_delay ))
then
  printf "Error: max_delay must be >= min_delay\n" >&2
  exit 1
fi

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

# Colors for output
declare -r green='\033[0;32m'
declare -r yellow='\033[1;33m'
declare -r red='\033[0;31m'
declare -r nc='\033[0m'

# Logging functions
log_header() {
  printf "==========================================\n"
  printf "%s\n" "$1"
  printf "==========================================\n\n"
}

log_step() {
  printf "${yellow}[%s]${nc} %s\n" "$1" "$2"
}

log_info() {
  printf "       %s\n" "$1"
}

log_success() {
  printf "${green}SUCCESS${nc} %s\n" "$1"
}

log_error() {
  printf "${red}FAILED${nc} %s\n" "$1" >&2
}

# Function to publish a message with delay
publish_message() {
  local -r msg_id="$1"
  local -ri delay_seconds="$2"
  local -ri delay_ms=$((delay_seconds * 1000))
  local -r timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  local -r message_body="Message $msg_id published at $timestamp"
  local -i http_code

  http_code=$(curl -s -u "$rabbitmq_user:$rabbitmq_pass" \
    -w "%{http_code}" \
    -o /dev/null \
    -X POST \
    -H "content-type:application/json" \
    -d "{
      \"properties\": {
        \"delivery_mode\": 2,
        \"headers\": {
          \"x-delay\": $delay_ms
        }
      },
      \"routing_key\": \"$routing_key\",
      \"payload\": \"$message_body\",
      \"payload_encoding\": \"string\"
    }" \
    "$base_url/exchanges/$vhost/$exchange_name/publish")

  if (( http_code == 200 ))
  then
    return 0
  else
    return 1
  fi
}

log_header "RabbitMQ Delayed Message Exchange Test"
log_info "Messages: $num_messages"
log_info "Delay range: ${min_delay}s - ${max_delay}s"
printf "\n"

# Step 1: Create delayed message exchange
log_step "1/5" "Creating delayed message exchange: $exchange_name"
declare -i http_code
http_code=$(curl -s -u "$rabbitmq_user:$rabbitmq_pass" \
  -w "%{http_code}" \
  -o /dev/null \
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
  "$base_url/exchanges/$vhost/$exchange_name")

if (( http_code == 201 || http_code == 204 ))
then
  log_success "Exchange created successfully"
else
  log_error "Failed to create exchange (HTTP $http_code)"
  exit 1
fi
printf "\n"

# Step 2: Create or purge queue
log_step "2/5" "Preparing queue: $queue_name"
declare -i queue_check_code
queue_check_code=$(curl -s -u "$rabbitmq_user:$rabbitmq_pass" \
  -w "%{http_code}" \
  -o /dev/null \
  "$base_url/queues/$vhost/$queue_name")

if (( queue_check_code == 200 ))
then
  log_info "Queue exists, purging..."
  declare -i purge_code
  purge_code=$(curl -s -u "$rabbitmq_user:$rabbitmq_pass" \
    -w "%{http_code}" \
    -o /dev/null \
    -X DELETE \
    "$base_url/queues/$vhost/$queue_name/contents")

  if (( purge_code == 204 ))
  then
    log_success "Queue purged successfully"
  else
    log_error "Failed to purge queue (HTTP $purge_code)"
    exit 1
  fi
else
  log_info "Queue does not exist, creating..."
  declare -i create_code
  create_code=$(curl -s -u "$rabbitmq_user:$rabbitmq_pass" \
    -w "%{http_code}" \
    -o /dev/null \
    -X PUT \
    -H "content-type:application/json" \
    -d '{
      "durable": true,
      "auto_delete": false,
      "arguments": {
        "x-queue-type": "quorum"
      }
    }' \
    "$base_url/queues/$vhost/$queue_name")

  if (( create_code == 201 || create_code == 204 ))
  then
    log_success "Queue created successfully"
  else
    log_error "Failed to create queue (HTTP $create_code)"
    exit 1
  fi
fi
printf "\n"

# Step 3: Bind queue to exchange
log_step "3/5" "Binding queue to exchange with routing key: $routing_key"
declare -i bind_code
bind_code=$(curl -s -u "$rabbitmq_user:$rabbitmq_pass" \
  -w "%{http_code}" \
  -o /dev/null \
  -X POST \
  -H "content-type:application/json" \
  -d "{
    \"routing_key\": \"$routing_key\"
  }" \
  "$base_url/bindings/$vhost/e/$exchange_name/q/$queue_name")

if (( bind_code == 201 || bind_code == 204 ))
then
  log_success "Binding created successfully"
else
  log_error "Failed to create binding (HTTP $bind_code)"
  exit 1
fi
printf "\n"

# Step 4: Publish messages with random delays
log_step "4/5" "Publishing $num_messages message(s) with random delays"
declare -i max_delay_used=0
declare -i i

for (( i=1; i<=num_messages; i++ ))
do
  # Calculate random delay in range [min_delay, max_delay]
  declare -i delay
  if (( min_delay == max_delay ))
  then
    delay=$min_delay
  else
    delay=$(( min_delay + (RANDOM % (max_delay - min_delay + 1)) ))
  fi

  # Track maximum delay used
  if (( delay > max_delay_used ))
  then
    max_delay_used=$delay
  fi

  log_info "Message $i: ${delay}s delay"

  if ! publish_message "$i" "$delay"
  then
    log_error "Failed to publish message $i"
    exit 1
  fi
done
log_success "All messages published successfully"
printf "\n"

# Step 5: Wait for delivery
declare -ri wait_time=$((max_delay_used + 1))
log_step "5/5" "Waiting $wait_time seconds for message delivery..."

for (( i=1; i<=wait_time; i++ ))
do
  printf "."
  sleep 1
done
printf "\n"
log_success "Wait complete"
printf "\n"

# Step 6: Fetch and verify messages
log_info "Fetching messages from queue..."
declare response
response=$(curl -s -u "$rabbitmq_user:$rabbitmq_pass" \
  -X POST \
  -H "content-type:application/json" \
  -d "{
    \"count\": $num_messages,
    \"ackmode\": \"ack_requeue_false\",
    \"encoding\": \"auto\"
  }" \
  "$base_url/queues/$vhost/$queue_name/get")

# Count messages received
declare -i message_count
message_count=$(jq 'length' <<< "$response")

if (( message_count == num_messages ))
then
  log_success "Received $message_count/$num_messages messages!"
  printf "\nMessage details:\n"
  jq '.' <<< "$response"
  printf "\n%b==========================================\n" "$green"
  printf "TEST PASSED\n"
  printf "==========================================%b\n" "$nc"
else
  log_error "Expected $num_messages messages, received $message_count"
  printf "\nResponse:\n"
  jq '.' <<< "$response"
  printf "\n%b==========================================\n" "$red"
  printf "TEST FAILED\n"
  printf "==========================================%b\n" "$nc"
  exit 1
fi
