#!/bin/bash
# vim: set ts=2 sw=2 et:
#
# Test script for RabbitMQ Delayed Message Exchange plugin
# Tests basic functionality: publish with delay, verify delivery
#

set -o errexit
set -o nounset
set -o pipefail

# Default values
declare -i num_messages=1
declare -i min_delay=3
declare -i max_delay=3
declare -a hosts
declare -i verbosity=0

# Parse arguments
show_usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Test RabbitMQ Delayed Message Exchange plugin by publishing messages with
random delays and verifying they are delivered correctly.

Options:
  -n, --num-messages NUM    Number of messages to publish (default: 1)
  -min, --min-delay SEC     Minimum delay in seconds (default: 3)
  -max, --max-delay SEC     Maximum delay in seconds (default: 3)
  -c, --connect HOST:PORT   RabbitMQ host:port (default: localhost:15672)
                            Can be specified multiple times for round-robin
  -v, --verbose             Increase verbosity (can be repeated)
  -h, --help                Show this help message

Examples:
  $0                                    # Use all defaults
  $0 -n 5                               # Publish 5 messages with 3s delay
  $0 -n 10 -min 1 -max 5                # 10 messages, random delays 1-5s
  $0 -n 20 -min 0 -max 10               # 20 messages, random delays 0-10s
  $0 -n 5 -v                            # Show message details
  $0 -c host1:15672 -c host2:15672 -n 5 # Round-robin across 2 hosts

EOF
}

while (( $# > 0 ))
do
  case "$1" in
    -h|--help)
      show_usage
      exit 0
      ;;
    -n|--num-messages)
      if [[ -z "${2:-}" ]]
      then
        printf "Error: --num-messages requires a value\n" >&2
        exit 1
      fi
      num_messages=$2
      shift 2
      ;;
    -min|--min-delay)
      if [[ -z "${2:-}" ]]
      then
        printf "Error: --min-delay requires a value\n" >&2
        exit 1
      fi
      min_delay=$2
      shift 2
      ;;
    -max|--max-delay)
      if [[ -z "${2:-}" ]]
      then
        printf "Error: --max-delay requires a value\n" >&2
        exit 1
      fi
      max_delay=$2
      shift 2
      ;;
    -c|--connect)
      if [[ -z "${2:-}" ]]
      then
        printf "Error: --connect requires a value\n" >&2
        exit 1
      fi
      # Validate host:port format
      if [[ ! "$2" =~ ^[^:]+:[0-9]+$ ]]
      then
        printf "Error: --connect must be in format host:port (got: %s)\n" "$2" >&2
        exit 1
      fi
      hosts+=("$2")
      shift 2
      ;;
    -v|--verbose)
      (( ++verbosity ))
      shift
      ;;
    *)
      printf "Error: Unknown option: %s\n" "$1" >&2
      show_usage
      exit 1
      ;;
  esac
done

if (( ${#hosts[@]} == 0 ))
then
  hosts=("localhost:15672")
fi

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
declare -r rabbitmq_user="guest"
declare -r rabbitmq_pass="guest"
declare -r vhost="%2F"

declare -r exchange_name="test-delayed-exchange"
declare -r queue_name="test-delayed-queue"
declare -r routing_key="test-key"

# Round-robin host index
declare -i current_host_index=0

# Function to get current base URL (does not modify counter)
get_base_url() {
  local host_port="${hosts[$current_host_index]}"
  printf "http://%s/api" "$host_port"
}

# Function to advance to next host
advance_host() {
  current_host_index=$(( (current_host_index + 1) % ${#hosts[@]} ))
}

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
  local base_url
  base_url=$(get_base_url)

  if (( verbosity >= 2 ))
  then
    log_info "Publishing to: $base_url (index was $current_host_index, now advancing)"
  fi

  advance_host

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
declare base_url
base_url=$(get_base_url)
advance_host

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
base_url=$(get_base_url)
advance_host

queue_check_code=$(curl -s -u "$rabbitmq_user:$rabbitmq_pass" \
  -w "%{http_code}" \
  -o /dev/null \
  "$base_url/queues/$vhost/$queue_name")

if (( queue_check_code == 200 ))
then
  log_info "Queue exists, purging..."
  declare -i purge_code
  base_url=$(get_base_url)
  advance_host

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
  base_url=$(get_base_url)
  advance_host

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
base_url=$(get_base_url)
advance_host

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
declare -ri wait_time=$((max_delay_used + 5))
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
base_url=$(get_base_url)
advance_host

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

  if (( verbosity >= 1 ))
  then
    printf "\nMessage details:\n"
    jq '.' <<< "$response"
  fi

  printf "\n%b==========================================\n" "$green"
  printf "TEST PASSED\n"
  printf "==========================================%b\n" "$nc"
else
  log_error "Expected $num_messages messages, received $message_count"

  if (( verbosity >= 1 ))
  then
    printf "\nResponse:\n"
    jq '.' <<< "$response"
  fi

  printf "\n%b==========================================\n" "$red"
  printf "TEST FAILED\n"
  printf "==========================================%b\n" "$nc"
  exit 1
fi
