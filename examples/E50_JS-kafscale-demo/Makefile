# E50 JS Agent KafScale Demo - Makefile
# This demo shows Kafka-based agent orchestration with JavaScript

.PHONY: help install setup topics clean run-agent run-panel run-consumer test-flow demo stop-all

# Default target: show help
help:
	@echo "E50 JavaScript Agent KafScale Demo"
	@echo "==================================="
	@echo ""
	@echo "Available targets:"
	@echo "  make install       - Install Node.js dependencies"
	@echo "  make setup         - Create required Kafka topics"
	@echo "  make topics        - List all agent topics"
	@echo "  make run-agent     - Start the agent service (terminal 1)"
	@echo "  make run-consumer  - Start response consumer (terminal 2)"
	@echo "  make run-panel     - Run interactive task panel (terminal 3)"
	@echo "  make test-flow     - Send a test task through the system"
	@echo "  make demo          - Run complete demo flow (requires Kafka running)"
	@echo "  make clean         - Delete all topics and reset state"
	@echo "  make stop-all      - Stop all running services"
	@echo ""
	@echo "Quick Start:"
	@echo "  1. make install    # Install dependencies"
	@echo "  2. make setup      # Create topics"
	@echo "  3. Terminal 1: make run-agent"
	@echo "  4. Terminal 2: make run-consumer"
	@echo "  5. Terminal 3: make run-panel"
	@echo ""
	@echo "Prerequisites:"
	@echo "  - Kafka or KafScale running on localhost:9092"
	@echo "  - Node.js 18+ installed"

# Install Node.js dependencies
install:
	@echo "Installing Node.js dependencies..."
	npm install

# Create Kafka topics required for the demo
setup: topics-create
	@echo "✓ Demo setup complete"
	@echo ""
	@echo "Next steps:"
	@echo "  Terminal 1: make run-agent"
	@echo "  Terminal 2: make run-consumer"
	@echo "  Terminal 3: make run-panel"

# Create all required topics
topics-create:
	@echo "Creating Kafka topics..."
	@kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic agent.requests \
		--partitions 3 \
		--replication-factor 1 \
		--if-not-exists || true
	@kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic agent.internal \
		--partitions 3 \
		--replication-factor 1 \
		--if-not-exists || true
	@kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic agent.responses \
		--partitions 3 \
		--replication-factor 1 \
		--if-not-exists || true
	@echo "✓ Topics created"

# List all agent topics
topics:
	@echo "Agent topics:"
	@kafka-topics --list \
		--bootstrap-server localhost:9092 | grep "^agent\."

# Run the agent service (consume requests, produce responses)
run-agent:
	@echo "Starting agent service..."
	@echo "Listening on: agent.requests"
	@echo "Publishing to: agent.responses"
	node src/agent.js

# Run the response consumer (human panel view)
run-consumer:
	@echo "Starting response consumer..."
	@echo "Listening on: agent.responses"
	node scripts/consume-response.js

# Run the interactive task submission panel
run-panel:
	@echo "Interactive Task Panel"
	@echo "====================="
	node src/panel.js

# Send a test task through the system
test-flow:
	@echo "Sending test task..."
	@echo '{"task":"Explain Kafka topics","spec":"Brief, 2 sentences","context":"For beginners","correlationId":"test-'$$(date +%s)'","createdAt":"'$$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}' | \
	kafka-console-producer \
		--bootstrap-server localhost:9092 \
		--topic agent.requests
	@echo "✓ Test task sent to agent.requests"
	@echo "Check the consumer output for the response"

# Run complete demo flow (for automated testing)
demo: check-kafka
	@echo "Running automated demo flow..."
	@echo ""
	@echo "Step 1: Creating topics..."
	@make topics-create
	@echo ""
	@echo "Step 2: Sending test task..."
	@make test-flow
	@echo ""
	@echo "Demo setup complete!"
	@echo "Start agent and consumer in separate terminals to see the flow"

# Check if Kafka is running
check-kafka:
	@echo "Checking Kafka connectivity..."
	@kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 || \
		(echo "❌ Cannot connect to Kafka at localhost:9092" && \
		 echo "Please start Kafka or KafScale first" && exit 1)
	@echo "✓ Kafka is running"

# Clean up: delete all topics and reset
clean:
	@echo "Cleaning up demo environment..."
	@kafka-topics --delete \
		--bootstrap-server localhost:9092 \
		--topic agent.requests 2>/dev/null || true
	@kafka-topics --delete \
		--bootstrap-server localhost:9092 \
		--topic agent.internal 2>/dev/null || true
	@kafka-topics --delete \
		--bootstrap-server localhost:9092 \
		--topic agent.responses 2>/dev/null || true
	@echo "✓ Topics deleted"
	@echo "Run 'make setup' to recreate"

# Stop all running services (pkill approach)
stop-all:
	@echo "Stopping agent services..."
	@pkill -f "node src/agent.js" || echo "No agent running"
	@pkill -f "node scripts/consume-response.js" || echo "No consumer running"
	@echo "✓ Services stopped"

# Development targets
dev-check:
	@echo "Checking demo configuration..."
	@test -f config/agent-config.json || (echo "❌ Missing config/agent-config.json" && exit 1)
	@test -f src/kafka.js || (echo "❌ Missing src/kafka.js" && exit 1)
	@test -f src/agent.js || (echo "❌ Missing src/agent.js" && exit 1)
	@test -f src/panel.js || (echo "❌ Missing src/panel.js" && exit 1)
	@echo "✓ All required files present"

# Show topic contents (for debugging)
peek-requests:
	@echo "Recent messages in agent.requests:"
	@kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic agent.requests \
		--from-beginning \
		--max-messages 10 \
		--timeout-ms 5000 2>/dev/null || echo "No messages yet"

peek-responses:
	@echo "Recent messages in agent.responses:"
	@kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic agent.responses \
		--from-beginning \
		--max-messages 10 \
		--timeout-ms 5000 2>/dev/null || echo "No messages yet"

# Show demo architecture
arch:
	@echo "Demo Architecture"
	@echo "================="
	@echo ""
	@echo "┌──────────────┐"
	@echo "│ Human Panel  │  (make run-panel)"
	@echo "│ (CLI / UI)   │"
	@echo "└──────┬───────┘"
	@echo "       │ produce"
	@echo "       ▼"
	@echo "┌──────────────────────┐"
	@echo "│ agent.requests       │  (Kafka topic)"
	@echo "│ (Kafka topic)        │"
	@echo "└──────┬───────────────┘"
	@echo "       │ consume"
	@echo "       ▼"
	@echo "┌──────────────────────┐"
	@echo "│ JS Agent Service     │  (make run-agent)"
	@echo "│ - loads config       │"
	@echo "│ - builds prompt      │"
	@echo "│ - calls LLM (stub)   │"
	@echo "└──────┬───────────────┘"
	@echo "       │ produce"
	@echo "       ▼"
	@echo "┌──────────────────────┐"
	@echo "│ agent.responses      │  (Kafka topic)"
	@echo "│ (Kafka topic)        │"
	@echo "└──────┬───────────────┘"
	@echo "       │ consume"
	@echo "       ▼"
	@echo "┌──────────────┐"
	@echo "│ Human Panel  │  (make run-consumer)"
	@echo "│ shows result │"
	@echo "└──────────────┘"
	@echo ""
