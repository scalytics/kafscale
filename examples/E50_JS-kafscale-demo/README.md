# E50: JavaScript Agent Simulation with Kafka / KafScale

A demonstration of **queue-driven agent architecture** using Kafka topics for orchestration. This example shows how Kafka (and KafScale) serves as the backbone for stateless agent workflows, similar to patterns used in WritingOS and multi-step AI systems.

## 🎯 What This Demo Shows

- **JavaScript (Node.js)** client using Kafka protocol directly via KafkaJS
- **Standard file-based configuration** (all settings in one place)
- **Kafka topics as agent queues** for task distribution
- **Stateless agent loop**: consume task → build prompt → call LLM → produce response
- **Human panel interface** (CLI) for task submission and response monitoring

## 🏗️ Architecture

```
┌──────────────┐
│ Human Panel  │  (make run-panel)
│ (CLI / UI)   │
└──────┬───────┘
       │ produce
       ▼
┌──────────────────────┐
│ agent.requests       │  (Kafka topic)
│ (Kafka topic)        │
└──────┬───────────────┘
       │ consume
       ▼
┌──────────────────────┐
│ JS Agent Service     │  (make run-agent)
│ - loads config       │
│ - builds prompt      │
│ - calls LLM (stub)   │
└──────┬───────────────┘
       │ produce
       ▼
┌──────────────────────┐
│ agent.responses      │  (Kafka topic)
│ (Kafka topic)        │
└──────┬───────────────┘
       │ consume
       ▼
┌──────────────┐
│ Human Panel  │  (make run-consumer)
│ shows result │
└──────────────┘
```

## 📁 Project Structure

```
E50_JS-kafscale-demo/
├── README.md                  # This file
├── Makefile                   # Build and run automation
├── package.json               # Node.js dependencies
├── config/
│   └── agent-config.json     # Central configuration (Kafka, topics, agent)
├── src/
│   ├── kafka.js              # Shared Kafka client setup
│   ├── agent.js              # Agent service (main worker)
│   ├── llm.js                # LLM stub (replace with real provider)
│   ├── panel.js              # Interactive task submission CLI
│   └── types.js              # Message format definitions
└── scripts/
    └── consume-response.js   # Response monitoring script
```

## 🚀 Quick Start

### Prerequisites

- **Kafka or KafScale** running on `localhost:9092`
- **Node.js 18+** installed

### 1. Install Dependencies

```bash
make install
# or: npm install
```

### 2. Create Kafka Topics

```bash
make setup
```

This creates three topics:
- `agent.requests` - Tasks from human → agent
- `agent.internal` - (Optional) Agent scratchpad/logs
- `agent.responses` - Results from agent → human

### 3. Run the Demo (3 Terminals)

**Terminal 1 - Start Agent Service:**
```bash
make run-agent
# or: node src/agent.js
```

**Terminal 2 - Start Response Consumer:**
```bash
make run-consumer
# or: node scripts/consume-response.js
```

**Terminal 3 - Submit Tasks:**
```bash
make run-panel
# or: node src/panel.js
```

## 📋 Makefile Commands

| Command | Description |
|---------|-------------|
| `make help` | Show all available commands |
| `make install` | Install Node.js dependencies |
| `make setup` | Create required Kafka topics |
| `make run-agent` | Start agent service (terminal 1) |
| `make run-consumer` | Start response consumer (terminal 2) |
| `make run-panel` | Run interactive task panel (terminal 3) |
| `make test-flow` | Send test message through system |
| `make topics` | List all agent topics |
| `make peek-requests` | View messages in requests topic |
| `make peek-responses` | View messages in responses topic |
| `make clean` | Delete all topics and reset |
| `make arch` | Display architecture diagram |

## ⚙️ Configuration

All configuration is in `config/agent-config.json`:

```json
{
  "kafka": {
    "clientId": "js-agent-demo",
    "brokers": ["localhost:9092"]
  },
  "topics": {
    "requests": "agent.requests",
    "internal": "agent.internal",
    "responses": "agent.responses"
  },
  "agent": {
    "systemPrompt": "You are a helpful technical writing agent.",
    "model": "stub-llm",
    "temperature": 0.3
  }
}
```

**To use with KafScale:** No changes needed! KafScale is Kafka-compatible.

**To use with remote Kafka:** Change `brokers` to your Kafka URL.

## 📨 Message Format

### Agent Request
```json
{
  "correlationId": "uuid-v4",
  "task": "Write a technical blog post",
  "spec": "800 words, markdown format",
  "context": "Audience: developers learning Kafka",
  "createdAt": "2026-01-08T12:00:00.000Z"
}
```

### Agent Response
```json
{
  "correlationId": "uuid-v4",
  "result": "LLM-generated response here...",
  "finishedAt": "2026-01-08T12:00:05.000Z"
}
```

## 🔧 Replacing the LLM Stub

The demo uses a stubbed LLM in `src/llm.js`. To integrate a real LLM:

### Option 1: Anthropic Claude
```bash
npm install @anthropic-ai/sdk
```

Update `src/llm.js`:
```javascript
import Anthropic from '@anthropic-ai/sdk'

const client = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY
})

export async function callLLM(prompt) {
  const response = await client.messages.create({
    model: 'claude-3-5-sonnet-20241022',
    max_tokens: 1024,
    messages: [{ role: 'user', content: prompt }]
  })
  return response.content[0].text
}
```

### Option 2: OpenAI
```bash
npm install openai
```

Update `src/llm.js`:
```javascript
import OpenAI from 'openai'

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
})

export async function callLLM(prompt) {
  const response = await client.chat.completions.create({
    model: 'gpt-4',
    messages: [{ role: 'user', content: prompt }]
  })
  return response.choices[0].message.content
}
```

## 🎓 Why This Example Is Powerful

This demo illustrates:

1. **Kafka as Agent Orchestration Fabric**
   - Decouples task submission from execution
   - Natural load balancing (multiple agents can consume same topic)
   - Built-in durability and replay capability

2. **Clean Separation of Concerns**
   - Human input layer (panel)
   - Agent logic layer (worker)
   - LLM execution layer (pluggable)

3. **Stateless Agent Design**
   - Agents emerge naturally from queue consumption
   - Horizontal scaling by adding more consumers
   - No shared state or coordination needed

4. **Why Kafka-Compatible Systems (KafScale) Excel Here**
   - Agent swarms
   - WritingOS workflows
   - Multi-step AI pipelines
   - Reliable message delivery
   - Event sourcing for audit trails

## 🚀 Natural Extensions

Ready to take this further? Try:

- [ ] Replace LLM stub with real API (Anthropic, OpenAI, etc.)
- [ ] Add `agent.internal` topic for agent scratchpad/thinking logs
- [ ] Implement retry logic with dead-letter topic
- [ ] Build a web UI instead of CLI (React + WebSockets)
- [ ] Add multiple agents consuming from same topic (load balancing)
- [ ] Integrate with Claims Registry for document tracking
- [ ] Add Semantica-powered agent memory
- [ ] Implement agent chains (output → next agent's input)

## 🐛 Troubleshooting

**Cannot connect to Kafka:**
```bash
# Check if Kafka/KafScale is running
make check-kafka

# Start KafScale (if installed)
kafscale start
```

**Topics not created:**
```bash
# Manually create topics
make setup

# List topics to verify
make topics
```

**Agent not processing messages:**
```bash
# Check agent is running
make run-agent

# Send test message
make test-flow

# Peek at topics to debug
make peek-requests
make peek-responses
```

## 📚 Learn More

- [KafScale Documentation](../../README.md)
- [KafkaJS Documentation](https://kafka.js.org/)
- [Kafka Concepts](https://kafka.apache.org/documentation/)

## 📄 License

MIT
