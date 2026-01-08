/**
 * Kafka Helper Module
 * Provides shared Kafka client, producer, and consumer factory functions
 * Loads configuration from config/agent-config.json
 */

import { Kafka } from 'kafkajs'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

// Load configuration
const configPath = path.join(__dirname, '../config/agent-config.json')
const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'))

// Create Kafka client
export const kafka = new Kafka(config.kafka)

// Factory functions
export const createProducer = () => kafka.producer()
export const createConsumer = (groupId) => kafka.consumer({ groupId })

// Export topic configuration
export const topics = config.topics

// Export full config for agent settings
export const agentConfig = config.agent
