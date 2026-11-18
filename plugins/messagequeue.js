'use strict'

const fp = require('fastify-plugin')
const rhea = require('rhea')

async function publishToRabbitMQ(body) {
  const container = rhea.create_container()
  const retryLimit = Number(process.env.ORDER_QUEUE_RECONNECT_LIMIT || 0)
  const connection = container.connect({
    hostname: process.env.ORDER_QUEUE_HOSTNAME,
    host: process.env.ORDER_QUEUE_HOSTNAME,
    port: process.env.ORDER_QUEUE_PORT,
    username: process.env.ORDER_QUEUE_USERNAME,
    password: process.env.ORDER_QUEUE_PASSWORD,
    transport: process.env.ORDER_QUEUE_TRANSPORT || 'tcp',
    reconnect: retryLimit > 0,
    reconnect_limit: retryLimit
  })
  let timer
  try {
    await new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error('Broker acknowledgement timed out')), 15000)
      container.once('sendable', ({ sender }) => {
        // Once sent, an interrupted delivery has an unknown outcome.
        connection.set_reconnect(false)
        sender.send({ durable: true, body: rhea.message.data_section(body) })
      })
      container.once('accepted', resolve)
      for (const event of ['rejected', 'released', 'modified', 'sender_error', 'session_error', 'connection_error', 'sender_close', 'connection_close']) {
        container.on(event, () => reject(new Error('Broker did not accept the order: ' + event)))
      }
      container.on('disconnected', ({ reconnecting }) => {
        if (!reconnecting) reject(new Error('Broker disconnected before acknowledgement'))
      })
      connection.open_sender(process.env.ORDER_QUEUE_NAME)
    })
  } finally {
    clearTimeout(timer)
    container.removeAllListeners('sendable')
    clearTimeout(connection.scheduled_reconnect)
    connection.set_reconnect(false)
    connection.close()
    // A peer that never opened AMQP cannot complete a graceful close.
    if (!connection.is_remote_open() && connection.socket) connection.socket.destroy()
  }
}

module.exports = fp(async function (fastify) {
  fastify.decorate('sendMessage', async function (body) {
    if (process.env.ORDER_QUEUE_USERNAME && process.env.ORDER_QUEUE_PASSWORD) {
      return publishToRabbitMQ(body)
    }
    if (process.env.USE_WORKLOAD_IDENTITY_AUTH !== 'true') {
      throw new Error('Order queue credentials are missing')
    }
    const { ServiceBusClient } = require('@azure/service-bus')
    const { DefaultAzureCredential } = require('@azure/identity')
    const host = process.env.ORDER_QUEUE_HOSTNAME || process.env.AZURE_SERVICEBUS_FULLYQUALIFIEDNAMESPACE
    const client = new ServiceBusClient(host, new DefaultAzureCredential())
    try {
      const sender = client.createSender(process.env.ORDER_QUEUE_NAME)
      await sender.sendMessages({ body: body.toString() })
    } finally {
      await client.close()
    }
  })
})
