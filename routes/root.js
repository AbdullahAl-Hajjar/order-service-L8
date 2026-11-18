'use strict'

module.exports = async function (fastify, opts) {
  fastify.post('/', async function (request, reply) {
    const msg = request.body
    try {
      await fastify.sendMessage(Buffer.from(JSON.stringify(msg)))
    } catch (err) {
      request.log.error({ err }, 'Order queue delivery failed')
      return reply.code(503).send({ error: 'Order queue did not confirm delivery' })
    }
    return reply.code(201).send()
  })

  fastify.get('/health', async function (request, reply) {
    const appVersion = process.env.APP_VERSION || '0.1.0'
    return { status: 'ok', version: appVersion }
  })

  fastify.get('/hugs', async function (request, reply) {
    return { hugs: fastify.someSupport() }
  })
}
