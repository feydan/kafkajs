const {
  createCluster,
  createModPartitioner,
  createTopic,
  newLogger,
  secureRandom,
  waitFor,
} = require('testHelpers')

const createAdmin = require('../index')
const createConsumer = require('../../consumer')
const createProducer = require('../../producer')

describe('Admin', () => {
  let topic, groupId, admin, cluster

  beforeEach(async () => {
    topic = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`
    cluster = createCluster()
    admin = createAdmin({ cluster, logger: newLogger() })
    await createTopic({ topic })
  })

  afterEach(async () => {
    await admin.disconnect()
  })

  describe('resetOffsetsByTimestamp', () => {
    test('throws errors with invalid parameters', async () => {
      await expect(admin.resetOffsetsByTimestamp({ groupId: null })).rejects.toHaveProperty(
        'message',
        'Invalid groupId null'
      )
      await expect(
        admin.resetOffsetsByTimestamp({ groupId: 'groupId', topic: null })
      ).rejects.toHaveProperty('message', 'Invalid topic null')
      await expect(
        admin.resetOffsetsByTimestamp({ groupId: 'groupId', topic, timestamp: null })
      ).rejects.toHaveProperty('message', 'Invalid timestamp null')
    })

    test('works with empty topic', async () => {
      await admin.resetOffsetsByTimestamp({
        groupId,
        topic,
        timestamp: 0,
      })

      const offsets = await admin.fetchOffsets({ groupId, topic })

      expect(offsets).toEqual([{ partition: 0, offset: '0', metadata: null }])
    })

    describe('works with sample messages', () => {
      let consumer, producer

      const sendMessages = async (n, start) => {
        await producer.connect()

        const messages = Array(n)
          .fill()
          .map((v, i) => {
            return { key: `key-${start + i}`, value: `v-${start}` }
          })

        await producer.send({ acks: 1, topic, messages })
      }

      beforeEach(async () => {
        consumer = createConsumer({
          cluster,
          groupId,
          maxWaitTimeInMs: 1,
          maxBytesPerPartition: 180,
          logger: newLogger(),
        })
        producer = createProducer({
          cluster: createCluster(),
          createPartitioner: createModPartitioner,
          logger: newLogger(),
        })
      })

      afterEach(async () => {
        await consumer.disconnect()
        await producer.disconnect()
      })

      it('resets to the timestamp', async () => {
        await sendMessages(2, 100)
        const timestamp = Date.now()
        await sendMessages(2, 200)
        expect(await admin.fetchOffsets({ groupId, topic })).toEqual([
          { partition: 0, offset: '-1', metadata: null },
        ])
        await admin.resetOffsetsByTimestamp({
          groupId,
          topic,
          timestamp,
        })
        expect(await admin.fetchOffsets({ groupId, topic })).toEqual([
          { partition: 0, offset: '2', metadata: null },
        ])
        await consumer.connect()
        await consumer.subscribe({ topic })
        let message
        let count = 0
        consumer.run({
          autoCommitInterval: 0,
          autoCommitThreshold: 0,
          eachMessage: async event => {
            if (!message) message = event.message
            ++count
          },
        })
        await waitFor(() => message)
        await expect(message.key.toString()).toEqual('key-200')
        await waitFor(() => count >= 2)
        await consumer.commitOffsets()
        expect(await admin.fetchOffsets({ groupId, topic })).toEqual([
          { partition: 0, offset: '4', metadata: null },
        ])
      })

      it('resets to the future timestamp', async () => {
        await sendMessages(3, 100)
        const timestamp = Date.now()
        expect(await admin.fetchOffsets({ groupId, topic })).toEqual([
          { partition: 0, offset: '-1', metadata: null },
        ])
        await admin.resetOffsetsByTimestamp({
          groupId,
          topic,
          timestamp,
        })
        expect(await admin.fetchOffsets({ groupId, topic })).toEqual([
          { partition: 0, offset: '3', metadata: null },
        ])
      })
    })
  })
})
