const createAdmin = require('../index')
const createConsumer = require('../../consumer')
const createProducer = require('../../producer')

const {
  createCluster,
  newLogger,
  createTopic,
  secureRandom,
  createModPartitioner,
  waitForConsumerToJoinGroup,
  waitForMessages,
} = require('testHelpers')

describe('Admin', () => {
  let admin, topicName, groupIds, cluster, consumers, producer

  beforeAll(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupIds = [
      `consumer-group-id-${secureRandom()}`,
      `consumer-group-id-${secureRandom()}`,
      `consumer-group-id-${secureRandom()}`,
    ]

    cluster = createCluster()
    admin = createAdmin({ cluster: cluster, logger: newLogger() })
    consumers = groupIds.map(groupId =>
      createConsumer({
        cluster,
        groupId,
        maxWaitTimeInMs: 100,
        logger: newLogger(),
      })
    )

    producer = createProducer({
      cluster,
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    })

    await Promise.all([
      admin.connect(),
      producer.connect(),
      ...[consumers.map(consumer => consumer.connect())],
    ])

    await createTopic({ topic: topicName })

    const messagesConsumed = []
    await Promise.all(
      consumers.map(async consumer => {
        await consumer.subscribe({ topic: topicName, fromBeginning: true })
        consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
        await waitForConsumerToJoinGroup(consumer)
      })
    )

    const messages = Array(1)
      .fill()
      .map(() => {
        const value = secureRandom()
        return { key: `key-${value}`, value: `value-${value}` }
      })

    await producer.send({ acks: 1, topic: topicName, messages })
    await waitForMessages(messagesConsumed, { number: messages.length * consumers.length })
  })

  afterAll(async () => {
    admin && (await admin.disconnect())
    consumers && (await Promise.all(consumers.map(consumer => consumer.disconnect())))
    producer && (await producer.disconnect())
  })

  describe('describeGroups', () => {
    test('returns describe group response for multiple groups', async () => {
      const describeGroupsResponse = await admin.describeGroups(groupIds)

      expect(describeGroupsResponse.groups).toIncludeSameMembers(
        groupIds.map(groupId => ({
          errorCode: 0,
          groupId,
          members: [
            {
              clientHost: expect.any(String),
              clientId: expect.any(String),
              memberId: expect.any(String),
              memberAssignment: expect.anything(),
              memberMetadata: expect.anything(),
            },
          ],
          protocol: 'RoundRobinAssigner',
          protocolType: 'consumer',
          state: 'Stable',
        }))
      )
    })

    test('returns a response for groups that do not exist', async () => {
      const groupId = `non-existent-consumer-group-id-${secureRandom()}`
      const response = await admin.describeGroups([groupId])

      expect(response).toEqual({
        groups: [
          {
            errorCode: 0,
            groupId,
            members: [],
            protocol: '',
            protocolType: '',
            state: 'Dead',
          },
        ],
      })
    })
  })
})
