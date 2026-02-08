const ConsumerGroup = require('../consumerGroup')
const { newLogger } = require('testHelpers')

describe('ConsumerGroup', () => {
  let consumerGroup

  beforeEach(() => {
    consumerGroup = new ConsumerGroup({
      logger: newLogger(),
      topics: ['topic1'],
      cluster: {},
    })
  })

  describe('uncommittedOffsets', () => {
    it("calls the offset manager's uncommittedOffsets", async () => {
      const mockOffsets = { topics: [] }
      consumerGroup.offsetManager = { uncommittedOffsets: jest.fn(() => mockOffsets) }

      expect(consumerGroup.uncommittedOffsets()).toStrictEqual(mockOffsets)
      expect(consumerGroup.offsetManager.uncommittedOffsets).toHaveBeenCalled()
    })
  })

  describe('commitOffsets', () => {
    it("calls the offset manager's commitOffsets", async () => {
      consumerGroup.offsetManager = { commitOffsets: jest.fn(() => Promise.resolve()) }

      const offsets = { topics: [{ partitions: [{ offset: '0', partition: 0 }] }] }
      await consumerGroup.commitOffsets(offsets)
      expect(consumerGroup.offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
      expect(consumerGroup.offsetManager.commitOffsets).toHaveBeenCalledWith(offsets)
    })
  })

  describe('groupInstanceId (static membership)', () => {
    it('stores groupInstanceId when provided', () => {
      const cg = new ConsumerGroup({
        logger: newLogger(),
        topics: ['topic1'],
        cluster: {},
        groupInstanceId: 'my-static-id',
      })
      expect(cg.groupInstanceId).toBe('my-static-id')
    })

    it('defaults groupInstanceId to null when not provided', () => {
      expect(consumerGroup.groupInstanceId).toBeNull()
    })

    describe('heartbeat', () => {
      it('includes groupInstanceId in heartbeat payload', async () => {
        const cg = new ConsumerGroup({
          logger: newLogger(),
          topics: ['topic1'],
          cluster: {},
          groupId: 'test-group',
          groupInstanceId: 'my-static-id',
        })

        cg.memberId = 'member-1'
        cg.generationId = 1
        cg.lastRequest = 0
        cg.coordinator = {
          heartbeat: jest.fn(() => Promise.resolve()),
        }
        cg.instrumentationEmitter = { emit: jest.fn() }

        await cg.heartbeat({ interval: 0 })

        expect(cg.coordinator.heartbeat).toHaveBeenCalledWith(
          expect.objectContaining({
            groupInstanceId: 'my-static-id',
          })
        )
      })
    })

    describe('leave', () => {
      it('skips leaveGroup when groupInstanceId is set', async () => {
        const cg = new ConsumerGroup({
          logger: newLogger(),
          topics: ['topic1'],
          cluster: {},
          groupId: 'test-group',
          groupInstanceId: 'my-static-id',
        })

        cg.memberId = 'member-1'
        cg.coordinator = {
          leaveGroup: jest.fn(() => Promise.resolve()),
        }

        await cg.leave()

        expect(cg.coordinator.leaveGroup).not.toHaveBeenCalled()
        expect(cg.memberId).toBeNull()
      })

      it('calls leaveGroup when groupInstanceId is not set', async () => {
        const cg = new ConsumerGroup({
          logger: newLogger(),
          topics: ['topic1'],
          cluster: {},
          groupId: 'test-group',
        })

        cg.memberId = 'member-1'
        cg.coordinator = {
          leaveGroup: jest.fn(() => Promise.resolve()),
        }

        await cg.leave()

        expect(cg.coordinator.leaveGroup).toHaveBeenCalledWith({
          groupId: 'test-group',
          memberId: 'member-1',
        })
        expect(cg.memberId).toBeNull()
      })
    })
  })
})
