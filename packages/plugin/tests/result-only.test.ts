import { describe, expect, it, vi } from 'vitest'
import { createRpcRequest } from '@dsh-remote/protocol'
import {
  filterResultOnly,
  RESULT_ONLY_CAPABILITY,
  RESULT_ONLY_MARKER_TYPE,
  ResultOnlyFilter,
  resultOnlyStream,
  takeResultOnly,
} from '../src/result-only.js'
import { RpcRouter } from '../src/rpc-router.js'

const assistant = (seq: number, content: unknown[], extra: Record<string, unknown> = {}) => ({
  type: 'assistant/message',
  seq,
  time: seq * 10,
  data: {
    turn: 1,
    step: 2,
    stream: [{ dt: 10, texts: ['secret token'] }],
    message: { role: 'assistant', content },
    ...extra,
  },
})

const event = (seq: number, type: string, data: Record<string, unknown> = {}) => ({ type, seq, time: seq * 10, data })

const page = (records: unknown[]) => ({ cursor: 9, throughSeq: 9, hasMore: true, activeTurnId: 'turn-1', records })

describe('result-only request flag', () => {
  it('strips the flag from root, request and native args without mutating the caller', () => {
    const native = { args: { request: { address: 'session-1', resultOnly: true } }, request: { resultOnly: true } }
    const copy = structuredClone(native)
    expect(takeResultOnly(native)).toEqual({
      payload: { args: { request: { address: 'session-1' } }, request: {} },
      resultOnly: true,
    })
    expect(native).toEqual(copy)
    expect(takeResultOnly({ args: { _request: { resultOnly: false } } })).toEqual({ payload: { args: { _request: {} } }, resultOnly: false })
    expect(takeResultOnly({ request: { address: 'session-1' } })).toEqual({ payload: { request: { address: 'session-1' } }, resultOnly: false })
  })

  it('does not add an absent legacy _request field when stripping args.request', () => {
    const payload = { args: { request: { address: 'session-1', resultOnly: true } } }
    const { payload: projected } = takeResultOnly(payload)
    const args = (projected as { args: Record<string, unknown> }).args
    expect(args).toEqual({ request: { address: 'session-1' } })
    expect(Object.hasOwn(args, '_request')).toBe(false)
  })
})

describe('ResultOnlyFilter', () => {
  it('keeps lifecycle and visible text, collapsing tool and step runs into markers', () => {
    const filter = new ResultOnlyFilter()
    const kept = [
      event(1, 'user/message', { content: [{ type: 'text', text: 'do it' }] }),
      event(2, 'tool/call', { name: 'Bash', arguments: '{"command":"ls"}' }),
      event(3, 'tool/result', { name: 'Bash', output: 'x'.repeat(1000) }),
      assistant(4, [{ type: 'reasoning', text: 'think' }, { type: 'text', text: 'done' }, { type: 'tool-call', toolName: 'Read' }]),
      assistant(5, [{ type: 'reasoning', text: 'only thinking' }]),
      event(6, 'step/start', { step: 2 }),
      event(7, 'turn/end', { reason: { kind: 'completed' } }),
    ].flatMap(value => filter.push(value))

    expect(kept.map(value => value.seq)).toEqual([1, 3, 4, 6, 7])
    expect(kept[1]).toMatchObject({ type: RESULT_ONLY_MARKER_TYPE, seq: 3, data: { fromSeq: 2, count: 2, kinds: ['tool/call', 'tool/result'], tools: ['Bash'] } })
    expect(kept[2]).toMatchObject({
      type: 'assistant/message',
      data: {
        message: { role: 'assistant', content: [{ type: 'text', text: 'done' }] },
      },
    })
    expect(kept[2]!.data).not.toHaveProperty('stream')
    expect(kept[3]).toMatchObject({ type: RESULT_ONLY_MARKER_TYPE, seq: 6, data: { fromSeq: 5, count: 2 } })
  })

  it('treats transient retries as process so they do not split an omission run', () => {
    const filter = new ResultOnlyFilter()
    const kept = [
      event(1, 'tool/call', { name: 'Bash', arguments: '{}' }),
      event(2, 'llm/retry', { attempt: 1 }),
      event(3, 'llm/retry-started', { attempt: 1 }),
      event(4, 'tool/result', { name: 'Bash', output: 'ok' }),
      event(5, 'turn/end', { reason: { kind: 'completed' } }),
    ].flatMap(value => filter.push(value))

    expect(kept.map(value => value.seq)).toEqual([4, 5])
    expect(kept[0]).toMatchObject({ type: RESULT_ONLY_MARKER_TYPE, seq: 4, data: { fromSeq: 1, count: 4 } })
  })

  it('drops assistant stream frames and trims snapshots without a trailing marker', () => {
    const filter = new ResultOnlyFilter()
    const snapshot = {
      type: 'snapshot',
      ...page([
        { type: 'event', event: event(1, 'user/message', { content: [] }) },
        { type: 'event', event: assistant(2, [{ type: 'reasoning', text: 'hidden' }]) },
        { type: 'event', event: assistant(3, [{ type: 'text', text: 'visible' }]) },
      ]),
      assistantStream: { revision: 7, activeAttempt: { turn: 1, stream: ['token'] } },
    }
    const projected = filter.snapshot(structuredClone(snapshot)) as {
      records: { event: { seq: number; type?: string; data?: Record<string, unknown> } }[]
      assistantStream: { revision: number }
    }
    expect(projected.records.map(row => row.event.seq)).toEqual([1, 2, 3])
    expect(projected.records[1]).toMatchObject({ event: { type: RESULT_ONLY_MARKER_TYPE, seq: 2, data: { fromSeq: 2, count: 1 } } })
    expect(projected.assistantStream).toEqual({ revision: 7 })
    expect(projected.records[2]!.event.data).not.toHaveProperty('stream')
    expect(filter.flush()).toBeUndefined()
  })
})

describe('filterResultOnly', () => {
  it('projects page records with wrapped markers and leaves cursors and other endpoints alone', () => {
    const value = page([
      { type: 'event', event: event(1, 'user/message') },
      { type: 'event', event: event(2, 'tool/call') },
      { type: 'event', event: event(3, 'tool/result') },
      { type: 'event', event: event(4, 'turn/end') },
    ])
    const original = structuredClone(value)
    const result = filterResultOnly('session/page', { ok: true, value }) as { ok: true; value: typeof value }
    expect(result.value).toMatchObject({ cursor: 9, throughSeq: 9, hasMore: true })
    expect(result.value.records).toHaveLength(3)
    expect(result.value.records[1]).toEqual({ type: 'event', event: { type: RESULT_ONLY_MARKER_TYPE, seq: 3, time: 30, data: { fromSeq: 2, count: 2, kinds: ['tool/call', 'tool/result'], bytes: expect.any(Number) } } })
    expect(value).toEqual(original)
    expect(filterResultOnly('session/list', { ok: true, value: original })).toEqual({ ok: true, value: original })
  })

  it('projects raw session/events lists', () => {
    const result = filterResultOnly('session/events', { ok: true, value: { events: [event(5, 'tool/call'), event(6, 'turn/end')] } }) as { value: { events: { type: string; seq: number }[] } }
    expect(result.value.events.map(item => item.type)).toEqual([RESULT_ONLY_MARKER_TYPE, 'turn/end'])
    expect(result.value.events[0]!.seq).toBe(5)
  })
})

describe('resultOnlyStream', () => {
  it('projects frames and flushes an open run at stream end', async () => {
    async function* source() {
      yield { type: 'snapshot', ...page([{ type: 'event', event: event(1, 'user/message') }]) }
      yield { type: 'assistant-stream', frame: { revision: 1, chunks: [] } }
      yield { type: 'event', event: event(2, 'tool/call') }
      yield { type: 'event', event: event(3, 'tool/result') }
    }
    const out: { type: string; event?: { type: string; seq: number } }[] = []
    for await (const frame of resultOnlyStream(source(), new AbortController().signal)) out.push(frame as never)
    expect(out.map(frame => frame.type)).toEqual(['snapshot', 'event'])
    expect(out[1]!.event).toMatchObject({ type: RESULT_ONLY_MARKER_TYPE, seq: 3, data: { fromSeq: 2, count: 2 } })
  })
})

it('applies result-only inside the bridge without forwarding the flag to the Gateway', async () => {
  const { HarnessRemoteBridge } = await import('../src/harness-remote-bridge.js')
  const source = page([
    { type: 'event', event: event(1, 'user/message') },
    { type: 'event', event: event(2, 'tool/call') },
    { type: 'event', event: assistant(3, [{ type: 'text', text: 'answer' }]) },
  ])
  const snapshot = { type: 'snapshot', ...source, assistantStream: { revision: 1, activeAttempt: { stream: ['token'] } } }
  const open = vi.fn(async () => (async function* () {
    yield snapshot
    yield { type: 'event', event: event(4, 'tool/call') }
    yield { type: 'event', event: event(5, 'turn/end') }
  })())
  const dispatch = vi.fn(async () => ({ ok: true, value: source }))
  const publish = vi.fn(async (_event: string, _data: unknown) => undefined)
  const gateway = { dispatch, open, failure: () => ({ code: 'FAILED', message: '', details: {} }), supportsCarrier: true }
  const bridge = new HarnessRemoteBridge(gateway as never, publish)

  expect(await bridge.call({ endpoint: 'session/page', payload: { args: { request: { resultOnly: true } } } })).toMatchObject({ ok: true })
  expect(dispatch).toHaveBeenCalledWith('session/page', { args: { request: {} } }, expect.anything())

  await bridge.openStream({ streamId: 'follow', endpoint: 'session/follow', payload: { args: { request: { address: 'session-1', resultOnly: true } } } })
  await vi.waitFor(() => expect(publish).toHaveBeenCalledWith('harness.remote.stream.closed', { streamId: 'follow', reason: 'completed' }))
  expect(open).toHaveBeenCalledWith('session/follow', { args: { request: { address: 'session-1' } } }, expect.anything())
  type PublishedFrame = { type?: string; records?: { event: { seq: number } }[]; assistantStream?: { revision?: number } }
  const frames = publish.mock.calls
    .filter(call => call[0] === 'harness.remote.frame')
    .map(call => (call[1] as { value?: unknown }).value as PublishedFrame)
  expect(frames[0]).toMatchObject({ type: 'snapshot', assistantStream: { revision: 1 } })
  expect(frames[0]!.records!.map(row => row.event.seq)).toEqual([1, 2, 3])
  expect(frames[1]).toMatchObject({ type: 'event', event: { type: RESULT_ONLY_MARKER_TYPE, seq: 4 } })
  expect(frames[2]).toMatchObject({ type: 'event', event: { type: 'turn/end', seq: 5 } })
  await bridge.closeAll()
})

describe('result-only capability', () => {
  it('is advertised only when the native Harness bridge is present', async () => {
    const capabilities = () => ['transport.relay', 'harness.remote.v1']
    const withBridge = new RpcRouter(undefined, undefined, undefined, {} as never, capabilities)
    const withoutBridge = new RpcRouter(undefined, undefined, undefined, undefined, capabilities)
    const describeRouter = async (router: RpcRouter) => {
      const response = await router.handle(createRpcRequest('harness.transport.describe', {}))
      return (response.payload as { result: { capabilities: string[] } }).result.capabilities
    }
    expect(await describeRouter(withBridge)).toContain(RESULT_ONLY_CAPABILITY)
    expect(await describeRouter(withoutBridge)).not.toContain(RESULT_ONLY_CAPABILITY)
  })
})
