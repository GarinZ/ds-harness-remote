/**
 * Result-only projection for Remote sessions.
 *
 * A phone that only wants the outcome of a Remote turn does not need every
 * streamed token, tool payload or subagent frame. This projection keeps the
 * durable skeleton (turn lifecycle, user/assistant text, approvals, errors)
 * and replaces each run of dropped process records with one small durable
 * marker. The marker's seq is the last dropped seq, so the follow stream stays
 * seq-contiguous and the client's gap/resync logic is unchanged.
 *
 * The projection is opt-in per request: the Host advertises the capability
 * and the client puts `resultOnly: true` in the native request payload. The flag is
 * stripped before the payload reaches the Harness gateway, whose strict codecs
 * reject unknown fields.
 */

export const RESULT_ONLY_CAPABILITY = 'remote.result.only.v1'

/** Durable event emitted in place of a dropped run. */
export const RESULT_ONLY_MARKER_TYPE = 'log/omitted'

/** Endpoints whose historical reads are filtered in result-only mode. */
export const RESULT_ONLY_HISTORY_ENDPOINTS = new Set([
  'session/events',
  'session/page',
  'session/history',
  'subagent/history',
])

/**
 * Whole families that are process detail, not outcome. Kept generic so a new
 * tool/subagent/job record type does not silently leak into result-only mode.
 */
const DROPPED_PREFIXES = ['tool/', 'step/', 'subagent/', 'job/', 'session-log-deepseek/', 'request/']
const DROPPED_TYPES = new Set(['assistant/attempt', 'todo/write', 'llm/retry', 'llm/retry-started'])

type RecordValue = Record<string, unknown>

interface OmittedRun {
  fromSeq: number
  seq: number
  count: number
  bytes: number
  kinds: string[]
  tools: string[]
  turn?: number
  step?: number
  time?: number
}

const isRecord = (value: unknown): value is RecordValue =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

/**
 * Remove the per-request flag from every shape the Client can send, then
 * report whether result-only was requested. The flag may sit at the payload
 * root, in `request`, or in `args.request` (the native gateway shape).
 */
export function takeResultOnly(payload: unknown): { payload: unknown; resultOnly: boolean } {
  if (!isRecord(payload)) return { payload, resultOnly: false }
  let resultOnly = false
  let changed = false
  const strip = (value: unknown): unknown => {
    if (!isRecord(value) || !Object.hasOwn(value, 'resultOnly')) return value
    if (value.resultOnly === true) resultOnly = true
    const next = { ...value }
    delete next.resultOnly
    changed = true
    return next
  }

  let next = strip(payload) as RecordValue
  let nextArgs = strip(next.args)
  let argsChanged = nextArgs !== next.args
  if (isRecord(nextArgs)) {
    let inner = nextArgs
    // Only touch keys that are present: adding `_request: undefined` makes the
    // Harness gateway reject the payload as an unexpected field.
    for (const key of ['request', '_request'] as const) {
      if (!Object.hasOwn(inner, key)) continue
      const projected = strip(inner[key])
      if (projected === inner[key]) continue
      inner = { ...inner, [key]: projected }
      argsChanged = true
    }
    if (inner !== nextArgs) nextArgs = inner
  }
  if (argsChanged) next = { ...next, args: nextArgs }
  const request = strip(next.request)
  if (request !== next.request) next = { ...next, request }
  return { payload: changed ? next : payload, resultOnly }
}

/** Keep only the assistant's visible text blocks; stream chunks are process. */
function trimAssistantMessage(event: RecordValue): RecordValue | undefined {
  const data = isRecord(event.data) ? event.data : undefined
  const message = data !== undefined && isRecord(data.message) ? data.message : undefined
  if (data === undefined || message === undefined) return undefined
  const content = Array.isArray(message.content)
    ? message.content.filter(block => isRecord(block) && block.type === 'text' && typeof block.text === 'string' && block.text.trim().length > 0)
    : []
  const text = typeof message.text === 'string' && message.text.trim().length > 0 ? message.text : undefined
  if (content.length === 0 && text === undefined) return undefined
  const nextData = { ...data }
  delete nextData.stream
  return {
    ...event,
    data: {
      ...nextData,
      message: { ...message, ...(content.length === 0 ? {} : { content }) },
    },
  }
}

function toolNameOf(event: RecordValue): string | undefined {
  const data = isRecord(event.data) ? event.data : undefined
  if (data === undefined) return undefined
  for (const key of ['name', 'toolName', 'tool']) {
    const value = data[key]
    if (typeof value === 'string' && value.length > 0) return value
  }
  const call = isRecord(data.call) ? data.call : undefined
  return call !== undefined && typeof call.name === 'string' && call.name.length > 0 ? call.name : undefined
}

function encodedLength(value: unknown): number {
  try {
    return JSON.stringify(value)?.length ?? 0
  } catch {
    return 0
  }
}

/** Stateful projection for one follow stream, page or snapshot. */
export class ResultOnlyFilter {
  private run: OmittedRun | undefined

  /** Project one durable event; may return the marker before the kept event. */
  push(event: RecordValue): RecordValue[] {
    const type = typeof event.type === 'string' ? event.type : ''
    const seq = typeof event.seq === 'number' && Number.isFinite(event.seq) ? event.seq : undefined
    if (this.isDropped(type)) {
      if (seq !== undefined) this.omit(event, type, seq)
      return []
    }
    if (type === 'assistant/message') {
      const trimmed = trimAssistantMessage(event)
      if (trimmed === undefined) {
        if (seq !== undefined) this.omit(event, type, seq)
        return []
      }
      const marker = this.takeRun()
      return marker === undefined ? [trimmed] : [marker, trimmed]
    }
    const marker = this.takeRun()
    return marker === undefined ? [event] : [marker, event]
  }

  /** End the current run, if any, and return its marker. */
  flush(): RecordValue | undefined {
    return this.takeRun()
  }

  /** Project a follow snapshot in place; a trailing omission needs no marker. */
  snapshot(value: RecordValue): RecordValue {
    this.run = undefined
    const next = { ...value }
    if (Array.isArray(value.records)) next.records = this.filterEntries(value.records, false)
    if (isRecord(next.assistantStream)) {
      const assistantStream = { ...next.assistantStream }
      delete assistantStream.activeAttempt
      next.assistantStream = assistantStream
    }
    this.run = undefined
    return next
  }

  /** Project a records list; pages flush the run at the page edge. */
  entries(records: unknown[], flush = true): unknown[] {
    return this.filterEntries(records, flush)
  }

  /** Project a raw events list (session/events). */
  events(events: unknown[], flush = true): unknown[] {
    const out: unknown[] = []
    for (const event of events) {
      if (!isRecord(event)) {
        out.push(event)
        continue
      }
      out.push(...this.push(event))
    }
    if (flush) {
      const marker = this.takeRun()
      if (marker !== undefined) out.push(marker)
    }
    return out
  }

  private filterEntries(records: unknown[], flush: boolean): unknown[] {
    const out: unknown[] = []
    for (const record of records) {
      if (isRecord(record) && isRecord(record.event)) {
        for (const event of this.push(record.event)) out.push({ ...record, event })
      } else if (isRecord(record)) {
        for (const event of this.push(record)) out.push(event)
      } else {
        out.push(record)
      }
    }
    if (flush) {
      const marker = this.takeRun()
      if (marker !== undefined) out.push({ type: 'event', event: marker })
    }
    return out
  }

  private isDropped(type: string): boolean {
    return DROPPED_TYPES.has(type) || DROPPED_PREFIXES.some(prefix => type.startsWith(prefix))
  }

  private omit(event: RecordValue, type: string, seq: number): void {
    const data = isRecord(event.data) ? event.data : undefined
    const run = this.run ?? {
      fromSeq: seq,
      seq,
      count: 0,
      bytes: 0,
      kinds: [],
      tools: [],
    }
    run.fromSeq = Math.min(run.fromSeq, seq)
    run.seq = Math.max(run.seq, seq)
    run.count += 1
    run.bytes += encodedLength(event)
    if (!run.kinds.includes(type) && run.kinds.length < 8) run.kinds.push(type)
    const tool = toolNameOf(event)
    if (tool !== undefined && !run.tools.includes(tool) && run.tools.length < 8) run.tools.push(tool)
    if (data !== undefined && typeof data.turn === 'number') run.turn = data.turn
    if (data !== undefined && typeof data.step === 'number') run.step = data.step
    if (typeof event.time === 'number') run.time = event.time
    this.run = run
  }

  private takeRun(): RecordValue | undefined {
    const run = this.run
    if (run === undefined) return undefined
    this.run = undefined
    return {
      type: RESULT_ONLY_MARKER_TYPE,
      seq: run.seq,
      time: run.time ?? 0,
      data: {
        fromSeq: run.fromSeq,
        count: run.count,
        bytes: run.bytes,
        kinds: run.kinds,
        ...(run.tools.length === 0 ? {} : { tools: run.tools }),
        ...(run.turn === undefined ? {} : { turn: run.turn }),
        ...(run.step === undefined ? {} : { step: run.step }),
      },
    }
  }
}

/** Project one history-style RPC result. Non-history endpoints pass through. */
export function filterResultOnly<T>(endpoint: string, result: T): T {
  if (!RESULT_ONLY_HISTORY_ENDPOINTS.has(endpoint)) return result
  if (!isRecord(result) || result.ok !== true || !isRecord(result.value)) return result
  const value = result.value
  if (endpoint === 'session/events' && Array.isArray(value.events)) {
    return { ...result, value: { ...value, events: new ResultOnlyFilter().events(value.events) } } as T
  }
  if (Array.isArray(value.records)) {
    return { ...result, value: { ...value, records: new ResultOnlyFilter().entries(value.records) } } as T
  }
  return result
}

/**
 * Project a session/follow stream. Assistant stream frames are dropped whole;
 * durable events keep their seq contract through markers; a run still open
 * when the stream closes is flushed as the final event.
 */
export async function* resultOnlyStream(source: AsyncIterable<unknown>, signal: AbortSignal): AsyncGenerator<unknown> {
  const filter = new ResultOnlyFilter()
  for await (const value of source) {
    if (signal.aborted) return
    if (!isRecord(value)) {
      yield value
      continue
    }
    if (value.type === 'snapshot') {
      yield filter.snapshot(value)
      continue
    }
    if (value.type === 'event') {
      if (!isRecord(value.event)) {
        yield value
        continue
      }
      for (const event of filter.push(value.event)) yield { ...value, event }
      continue
    }
    if (value.type === 'assistant-stream') continue
    yield value
  }
  if (signal.aborted) return
  const marker = filter.flush()
  if (marker !== undefined) yield { type: 'event', event: marker }
}
