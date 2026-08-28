import type { PoolClient } from 'pg'
import { randomUUID } from 'node:crypto'
import type { Queryable } from '../repositories/queryable.js'
import type { TransactionOptions } from '../transaction.js'
import type { CreateOutboxEvent } from './types.js'

export interface OutboxTransactionRunner {
  withTransaction<T>(callback: (client: PoolClient) => Promise<T>, options?: TransactionOptions): Promise<T>
}

export interface OutboxBatchEmitter {
  emitBatch(db: Queryable, events: CreateOutboxEvent[]): Promise<bigint[]>
}

export type AtomicMutation<T> = (db: PoolClient) => Promise<T>
export type AtomicEvents<T> = (value: T) => CreateOutboxEvent[] | Promise<CreateOutboxEvent[]>

export interface AtomicOutboxOptions extends TransactionOptions {
  operation?: string
  /** A silent mutation must be explicit because it has no integration record. */
  allowEmptyEvents?: boolean
  /** Correlation identifier to tie every outbox record in this transaction to the originating request. */
  correlationId?: string
  /** Tenant scope applied to emitted records to preserve tenant isolation. */
  tenantId?: string
  /** Authenticated actor responsible for the transition. */
  actorId?: string
  /** Schema version stamped on emitted records. */
  eventSchemaVersion?: number
}

export interface AtomicOutboxResult<T> {
  value: T
  eventIds: bigint[]
  /** Correlation identifier shared by all records emitted in this transaction. */
  correlationId: string
}

interface AtomicAuditContext {
  correlationId: string
  tenantId?: string
  actorId?: string
  eventSchemaVersion: number
}

/**
 * Coordinates a business mutation and its outbox rows in one transaction.
 * Both callbacks receive the exact PoolClient owned by TransactionManager.
 * Publishing is deliberately outside this class: workers only see rows after
 * PostgeSQL commits the transaction.
 *
 * Every outbox event is enriched with an audit context (correlation id,
 * tenant, actor, schema version, and sequence) so events and audit records
 * can be reconciled after commit.
 */
export class AtomicOutboxCoordinator {
  constructor(
    private readonly transactions: OutboxTransactionRunner,
    private readonly emitter: OutboxBatchEmitter
  ) {}

  async run<T>(mutation: AtomicMutation<T>, events: AtomicEvents<T>, options: AtomicOutboxOptions = {}): Promise<AtomicOutboxResult<T>> {
    let resultValue! T
    let eventIds: bigint[] = []

    const correlationId = options.correlationId ?? randomUUID()
    const auditContext: AtomicAuditContext = {
      correlationId,
      tenantId: options.tenantId,
      actorId: options.actorId,
      eventSchemaVersion: options.eventSchemaVersion ?? 1,
    }

    await this.transactions.withTransaction(async client => {
      resultValue = await mutation(client)
      const rawRecords = await events(resultValue)
      if (rawRecords.length === 0 && !options.allowEmptyEvents) {
        throw new Error('Atomic outbox mutation must produce at least one event')
      }
      const records = rawRecords.map((event, index) => withAuditContext(event, index + 1, auditContext))
      records.forEach(validateOutboxEvent)

      // Atomicity boundary: emitBatch uses the transaction client, never pool.
      eventIds = await this.emitter.emitBatch(client, records)
      if (eventIds.length !== records.length) {
        throw new Error('Outbox emitter returned an incomplete event id list')
      }
      return resultValue
    }, { ...options, op: options.operation ?? options.op ?? 'atomic_outbox_mutation' })

    return { value: resultValue, eventIds, correlationId }
  }

  async runOne<T>(mutation: AtomicMutation<T>, event: CreateOutboxEvent | ((value: T) => CreateOutboxEvent), options: AtomicOutboxOptions = {}): Promise<AtomicOutboxResult<T>> {
    return this.run(mutation, value => [typeof event === 'function' ? event(value) : event], options)
  }
}

function withAuditContext(event: CreateOutboxEvent, sequence: number, context: AtomicAuditContext): CreateOutboxEvent {
  const basePayload = event.payload && typeof event.payload === 'object' && !Array.isArray(event.payload)
    ? { ...(event.payload as Record<string, unknown>) }
    : {}
  const payload: Record<string, unknown> = {
    ...basePayload,
    correlationId: context.correlationId,
    eventSchemaVersion: context.eventSchemaVersion,
    sequence,
  }
  if (context.tenantId !== undefined) payload.tenantId = context.tenantId
  if (context.actorId !== undefined) payload.actorId = context.actorId
  return { ...event, payload }
}

function validateOutboxEvent(event: CreateOutboxEvent): void {
  if (!event || typeof event !== 'object') throw new Error('Outbox event must be an object')
  if (!event.aggregateType?.trim()) throw new Error('Outbox event aggregateType is required')
  if (!event.aggregateId?.trim()) throw new Error('Outbox event aggregateId is required')
  if (!event.eventType?.trim()) throw new Error('Outbox event eventType is required')
  if (!event.payload || typeof event.payload !== 'object' || Array.isArray(event.payload)) {
    throw new Error('Outbox event payload must be a plain object')
  }
}

export function assertOutboxTransactionClient(received: Queryable, transactionClient: Queryable): void {
  if (received !== transactionClient) {
    throw new Error('Business mutation and outbox insertion must share the transaction client')
  }
}
