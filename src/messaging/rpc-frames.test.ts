import { describe, expect, it } from '@effect/vitest';
import * as fc from 'effect/FastCheck';
import { Effect, Option } from 'effect';

import { decodeFromClientFrame, decodeFromServerFrame } from './rpc-frames';

const jsonRoundTrip = (frame: unknown): unknown =>
	JSON.parse(JSON.stringify(frame));

const requestFrame = fc.record({
	_tag: fc.constant('Request'),
	id: fc.string(),
	tag: fc.string(),
	payload: fc.jsonValue(),
	headers: fc.array(fc.tuple(fc.string(), fc.string())),
	traceId: fc.string(),
	spanId: fc.string(),
	sampled: fc.boolean(),
});

const ackFrame = fc.record({
	_tag: fc.constant('Ack'),
	requestId: fc.string(),
});

const interruptFrame = fc.record({
	_tag: fc.constant('Interrupt'),
	requestId: fc.string(),
});

const chunkFrame = fc.record({
	_tag: fc.constant('Chunk'),
	requestId: fc.string(),
	values: fc.array(fc.jsonValue(), { minLength: 1 }),
});

const exitFrame = fc.record({
	_tag: fc.constant('Exit'),
	requestId: fc.string(),
	exit: fc.record({
		_tag: fc.constant('Success'),
		value: fc.jsonValue(),
	}),
});

const clientSample = fc
	.oneof(requestFrame, ackFrame, interruptFrame)
	.map((frame) => ({ fromClient: true, frame }));

const serverSample = fc
	.oneof(chunkFrame, exitFrame)
	.map((frame) => ({ fromClient: false, frame }));

describe('rpc frames', () => {
	it.effect.prop(
		'rpc-shaped frames survive a json round trip',
		[fc.oneof(clientSample, serverSample)],
		([sample]) =>
			Effect.sync(() => {
				const parsed = jsonRoundTrip(sample.frame);
				if (sample.fromClient) {
					expect(Option.isSome(decodeFromClientFrame(parsed))).toBe(true);
					return;
				}
				expect(Option.isSome(decodeFromServerFrame(parsed))).toBe(true);
			}),
	);

	it.effect('rejects frames that are not a kit envelope', () =>
		Effect.sync(() => {
			const rejected: ReadonlyArray<unknown> = [
				{ _tag: 'Request', tag: 'Echo', payload: {}, headers: [] },
				{ _tag: 'Chunk', requestId: '1', values: [] },
				{ _tag: 'Nope' },
				{
					_tag: 'Request',
					id: '1',
					tag: 'Echo',
					payload: {},
					headers: ['nope'],
				},
				{
					_tag: 'Request',
					id: '1',
					tag: 'Echo',
					payload: {},
					headers: [['only-one']],
				},
				{
					_tag: 'ClientProtocolError',
					error: {
						_tag: 'RpcClientError',
						reason: 'Protocol',
						message: 'x',
					},
				},
			];
			for (const frame of rejected) {
				expect(Option.isNone(decodeFromClientFrame(frame))).toBe(true);
				expect(Option.isNone(decodeFromServerFrame(frame))).toBe(true);
			}
		}),
	);
});
