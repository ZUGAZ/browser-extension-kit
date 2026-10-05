import { describe, expect, layer } from '@effect/vitest';
import { Effect, Equal, Logger, Schema } from 'effect';

import {
	Disconnected,
	ExtensionContextInvalidated,
	ReceiverUnavailable,
	Timeout,
} from './connection-errors';
import { Content } from './endpoint';

const silentLogger = Logger.replace(Logger.defaultLogger, Logger.none);

const roundTrip = <A, I>(schema: Schema.Schema<A, I>, value: A) =>
	Effect.gen(function* () {
		const encoded = yield* Schema.encode(schema)(value);
		const decoded = yield* Schema.decodeUnknown(schema)(encoded);
		expect(Equal.equals(decoded, value)).toBe(true);
	});

describe('connection errors', () => {
	layer(silentLogger)((it) => {
		it.effect('round-trips Disconnected', () =>
			roundTrip(Disconnected, new Disconnected({ detail: 'closed locally' })),
		);

		it.effect('round-trips Disconnected without detail', () =>
			roundTrip(Disconnected, new Disconnected({})),
		);

		it.effect('round-trips ReceiverUnavailable with a Content endpoint', () =>
			roundTrip(
				ReceiverUnavailable,
				new ReceiverUnavailable({
					endpoint: new Content({ tabId: 1, frameId: 0 }),
				}),
			),
		);

		it.effect('round-trips Timeout', () =>
			roundTrip(Timeout, new Timeout({ afterMillis: 1500 })),
		);

		it.effect('round-trips ExtensionContextInvalidated', () =>
			roundTrip(ExtensionContextInvalidated, new ExtensionContextInvalidated()),
		);
	});
});
