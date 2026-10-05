import { describe, expect, layer } from '@effect/vitest';
import * as fc from 'effect/FastCheck';
import { Effect, Equal, Hash, HashMap, Logger, Option, Schema } from 'effect';

import { Background, Content, Endpoint, ExtensionPage, Popup } from './index';

type ExportedEndpoint = import('./index').Endpoint;

const silentLogger = Logger.replace(Logger.defaultLogger, Logger.none);

describe('Endpoint', () => {
	layer(silentLogger)((it) => {
		it.effect.prop(
			'Content values share Equal, Hash, and HashMap keys',
			[fc.integer(), fc.integer()],
			([tabId, frameId]) =>
				Effect.sync(() => {
					const left = new Content({ tabId, frameId });
					const right = new Content({ tabId, frameId });
					expect(Equal.equals(left, right)).toBe(true);
					expect(Hash.hash(left)).toBe(Hash.hash(right));
					const map = HashMap.set(
						HashMap.empty<Content, string>(),
						left,
						'open',
					);
					expect(
						Equal.equals(HashMap.get(map, right), Option.some('open')),
					).toBe(true);
				}),
		);

		it.effect('round-trips every variant', () =>
			Effect.gen(function* () {
				const variants: ReadonlyArray<ExportedEndpoint> = [
					new Background(),
					new Popup(),
					new ExtensionPage({ tabId: 5 }),
					new Content({ tabId: 1, frameId: 0 }),
				];
				for (const value of variants) {
					const encoded = yield* Schema.encode(Endpoint)(value);
					const decoded = yield* Schema.decodeUnknown(Endpoint)(encoded);
					expect(Equal.equals(decoded, value)).toBe(true);
				}
			}),
		);
	});
});
