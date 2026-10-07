/* eslint-disable @typescript-eslint/no-deprecated -- the task requires it.scoped */
import { describe, expect, it } from '@effect/vitest';
import {
	Clock,
	Deferred,
	Duration,
	Effect,
	Either,
	Equal,
	Fiber,
	Layer,
	Option,
	Ref,
	Schedule,
	Schema,
	TestClock,
} from 'effect';

import {
	Disconnected,
	ExtensionContextInvalidated,
	Timeout,
} from './connection-errors';
import { Background, Content } from './endpoint';
import {
	countConnects,
	ExampleRpcs,
	makeExampleHandlers,
	settle,
	silentLogger,
} from './example-rpcs.test-support';
import {
	layerPortServer,
	makeBackgroundClient,
	makeContentClient,
	withPortErrors,
} from './index';
import { PortConnector } from './port-connector';
import { makeFakePageLifecycle, makeFakePortHub } from '../testing/index';

const rpcName = 'rpc';

const quickSchedule = Schedule.exponential('100 millis', 2);

const frame = new Content({ tabId: 1, frameId: 0 });

const isDisconnected = Schema.is(Disconnected);
const isInvalidated = Schema.is(ExtensionContextInvalidated);

const recordConnects = (times: Ref.Ref<Array<number>>) =>
	Layer.effect(
		PortConnector,
		Effect.gen(function* () {
			const connector = yield* PortConnector;
			return {
				connect: (
					target: Parameters<PortConnector['Type']['connect']>[0],
					name: string,
				) =>
					Clock.currentTimeMillis.pipe(
						Effect.tap((now) =>
							Ref.update(times, (previous) => [...previous, now]),
						),
						Effect.andThen(connector.connect(target, name)),
					),
				listen: (name: string) => connector.listen(name),
			};
		}),
	);

describe('port rpc client', () => {
	it.scoped('backs off until a listener appears', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const lifecycle = yield* makeFakePageLifecycle;
			const connects = yield* Ref.make(0);
			const client = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
				reconnectSchedule: quickSchedule,
			}).pipe(
				Effect.provide(
					countConnects(connects).pipe(Layer.provide(hub.layerFor(frame))),
				),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(1);
			const echo = yield* client
				.Echo({ text: 'hi' })
				.pipe(withPortErrors({ timeout: '1 minute' }), Effect.fork);
			yield* TestClock.adjust('99 millis');
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(1);
			yield* TestClock.adjust('1 milli');
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(2);
			yield* TestClock.adjust('200 millis');
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(3);
			yield* TestClock.adjust('400 millis');
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(4);
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			yield* TestClock.adjust('800 millis');
			expect(yield* Fiber.join(echo)).toBe('hi');
		}),
	);

	it.scoped('caps the default schedule', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const lifecycle = yield* makeFakePageLifecycle;
			const times = yield* Ref.make<Array<number>>([]);
			yield* makeBackgroundClient(ExampleRpcs, { name: rpcName }).pipe(
				Effect.provide(
					recordConnects(times).pipe(Layer.provide(hub.layerFor(frame))),
				),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			yield* settle;
			yield* TestClock.adjust('2 minutes');
			yield* settle;
			const stamps = yield* Ref.get(times);
			const delays: Array<number> = [];
			for (
				let index = 1;
				index < stamps.length && delays.length < 10;
				index++
			) {
				const current = stamps[index];
				const previous = stamps[index - 1];
				if (current === undefined || previous === undefined) {
					return yield* Effect.die('missing connect time');
				}
				delays.push(current - previous);
			}
			expect(delays.length).toBe(10);
			const first = delays[0];
			if (first === undefined) {
				return yield* Effect.die('missing first delay');
			}
			expect(first).toBeGreaterThanOrEqual(80);
			expect(first).toBeLessThanOrEqual(120);
			for (const delay of delays) {
				expect(delay).toBeLessThanOrEqual(Duration.toMillis('6 seconds'));
			}
		}),
	);

	it.scoped('reconnects immediately after a stable session', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const lifecycle = yield* makeFakePageLifecycle;
			const connects = yield* Ref.make(0);
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const client = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
				reconnectSchedule: quickSchedule,
			}).pipe(
				Effect.provide(
					countConnects(connects).pipe(Layer.provide(hub.layerFor(frame))),
				),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
				'hi',
			);
			yield* TestClock.adjust('2 seconds');
			// Open the stream before Hang. Failing Hang resumes this test
			// inline, and the stream mailbox has to be failed before that.
			const mailbox = yield* client.Counter(
				{ upTo: 1000 },
				{ asMailbox: true },
			);
			expect(yield* mailbox.take.pipe(withPortErrors())).toBe(1);
			const hang = yield* client
				.Hang(undefined)
				.pipe(withPortErrors(), Effect.fork);
			yield* Deferred.await(handlers.started);
			const countBefore = yield* Ref.get(connects);
			yield* hub.stopServiceWorker;
			const hangError = yield* Fiber.join(hang).pipe(Effect.flip);
			expect(isDisconnected(hangError)).toBe(true);
			const streamError = yield* Effect.gen(function* () {
				for (let attempt = 0; attempt < 32; attempt++) {
					const result = yield* mailbox.take.pipe(
						withPortErrors(),
						Effect.either,
					);
					if (Either.isLeft(result)) {
						return result.left;
					}
				}
				return yield* Effect.die('stream stayed open');
			});
			expect(isDisconnected(streamError)).toBe(true);
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(countBefore + 1);
			expect(yield* client.Echo({ text: 'again' }).pipe(withPortErrors())).toBe(
				'again',
			);
		}),
	);

	it.scoped('waits after an unstable session', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const lifecycle = yield* makeFakePageLifecycle;
			const connects = yield* Ref.make(0);
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const client = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
				reconnectSchedule: quickSchedule,
			}).pipe(
				Effect.provide(
					countConnects(connects).pipe(Layer.provide(hub.layerFor(frame))),
				),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
				'hi',
			);
			expect(yield* Ref.get(connects)).toBe(1);
			yield* hub.stopServiceWorker;
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(1);
			yield* TestClock.adjust('99 millis');
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(1);
			yield* TestClock.adjust('1 milli');
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(2);
		}),
	);

	it.scoped('a call during backoff waits for the next connection', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const lifecycle = yield* makeFakePageLifecycle;
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const client = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
				reconnectSchedule: quickSchedule,
			}).pipe(
				Effect.provide(hub.layerFor(frame)),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
				'hi',
			);
			yield* hub.stopServiceWorker;
			yield* settle;
			const echo = yield* client
				.Echo({ text: 'later' })
				.pipe(withPortErrors(), Effect.fork);
			yield* TestClock.adjust('99 millis');
			yield* settle;
			expect(Option.isNone(yield* Fiber.poll(echo))).toBe(true);
			yield* TestClock.adjust('1 milli');
			expect(yield* Fiber.join(echo)).toBe('later');
		}),
	);

	it.scoped('pauses in the back/forward cache', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const lifecycle = yield* makeFakePageLifecycle;
			const connects = yield* Ref.make(0);
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const client = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
				reconnectSchedule: quickSchedule,
			}).pipe(
				Effect.provide(
					countConnects(connects).pipe(Layer.provide(hub.layerFor(frame))),
				),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			const hang = yield* client
				.Hang(undefined)
				.pipe(withPortErrors(), Effect.fork);
			yield* Deferred.await(handlers.started);
			const countBefore = yield* Ref.get(connects);
			yield* lifecycle.enterCache;
			yield* hub.enterBackForwardCache(frame);
			expect(isDisconnected(yield* Fiber.join(hang).pipe(Effect.flip))).toBe(
				true,
			);
			yield* TestClock.adjust('1 minute');
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(countBefore);
			yield* lifecycle.restore;
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(countBefore + 1);
			expect(yield* client.Echo({ text: 'back' }).pipe(withPortErrors())).toBe(
				'back',
			);
		}),
	);

	it.scoped('stops when the extension context is invalidated', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const lifecycle = yield* makeFakePageLifecycle;
			const connects = yield* Ref.make(0);
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const client = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
				reconnectSchedule: quickSchedule,
			}).pipe(
				Effect.provide(
					countConnects(connects).pipe(Layer.provide(hub.layerFor(frame))),
				),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			const hang = yield* client
				.Hang(undefined)
				.pipe(withPortErrors(), Effect.fork);
			yield* Deferred.await(handlers.started);
			const countBefore = yield* Ref.get(connects);
			yield* hub.invalidateExtensionContext(frame);
			expect(isInvalidated(yield* Fiber.join(hang).pipe(Effect.flip))).toBe(
				true,
			);
			const echo = yield* client
				.Echo({ text: 'hi' })
				.pipe(withPortErrors(), Effect.fork);
			yield* settle;
			expect(Option.isSome(yield* Fiber.poll(echo))).toBe(true);
			expect(isInvalidated(yield* Fiber.join(echo).pipe(Effect.flip))).toBe(
				true,
			);
			yield* TestClock.adjust('10 minutes');
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(countBefore);
		}),
	);

	it.scoped('times out a call and interrupts the handler', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const lifecycle = yield* makeFakePageLifecycle;
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const client = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
			}).pipe(
				Effect.provide(hub.layerFor(frame)),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			const hang = yield* client
				.Hang(undefined)
				.pipe(withPortErrors(), Effect.fork);
			yield* Deferred.await(handlers.started);
			yield* TestClock.adjust('9999 millis');
			yield* settle;
			expect(Option.isNone(yield* Fiber.poll(hang))).toBe(true);
			yield* TestClock.adjust('1 milli');
			expect(
				Equal.equals(
					yield* Fiber.join(hang).pipe(Effect.flip),
					new Timeout({ afterMillis: 10000 }),
				),
			).toBe(true);
			yield* Deferred.await(handlers.interrupted);

			const quicker = yield* client
				.Hang(undefined)
				.pipe(withPortErrors({ timeout: '2 seconds' }), Effect.fork);
			yield* settle;
			yield* TestClock.adjust('1999 millis');
			yield* settle;
			expect(Option.isNone(yield* Fiber.poll(quicker))).toBe(true);
			yield* TestClock.adjust('1 milli');
			expect(
				Equal.equals(
					yield* Fiber.join(quicker).pipe(Effect.flip),
					new Timeout({ afterMillis: 2000 }),
				),
			).toBe(true);
		}),
	);

	it.scoped('becomes terminal when the schedule is exhausted', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const lifecycle = yield* makeFakePageLifecycle;
			const connects = yield* Ref.make(0);
			const client = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
				reconnectSchedule: Schedule.recurs(2),
			}).pipe(
				Effect.provide(
					countConnects(connects).pipe(Layer.provide(hub.layerFor(frame))),
				),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(3);
			const echo = yield* client
				.Echo({ text: 'hi' })
				.pipe(withPortErrors(), Effect.fork);
			yield* settle;
			expect(Option.isSome(yield* Fiber.poll(echo))).toBe(true);
			expect(isDisconnected(yield* Fiber.join(echo).pipe(Effect.flip))).toBe(
				true,
			);
			const count = yield* Ref.get(connects);
			yield* TestClock.adjust('1 minute');
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(count);
		}),
	);

	it.scoped('content client does not reconnect', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const content = new Content({ tabId: 1, frameId: 0 });
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(content)),
					Layer.provide(silentLogger),
				),
			);
			const client = yield* makeContentClient(ExampleRpcs, {
				name: rpcName,
				target: content,
			}).pipe(
				Effect.provide(
					countConnects(connects).pipe(
						Layer.provide(hub.layerFor(new Background())),
					),
				),
				Effect.provide(silentLogger),
			);
			expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
				'hi',
			);
			const hang = yield* client
				.Hang(undefined)
				.pipe(withPortErrors(), Effect.fork);
			yield* Deferred.await(handlers.started);
			yield* hub.navigateTab(1);
			expect(isDisconnected(yield* Fiber.join(hang).pipe(Effect.flip))).toBe(
				true,
			);
			const later = yield* client
				.Echo({ text: 'again' })
				.pipe(withPortErrors(), Effect.fork);
			yield* settle;
			expect(Option.isSome(yield* Fiber.poll(later))).toBe(true);
			expect(isDisconnected(yield* Fiber.join(later).pipe(Effect.flip))).toBe(
				true,
			);
			yield* TestClock.adjust('1 minute');
			yield* settle;
			expect(yield* Ref.get(connects)).toBe(1);
		}),
	);
});
