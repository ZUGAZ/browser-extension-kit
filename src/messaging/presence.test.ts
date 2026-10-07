/* eslint-disable @typescript-eslint/no-deprecated -- the task requires it.scoped */
import { describe, expect, it } from '@effect/vitest';
import * as fc from 'effect/FastCheck';
import {
	Effect,
	Exit,
	Fiber,
	HashMap,
	Layer,
	ManagedRuntime,
	Option,
	Schedule,
	Scope,
	Stream,
	TestClock,
} from 'effect';

import type { Endpoint } from './endpoint';
import {
	Background,
	Content,
	ExtensionPage,
	layerPortServer,
	layerPresence,
	makeBackgroundClient,
	PageLifecycleNone,
	Popup,
	watchPresence,
} from './index';
import {
	ExampleRpcs,
	makeExampleHandlers,
	presentWhere,
	settle,
	silentLogger,
} from './example-rpcs.test-support';
import { PageLifecycle } from './page-lifecycle';
import { presenceFromPeers, type PortInfo } from './presence';
import type { FakePortHub } from '../testing/fake-port-hub';
import { makeFakePageLifecycle, makeFakePortHub } from '../testing/index';

const backgroundChannel = 'to-background';

const endpointArbitrary: fc.Arbitrary<Endpoint> = fc.oneof(
	fc.constant(new Background()),
	fc.constant(new Popup()),
	fc.integer().map((tabId) => new ExtensionPage({ tabId })),
	fc
		.tuple(fc.integer(), fc.integer())
		.map(([tabId, frameId]) => new Content({ tabId, frameId })),
);

const backgroundLayer = <A, E, R>(
	hub: FakePortHub,
	handlersLayer: Layer.Layer<A, E, R>,
) =>
	layerPresence.pipe(
		Layer.provideMerge(
			layerPortServer(ExampleRpcs, { name: backgroundChannel }).pipe(
				Layer.provide(handlersLayer),
				Layer.provide(hub.layerFor(new Background())),
				Layer.provide(silentLogger),
			),
		),
	);

const connectionCount = (
	presence: HashMap.HashMap<Endpoint, PortInfo>,
	endpoint: Endpoint,
) =>
	Option.match(HashMap.get(presence, endpoint), {
		onNone: () => 0,
		onSome: (info) => info.connections,
	});

const requirePresence = (
	predicate: (presence: HashMap.HashMap<Endpoint, PortInfo>) => boolean,
) =>
	Effect.gen(function* () {
		const found = yield* presentWhere(predicate);
		if (Option.isNone(found)) {
			return yield* Effect.die('presence stream ended');
		}
		return found.value;
	});

const completesWithoutClock = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
	Effect.gen(function* () {
		const fiber = yield* Effect.fork(effect);
		for (let attempt = 0; attempt < 4; attempt++) {
			yield* settle;
			const polled = yield* Fiber.poll(fiber);
			if (Option.isNone(polled)) {
				continue;
			}
			if (Exit.isFailure(polled.value)) {
				return yield* Effect.failCause(polled.value.cause);
			}
			return polled.value.value;
		}
		return yield* Effect.die('still running without a clock advance');
	});

const connect = (
	hub: FakePortHub,
	endpoint: Endpoint,
	lifecycle: Layer.Layer<PageLifecycle>,
	scope?: Scope.Scope,
) => {
	const client = makeBackgroundClient(ExampleRpcs, {
		name: backgroundChannel,
	}).pipe(
		Effect.provide(hub.layerFor(endpoint)),
		Effect.provide(lifecycle),
		Effect.provide(silentLogger),
	);
	if (scope === undefined) {
		return client;
	}
	return client.pipe(Effect.provideService(Scope.Scope, scope));
};

describe('presence', () => {
	it.effect.prop(
		'counts connections per endpoint',
		[fc.array(endpointArbitrary)],
		([endpoints]) =>
			Effect.gen(function* () {
				let peers = HashMap.empty<number, Endpoint>();
				for (let index = 0; index < endpoints.length; index++) {
					const endpoint = endpoints[index];
					if (endpoint === undefined) {
						return yield* Effect.die('missing endpoint');
					}
					peers = HashMap.set(peers, index, endpoint);
				}
				const presence = presenceFromPeers(peers);
				if (endpoints.length === 0) {
					expect(HashMap.size(presence)).toBe(0);
				}
				const total = HashMap.reduce(
					presence,
					0,
					(sum, info) => sum + info.connections,
				);
				expect(total).toBe(endpoints.length);
				for (const info of HashMap.values(presence)) {
					expect(info.connections).toBeGreaterThanOrEqual(1);
				}
				const distinct = HashMap.reduce(
					peers,
					HashMap.empty<Endpoint, number>(),
					(map, endpoint) => HashMap.set(map, endpoint, 1),
				);
				expect(HashMap.size(presence)).toBe(HashMap.size(distinct));
				for (const endpoint of HashMap.values(peers)) {
					expect(HashMap.has(presence, endpoint)).toBe(true);
				}
			}),
	);

	it.scoped('emits the current registry to a new subscriber', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const frame = new Content({ tabId: 1, frameId: 0 });
			yield* Effect.gen(function* () {
				const first = yield* watchPresence.pipe(Stream.runHead);
				expect(Option.isSome(first) && HashMap.size(first.value) === 0).toBe(
					true,
				);
				yield* connect(hub, frame, PageLifecycleNone);
				yield* requirePresence((current) => HashMap.has(current, frame));
				const next = yield* watchPresence.pipe(Stream.runHead);
				if (Option.isNone(next)) {
					return yield* Effect.die('missing presence');
				}
				expect(connectionCount(next.value, frame)).toBe(1);
			}).pipe(Effect.provide(backgroundLayer(hub, handlers.layer)));
		}),
	);

	it.scoped('tracks content, popup, and extension page ports', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const frame = new Content({ tabId: 1, frameId: 0 });
			const page = new ExtensionPage({ tabId: 5 });
			yield* Effect.gen(function* () {
				const contentScope = yield* Scope.make();
				yield* connect(hub, frame, PageLifecycleNone, contentScope);
				yield* requirePresence((current) => HashMap.has(current, frame));
				yield* Scope.close(contentScope, Exit.void);
				yield* requirePresence((current) => !HashMap.has(current, frame));

				yield* connect(hub, new Popup(), PageLifecycleNone);
				yield* requirePresence((current) => HashMap.has(current, new Popup()));
				yield* hub.closePopup;
				yield* requirePresence((current) => !HashMap.has(current, new Popup()));

				const pageScope = yield* Scope.make();
				yield* connect(hub, page, PageLifecycleNone, pageScope);
				yield* requirePresence((current) => HashMap.has(current, page));
				yield* Scope.close(pageScope, Exit.void);
				yield* requirePresence((current) => !HashMap.has(current, page));
			}).pipe(Effect.provide(backgroundLayer(hub, handlers.layer)));
		}),
	);

	it.scoped('keeps each content frame as its own key', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const frame00 = new Content({ tabId: 1, frameId: 0 });
			const frame01 = new Content({ tabId: 1, frameId: 1 });
			const frame20 = new Content({ tabId: 2, frameId: 0 });
			yield* Effect.gen(function* () {
				yield* connect(hub, frame00, PageLifecycleNone);
				yield* connect(hub, frame01, PageLifecycleNone);
				yield* connect(hub, frame20, PageLifecycleNone);
				yield* requirePresence(
					(current) =>
						HashMap.has(current, frame00) &&
						HashMap.has(current, frame01) &&
						HashMap.has(current, frame20),
				);
				yield* hub.navigateTab(1);
				const left = yield* requirePresence(
					(current) =>
						!HashMap.has(current, frame00) &&
						!HashMap.has(current, frame01) &&
						HashMap.has(current, frame20),
				);
				expect(connectionCount(left, frame20)).toBe(1);
			}).pipe(Effect.provide(backgroundLayer(hub, handlers.layer)));
		}),
	);

	it.scoped('stays present while one of two ports is still open', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const frame = new Content({ tabId: 1, frameId: 0 });
			yield* Effect.gen(function* () {
				const first = yield* Scope.make();
				const second = yield* Scope.make();
				yield* connect(hub, frame, PageLifecycleNone, first);
				yield* connect(hub, frame, PageLifecycleNone, second);
				const overlapped = yield* requirePresence(
					(current) => connectionCount(current, frame) === 2,
				);
				expect(connectionCount(overlapped, frame)).toBe(2);
				yield* Scope.close(first, Exit.void);
				const one = yield* requirePresence(
					(current) => connectionCount(current, frame) === 1,
				);
				expect(HashMap.has(one, frame)).toBe(true);
				yield* Scope.close(second, Exit.void);
				yield* requirePresence((current) => !HashMap.has(current, frame));
			}).pipe(Effect.provide(backgroundLayer(hub, handlers.layer)));
		}),
	);

	it.scoped(
		'drops a cached frame and restores it without a clock advance',
		() =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const handlers = yield* makeExampleHandlers;
				const lifecycle = yield* makeFakePageLifecycle;
				const frame = new Content({ tabId: 1, frameId: 0 });
				yield* Effect.gen(function* () {
					yield* connect(hub, frame, lifecycle.layer);
					yield* requirePresence((current) => HashMap.has(current, frame));
					yield* lifecycle.enterCache;
					yield* hub.enterBackForwardCache(frame);
					yield* requirePresence((current) => !HashMap.has(current, frame));
					yield* settle;
					yield* completesWithoutClock(
						Effect.gen(function* () {
							yield* lifecycle.restore;
							yield* requirePresence((current) => HashMap.has(current, frame));
						}),
					);
				}).pipe(Effect.provide(backgroundLayer(hub, handlers.layer)));
			}),
	);

	it.scoped('empties when the service worker stops, then fills again', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const frame = new Content({ tabId: 1, frameId: 0 });
			yield* Effect.gen(function* () {
				yield* connect(hub, frame, PageLifecycleNone);
				yield* connect(hub, new Popup(), PageLifecycleNone);
				yield* requirePresence(
					(current) =>
						HashMap.has(current, frame) && HashMap.has(current, new Popup()),
				);
				yield* TestClock.adjust('2 seconds');
				yield* settle;
				const emptied = yield* watchPresence.pipe(
					Stream.drop(1),
					Stream.filter((current) => HashMap.size(current) === 0),
					Stream.runHead,
					Effect.fork,
				);
				yield* hub.stopServiceWorker;
				const empty = yield* Fiber.join(emptied);
				expect(Option.isSome(empty)).toBe(true);
				yield* completesWithoutClock(
					requirePresence(
						(current) =>
							HashMap.has(current, frame) && HashMap.has(current, new Popup()),
					),
				);
			}).pipe(Effect.provide(backgroundLayer(hub, handlers.layer)));
		}),
	);

	it.scoped('starts synchronously and refills after a fresh worker', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const lifecycle = yield* makeFakePageLifecycle;
			const frame = new Content({ tabId: 1, frameId: 0 });
			const layer = backgroundLayer(hub, handlers.layer);
			const runtime = ManagedRuntime.make(layer);
			let disposed = false;
			const dispose = Effect.suspend(() => {
				if (disposed) {
					return Effect.void;
				}
				disposed = true;
				return Effect.promise(() => runtime.dispose());
			});
			yield* Effect.addFinalizer(() => dispose);
			expect(() => {
				runtime.runSync(Effect.void);
			}).not.toThrow();
			yield* makeBackgroundClient(ExampleRpcs, {
				name: backgroundChannel,
				reconnectSchedule: Schedule.spaced('100 millis'),
			}).pipe(
				Effect.provide(hub.layerFor(frame)),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
				Effect.fork,
			);
			yield* Effect.promise(() =>
				runtime.runPromise(
					requirePresence((current) => HashMap.has(current, frame)).pipe(
						Effect.asVoid,
					),
				),
			);
			yield* dispose;

			const next = ManagedRuntime.make(layer);
			yield* Effect.addFinalizer(() => Effect.promise(() => next.dispose()));
			expect(() => {
				next.runSync(Effect.void);
			}).not.toThrow();
			const first = yield* Effect.promise(() =>
				next.runPromise(watchPresence.pipe(Stream.runHead)),
			);
			expect(Option.isSome(first) && HashMap.size(first.value) === 0).toBe(
				true,
			);
			yield* TestClock.adjust('100 millis');
			yield* Effect.promise(() =>
				next.runPromise(
					requirePresence((current) => HashMap.has(current, frame)).pipe(
						Effect.asVoid,
					),
				),
			);
		}),
	);

	it.scoped('does not restore an invalidated frame', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const lifecycle = yield* makeFakePageLifecycle;
			const frame = new Content({ tabId: 1, frameId: 0 });
			yield* Effect.gen(function* () {
				yield* connect(hub, frame, lifecycle.layer);
				yield* requirePresence((current) => HashMap.has(current, frame));
				yield* hub.invalidateExtensionContext(frame);
				yield* requirePresence((current) => !HashMap.has(current, frame));
				yield* TestClock.adjust('1 minute');
				yield* settle;
				const current = yield* watchPresence.pipe(Stream.runHead);
				if (Option.isNone(current)) {
					return yield* Effect.die('missing presence');
				}
				expect(HashMap.has(current.value, frame)).toBe(false);
			}).pipe(Effect.provide(backgroundLayer(hub, handlers.layer)));
		}),
	);
});
