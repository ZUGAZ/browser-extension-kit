/* eslint-disable @typescript-eslint/no-deprecated -- the task requires it.scoped */
import { describe, expect, it } from '@effect/vitest';
import {
	Deferred,
	Effect,
	Exit,
	Fiber,
	Layer,
	Option,
	Ref,
	Schedule,
	Schema,
	Scope,
	Stream,
	SubscriptionRef,
	TestClock,
} from 'effect';

import { Disconnected, ExtensionContextInvalidated } from './connection-errors';
import { Background, Content, Popup } from './endpoint';
import {
	countConnects,
	settle,
	silentLogger,
} from './example-rpcs.test-support';
import {
	layerPortServer,
	makeBackgroundClient,
	StateStatus,
	subscribeState,
} from './index';
import { PageLifecycleNone, type PageLifecycle } from './page-lifecycle';
import {
	CounterRef,
	CounterRpcs,
	CounterState,
	makeCounterHandlers,
} from './watch-state-example.test-support';
import type { FakePortHub } from '../testing/fake-port-hub';
import { makeFakePageLifecycle, makeFakePortHub } from '../testing/index';

const rpcName = 'kit';
const quickSchedule = Schedule.exponential('100 millis', 2);
const frame = new Content({ tabId: 1, frameId: 0 });

const counter = (
	count: number,
	ownerTab: Option.Option<number> = Option.none(),
) => new CounterState({ count, ownerTab });

const isInvalidated = Schema.is(ExtensionContextInvalidated);
const isDisconnected = Schema.is(Disconnected);

const boot = (
	hub: FakePortHub,
	endpoint: Popup | Content,
	lifecycle: Layer.Layer<PageLifecycle>,
	initial: CounterState,
	schedule: Schedule.Schedule<unknown, Disconnected> = quickSchedule,
	releaseHold = true,
) =>
	Effect.gen(function* () {
		const ref = yield* SubscriptionRef.make(initial);
		const handlers = yield* makeCounterHandlers.pipe(
			Effect.provideService(CounterRef, ref),
		);
		if (releaseHold) {
			yield* Deferred.succeed(handlers.hold, undefined);
		}
		const connects = yield* Ref.make(0);
		const serverScope = yield* Scope.make();
		yield* Layer.build(
			layerPortServer(CounterRpcs, { name: rpcName }).pipe(
				Layer.provide(handlers.layer),
				Layer.provide(hub.layerFor(new Background())),
				Layer.provide(silentLogger),
			),
		).pipe(Effect.provideService(Scope.Scope, serverScope));
		const { client, connection } = yield* makeBackgroundClient(CounterRpcs, {
			name: rpcName,
			reconnectSchedule: schedule,
		}).pipe(
			Effect.provide(
				countConnects(connects).pipe(Layer.provide(hub.layerFor(endpoint))),
			),
			Effect.provide(lifecycle),
			Effect.provide(silentLogger),
		);
		return { ref, handlers, connects, serverScope, client, connection };
	});

const track = <A, E, R>(stream: Stream.Stream<StateStatus<A>, E, R>) =>
	Effect.gen(function* () {
		const latest = yield* SubscriptionRef.make<StateStatus<A>>(
			StateStatus.Connecting(),
		);
		const fiber = yield* stream.pipe(
			Stream.runForEach((status) => SubscriptionRef.set(latest, status)),
			Effect.forkScoped,
		);
		return { latest, fiber };
	});

const current = <A>(latest: SubscriptionRef.SubscriptionRef<StateStatus<A>>) =>
	SubscriptionRef.get(latest);

describe('subscribeState', () => {
	it.scoped(
		'stays connecting until the server is up, then follows the ref',
		() =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const lifecycle = yield* makeFakePageLifecycle;
				const ref = yield* SubscriptionRef.make(counter(0));
				const handlers = yield* makeCounterHandlers.pipe(
					Effect.provideService(CounterRef, ref),
				);
				yield* Deferred.succeed(handlers.hold, undefined);
				const { client, connection } = yield* makeBackgroundClient(
					CounterRpcs,
					{
						name: rpcName,
						reconnectSchedule: quickSchedule,
					},
				).pipe(
					Effect.provide(hub.layerFor(new Popup())),
					Effect.provide(lifecycle.layer),
					Effect.provide(silentLogger),
				);
				const { latest } = yield* track(
					subscribeState(client.WatchCounter(undefined), connection),
				);
				yield* settle;
				yield* TestClock.adjust('50 millis');
				yield* settle;
				expect(StateStatus.$is('Connecting')(yield* current(latest))).toBe(
					true,
				);
				yield* Layer.build(
					layerPortServer(CounterRpcs, { name: rpcName }).pipe(
						Layer.provide(handlers.layer),
						Layer.provide(hub.layerFor(new Background())),
						Layer.provide(silentLogger),
					),
				);
				yield* TestClock.adjust('50 millis');
				yield* settle;
				const live = yield* current(latest);
				expect(StateStatus.$is('Live')(live) && live.value.count === 0).toBe(
					true,
				);
				yield* SubscriptionRef.set(ref, counter(1));
				yield* settle;
				const next = yield* current(latest);
				expect(StateStatus.$is('Live')(next) && next.value.count === 1).toBe(
					true,
				);
			}),
	);

	it.scoped('projects the caller across the port', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const ref = yield* SubscriptionRef.make(counter(0, Option.some(1)));
			const handlers = yield* makeCounterHandlers.pipe(
				Effect.provideService(CounterRef, ref),
			);
			yield* Deferred.succeed(handlers.hold, undefined);
			yield* Layer.build(
				layerPortServer(CounterRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const open = (endpoint: Popup | Content) =>
				makeBackgroundClient(CounterRpcs, {
					name: rpcName,
					reconnectSchedule: quickSchedule,
				}).pipe(
					Effect.provide(hub.layerFor(endpoint)),
					Effect.provide(PageLifecycleNone),
					Effect.provide(silentLogger),
				);
			const popup = yield* open(new Popup());
			const content = yield* open(frame);
			const popupStatus = yield* track(
				subscribeState(popup.client.WatchCounter(undefined), popup.connection),
			);
			const contentStatus = yield* track(
				subscribeState(
					content.client.WatchCounter(undefined),
					content.connection,
				),
			);
			yield* settle;
			const popupLive = yield* current(popupStatus.latest);
			const contentLive = yield* current(contentStatus.latest);
			expect(
				StateStatus.$is('Live')(popupLive) && !popupLive.value.isOwner,
			).toBe(true);
			expect(
				StateStatus.$is('Live')(contentLive) && contentLive.value.isOwner,
			).toBe(true);
		}),
	);

	it.scoped('reconnects immediately after a stable session', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const lifecycle = yield* makeFakePageLifecycle;
			const session = yield* boot(
				hub,
				new Popup(),
				lifecycle.layer,
				counter(0),
			);
			const { latest } = yield* track(
				subscribeState(
					session.client.WatchCounter(undefined),
					session.connection,
				),
			);
			yield* settle;
			yield* TestClock.adjust('2 seconds');
			yield* settle;
			const subscribedBefore = yield* Ref.get(session.handlers.subscribed);
			const connectsBefore = yield* Ref.get(session.connects);
			yield* hub.stopServiceWorker;
			yield* SubscriptionRef.set(session.ref, counter(9));
			const dropped = yield* Effect.gen(function* () {
				for (let step = 0; step < 32; step++) {
					const status = yield* current(latest);
					if (StateStatus.$is('Reconnecting')(status)) {
						return status;
					}
					yield* Effect.yieldNow();
				}
				return yield* current(latest);
			});
			expect(
				StateStatus.$is('Reconnecting')(dropped) && dropped.value.count === 0,
			).toBe(true);
			yield* settle;
			expect(yield* Ref.get(session.connects)).toBe(connectsBefore + 1);
			const restored = yield* current(latest);
			expect(
				StateStatus.$is('Live')(restored) && restored.value.count === 9,
			).toBe(true);
			expect(yield* Ref.get(session.handlers.subscribed)).toBe(
				subscribedBefore + 1,
			);
		}),
	);

	it.scoped('follows the protocol backoff after an unstable session', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const lifecycle = yield* makeFakePageLifecycle;
			const session = yield* boot(hub, frame, lifecycle.layer, counter(0));
			const { latest } = yield* track(
				subscribeState(
					session.client.WatchCounter(undefined),
					session.connection,
				),
			);
			yield* settle;
			expect(StateStatus.$is('Live')(yield* current(latest))).toBe(true);
			yield* hub.stopServiceWorker;
			yield* settle;
			yield* TestClock.adjust('99 millis');
			yield* settle;
			const waiting = yield* current(latest);
			expect(
				StateStatus.$is('Reconnecting')(waiting) && waiting.value.count === 0,
			).toBe(true);
			expect(yield* Ref.get(session.connects)).toBe(1);
			yield* TestClock.adjust('1 milli');
			yield* settle;
			const restored = yield* current(latest);
			expect(
				StateStatus.$is('Live')(restored) && restored.value.count === 0,
			).toBe(true);
			expect(yield* Ref.get(session.handlers.subscribed)).toBe(2);
		}),
	);

	it.scoped(
		'pauses in the back/forward cache and restores the fresh state',
		() =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const lifecycle = yield* makeFakePageLifecycle;
				const session = yield* boot(hub, frame, lifecycle.layer, counter(0));
				const { latest } = yield* track(
					subscribeState(
						session.client.WatchCounter(undefined),
						session.connection,
					),
				);
				yield* settle;
				const connectsBefore = yield* Ref.get(session.connects);
				yield* lifecycle.enterCache;
				yield* hub.enterBackForwardCache(frame);
				yield* settle;
				expect(StateStatus.$is('Reconnecting')(yield* current(latest))).toBe(
					true,
				);
				yield* SubscriptionRef.set(session.ref, counter(4));
				yield* TestClock.adjust('1 minute');
				yield* settle;
				expect(StateStatus.$is('Reconnecting')(yield* current(latest))).toBe(
					true,
				);
				expect(yield* Ref.get(session.connects)).toBe(connectsBefore);
				yield* lifecycle.restore;
				yield* settle;
				const restored = yield* current(latest);
				expect(
					StateStatus.$is('Live')(restored) && restored.value.count === 4,
				).toBe(true);
			}),
	);

	it.scoped('terminates when the extension context is invalidated', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const lifecycle = yield* makeFakePageLifecycle;
			const session = yield* boot(hub, frame, lifecycle.layer, counter(3));
			const { latest, fiber } = yield* track(
				subscribeState(
					session.client.WatchCounter(undefined),
					session.connection,
				),
			);
			yield* settle;
			const connectsBefore = yield* Ref.get(session.connects);
			yield* hub.invalidateExtensionContext(frame);
			yield* Fiber.join(fiber);
			const status = yield* current(latest);
			expect(
				StateStatus.$is('Terminated')(status) &&
					isInvalidated(status.error) &&
					Option.isSome(status.value) &&
					status.value.value.count === 3,
			).toBe(true);
			yield* TestClock.adjust('10 minutes');
			yield* settle;
			expect(yield* Ref.get(session.connects)).toBe(connectsBefore);
		}),
	);

	it.scoped('terminates when the reconnect schedule is exhausted', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const lifecycle = yield* makeFakePageLifecycle;
			const session = yield* boot(
				hub,
				frame,
				lifecycle.layer,
				counter(0),
				Schedule.recurs(2),
			);
			const attempts = yield* Ref.make(0);
			const subscription = Stream.unwrap(
				Ref.update(attempts, (count) => count + 1).pipe(
					Effect.as(session.client.WatchCounter(undefined)),
				),
			);
			const { latest, fiber } = yield* track(
				subscribeState(subscription, session.connection),
			);
			yield* settle;
			yield* Scope.close(session.serverScope, Exit.void);
			yield* Fiber.join(fiber);
			const status = yield* current(latest);
			expect(
				StateStatus.$is('Terminated')(status) && isDisconnected(status.error),
			).toBe(true);
			const connectCount = yield* Ref.get(session.connects);
			expect(yield* Ref.get(attempts)).toBeLessThanOrEqual(connectCount + 1);
		}),
	);

	it.scoped('gives a slow reader the latest status', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const session = yield* boot(
				hub,
				new Popup(),
				PageLifecycleNone,
				counter(0),
			);
			const release = yield* Deferred.make<undefined>();
			const pull = yield* Stream.toPull(
				subscribeState(
					session.client.WatchCounter(undefined),
					session.connection,
				),
			);
			yield* pull;
			const next = yield* Deferred.await(release).pipe(
				Effect.andThen(pull),
				Effect.forkScoped,
			);
			for (let count = 1; count <= 50; count++) {
				yield* SubscriptionRef.set(session.ref, counter(count));
			}
			yield* settle;
			yield* Deferred.succeed(release, undefined);
			const chunk = Array.from(yield* Fiber.join(next));
			const status = chunk[0];
			if (status === undefined) {
				return yield* Effect.die('missing status');
			}
			expect(StateStatus.$is('Live')(status) && status.value.count === 50).toBe(
				true,
			);
		}),
	);

	it.scoped('interrupts the server handler when the consumer stops', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const session = yield* boot(
				hub,
				new Popup(),
				PageLifecycleNone,
				counter(0),
			);
			const fiber = yield* subscribeState(
				session.client.WatchCounter(undefined),
				session.connection,
			).pipe(Stream.runDrain, Effect.forkScoped);
			yield* settle;
			expect(yield* Ref.get(session.handlers.active)).toBe(1);
			yield* Fiber.interrupt(fiber);
			yield* settle;
			expect(yield* Ref.get(session.handlers.active)).toBe(0);
		}),
	);

	it.scoped('a drop before the first value stays connecting', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const lifecycle = yield* makeFakePageLifecycle;
			const session = yield* boot(
				hub,
				frame,
				lifecycle.layer,
				counter(0),
				quickSchedule,
				false,
			);
			const { latest } = yield* track(
				subscribeState(
					session.client.WatchCounter(undefined),
					session.connection,
				),
			);
			yield* settle;
			yield* hub.stopServiceWorker;
			yield* settle;
			yield* Deferred.succeed(session.handlers.hold, undefined);
			yield* settle;
			expect(StateStatus.$is('Connecting')(yield* current(latest))).toBe(true);
			expect(StateStatus.$is('Reconnecting')(yield* current(latest))).toBe(
				false,
			);
		}),
	);
});
