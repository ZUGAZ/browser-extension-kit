/* eslint-disable @typescript-eslint/no-deprecated -- the task requires it.scoped */
import { describe, expect, it } from '@effect/vitest';
import type * as rpc from '@effect/rpc/Rpc';
import {
	Cause,
	Deferred,
	Effect,
	Equal,
	Exit,
	Fiber,
	HashMap,
	Layer,
	Option,
	Ref,
	Schema,
	Scope,
	TestClock,
	type Context,
} from 'effect';

import type { Endpoint } from './endpoint';
import {
	Background,
	Content,
	type ContentClients,
	Disconnected,
	ExtensionPage,
	isBackground,
	layerPortServer,
	layerPresence,
	makeBackgroundClient,
	makeContentClients,
	PageLifecycleNone,
	Popup,
	PortInfo,
	ReceiverUnavailable,
	withPortErrors,
} from './index';
import {
	countConnects,
	ExampleRpcs,
	makeExampleHandlers,
	presentWhere,
	settle,
	silentLogger,
} from './example-rpcs.test-support';
import type { FakePortHub } from '../testing/fake-port-hub';
import { makeFakePortHub } from '../testing/index';

const backgroundChannel = 'to-background';
const contentChannel = 'to-content';

const frame0 = new Content({ tabId: 1, frameId: 0 });
const frame1 = new Content({ tabId: 1, frameId: 1 });

const isDisconnected = Schema.is(Disconnected);

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

const contentServer = <A, E, R>(
	hub: FakePortHub,
	handlersLayer: Layer.Layer<A, E, R>,
	frame: Content,
) =>
	layerPortServer(ExampleRpcs, { name: contentChannel }).pipe(
		Layer.provide(handlersLayer),
		Layer.provide(hub.layerFor(frame)),
		Layer.provide(silentLogger),
	);

const connectionCount = (
	presence: HashMap.HashMap<Endpoint, PortInfo>,
	endpoint: Endpoint,
) =>
	Option.match(HashMap.get(presence, endpoint), {
		onNone: () => 0,
		onSome: (info) => info.connections,
	});

const waitPresent = (
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

const untilPortGone = (hub: FakePortHub, frame: Content): Effect.Effect<void> =>
	hub.openPorts.pipe(
		Effect.flatMap((ports) => {
			const open = ports.some(
				(port) => isBackground(port.from) && Equal.equals(port.to, frame),
			);
			if (!open) {
				return Effect.void;
			}
			return Effect.yieldNow().pipe(Effect.andThen(untilPortGone(hub, frame)));
		}),
	);

const expectDisconnected = (exit: Exit.Exit<unknown, unknown>) => {
	expect(Exit.isInterrupted(exit)).toBe(false);
	expect(Exit.isFailure(exit)).toBe(true);
	if (Exit.isFailure(exit)) {
		const failure = Cause.failureOption(exit.cause);
		expect(Option.isSome(failure) && isDisconnected(failure.value)).toBe(true);
	}
};

const openPresence = (
	hub: FakePortHub,
	frame: Content,
	scope?: Scope.Scope,
) => {
	const client = makeBackgroundClient(ExampleRpcs, {
		name: backgroundChannel,
	}).pipe(
		Effect.provide(hub.layerFor(frame)),
		Effect.provide(PageLifecycleNone),
		Effect.provide(silentLogger),
	);
	if (scope === undefined) {
		return client;
	}
	return client.pipe(Effect.provideService(Scope.Scope, scope));
};

const openClients = <R>(
	hub: FakePortHub,
	connects: Ref.Ref<number>,
	context: Context.Context<R>,
	scope?: Scope.Scope,
) => {
	const clients = makeContentClients(ExampleRpcs, {
		name: contentChannel,
	}).pipe(
		Effect.provide(
			countConnects(connects).pipe(
				Layer.provide(hub.layerFor(new Background())),
			),
		),
		Effect.provide(context),
		Effect.provide(silentLogger),
	);
	if (scope === undefined) {
		return clients;
	}
	return clients.pipe(Effect.provideService(Scope.Scope, scope));
};

describe('content clients', () => {
	it.effect('rejects non-content endpoints', () =>
		Effect.sync(() => {
			const probe = (clients: ContentClients<rpc.Any>): void => {
				// @ts-expect-error Popup is not a content endpoint
				void clients.clientFor(new Popup());
				// @ts-expect-error ExtensionPage is not a content endpoint
				void clients.clientFor(new ExtensionPage({ tabId: 5 }));
				// @ts-expect-error Background is not a content endpoint
				void clients.clientFor(new Background());
			};
			expect(typeof probe).toBe('function');
		}),
	);

	it.scoped('fails before opening a port when the endpoint is absent', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const context = yield* Layer.build(backgroundLayer(hub, handlers.layer));
			const clients = yield* openClients(hub, connects, context);
			const error = yield* clients.clientFor(frame0).pipe(Effect.flip);
			expect(
				Equal.equals(error, new ReceiverUnavailable({ endpoint: frame0 })),
			).toBe(true);
			expect(yield* Ref.get(connects)).toBe(0);
			const ports = yield* hub.openPorts;
			expect(ports.some((port) => isBackground(port.from))).toBe(false);
		}),
	);

	it.scoped('calls a present content frame', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const context = yield* Layer.build(backgroundLayer(hub, handlers.layer));
			yield* Effect.gen(function* () {
				yield* openPresence(hub, frame0);
				yield* waitPresent((current) => HashMap.has(current, frame0));
				yield* Layer.build(contentServer(hub, handlers.layer, frame0));
				const clients = yield* openClients(hub, connects, context);
				const client = yield* clients.clientFor(frame0);
				expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				expect(
					Equal.equals(
						yield* client.WhoAmI(undefined).pipe(withPortErrors()),
						new Background(),
					),
				).toBe(true);
			}).pipe(Effect.provide(context));
		}),
	);

	it.scoped('reuses one port for sequential and concurrent calls', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const context = yield* Layer.build(backgroundLayer(hub, handlers.layer));
			yield* Effect.gen(function* () {
				yield* openPresence(hub, frame0);
				yield* waitPresent((current) => HashMap.has(current, frame0));
				yield* Layer.build(contentServer(hub, handlers.layer, frame0));
				const clients = yield* openClients(hub, connects, context);
				const first = yield* clients.clientFor(frame0);
				const second = yield* clients.clientFor(frame0);
				expect(yield* first.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				expect(yield* second.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				const echoes = yield* Effect.all(
					Array.from({ length: 5 }, () =>
						Effect.gen(function* () {
							const client = yield* clients.clientFor(frame0);
							return yield* client.Echo({ text: 'hi' }).pipe(withPortErrors());
						}),
					),
					{ concurrency: 'unbounded' },
				);
				expect(echoes).toEqual(['hi', 'hi', 'hi', 'hi', 'hi']);
				expect(yield* Ref.get(connects)).toBe(1);
			}).pipe(Effect.provide(context));
		}),
	);

	it.scoped('releases the port when presence drops', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const context = yield* Layer.build(backgroundLayer(hub, handlers.layer));
			const presenceScope = yield* Scope.make();
			yield* Effect.gen(function* () {
				yield* openPresence(hub, frame0, presenceScope);
				yield* waitPresent((current) => HashMap.has(current, frame0));
				yield* Layer.build(contentServer(hub, handlers.layer, frame0));
				const clients = yield* openClients(hub, connects, context);
				const client = yield* clients.clientFor(frame0);
				expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				yield* Scope.close(presenceScope, Exit.void);
				yield* completesWithoutClock(
					Effect.gen(function* () {
						yield* waitPresent((current) => !HashMap.has(current, frame0));
						yield* untilPortGone(hub, frame0);
					}),
				);
				const error = yield* clients.clientFor(frame0).pipe(Effect.flip);
				expect(
					Equal.equals(error, new ReceiverUnavailable({ endpoint: frame0 })),
				).toBe(true);
			}).pipe(Effect.provide(context));
		}),
	);

	it.scoped(
		'fails an in-flight call with Disconnected when presence drops',
		() =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const handlers = yield* makeExampleHandlers;
				const connects = yield* Ref.make(0);
				const context = yield* Layer.build(
					backgroundLayer(hub, handlers.layer),
				);
				const presenceScope = yield* Scope.make();
				yield* Effect.gen(function* () {
					yield* openPresence(hub, frame0, presenceScope);
					yield* waitPresent((current) => HashMap.has(current, frame0));
					yield* Layer.build(contentServer(hub, handlers.layer, frame0));
					const clients = yield* openClients(hub, connects, context);
					const client = yield* clients.clientFor(frame0);
					const hang = yield* client
						.Hang(undefined)
						.pipe(withPortErrors(), Effect.fork);
					yield* Deferred.await(handlers.started);
					yield* Scope.close(presenceScope, Exit.void);
					expectDisconnected(yield* Fiber.await(hang));
					yield* Deferred.await(handlers.interrupted);
				}).pipe(Effect.provide(context));
			}),
	);

	it.scoped('disconnects when the frame is present but not serving', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const context = yield* Layer.build(backgroundLayer(hub, handlers.layer));
			yield* Effect.gen(function* () {
				yield* openPresence(hub, frame0);
				yield* waitPresent((current) => HashMap.has(current, frame0));
				const clients = yield* openClients(hub, connects, context);
				const client = yield* clients.clientFor(frame0);
				const error = yield* client
					.Echo({ text: 'hi' })
					.pipe(withPortErrors(), Effect.flip);
				expect(isDisconnected(error)).toBe(true);
				expect(yield* Ref.get(connects)).toBe(1);
				yield* settle;
				yield* Layer.build(contentServer(hub, handlers.layer, frame0));
				const again = yield* clients.clientFor(frame0);
				expect(yield* again.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				expect(yield* Ref.get(connects)).toBe(2);
			}).pipe(Effect.provide(context));
		}),
	);

	it.scoped('fails an in-flight call when the tab navigates', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const context = yield* Layer.build(backgroundLayer(hub, handlers.layer));
			yield* Effect.gen(function* () {
				yield* openPresence(hub, frame0);
				yield* waitPresent((current) => HashMap.has(current, frame0));
				yield* Layer.build(contentServer(hub, handlers.layer, frame0));
				const clients = yield* openClients(hub, connects, context);
				const client = yield* clients.clientFor(frame0);
				const hang = yield* client
					.Hang(undefined)
					.pipe(withPortErrors(), Effect.fork);
				yield* Deferred.await(handlers.started);
				const count = yield* Ref.get(connects);
				yield* hub.navigateTab(1);
				expectDisconnected(yield* Fiber.await(hang));
				yield* waitPresent((current) => !HashMap.has(current, frame0));
				const error = yield* clients.clientFor(frame0).pipe(Effect.flip);
				expect(
					Equal.equals(error, new ReceiverUnavailable({ endpoint: frame0 })),
				).toBe(true);
				expect(yield* Ref.get(connects)).toBe(count);
			}).pipe(Effect.provide(context));
		}),
	);

	it.scoped('keeps another frame when one enters the back/forward cache', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const context = yield* Layer.build(backgroundLayer(hub, handlers.layer));
			yield* Effect.gen(function* () {
				yield* openPresence(hub, frame0);
				yield* openPresence(hub, frame1);
				yield* waitPresent(
					(current) =>
						HashMap.has(current, frame0) && HashMap.has(current, frame1),
				);
				yield* Layer.build(contentServer(hub, handlers.layer, frame0));
				yield* Layer.build(contentServer(hub, handlers.layer, frame1));
				const clients = yield* openClients(hub, connects, context);
				const client0 = yield* clients.clientFor(frame0);
				const client1 = yield* clients.clientFor(frame1);
				expect(yield* client0.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				expect(yield* client1.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				expect(yield* Ref.get(connects)).toBe(2);
				yield* hub.enterBackForwardCache(frame1);
				yield* completesWithoutClock(untilPortGone(hub, frame1));
				expect(yield* client0.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				expect(yield* Ref.get(connects)).toBe(2);
			}).pipe(Effect.provide(context));
		}),
	);

	it.scoped('keeps the cached client across a reconnect overlap', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const context = yield* Layer.build(backgroundLayer(hub, handlers.layer));
			const first = yield* Scope.make();
			const second = yield* Scope.make();
			yield* Effect.gen(function* () {
				yield* openPresence(hub, frame0, first);
				yield* openPresence(hub, frame0, second);
				yield* waitPresent((current) => connectionCount(current, frame0) === 2);
				yield* Layer.build(contentServer(hub, handlers.layer, frame0));
				const clients = yield* openClients(hub, connects, context);
				const client = yield* clients.clientFor(frame0);
				expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				expect(yield* Ref.get(connects)).toBe(1);
				yield* Scope.close(first, Exit.void);
				yield* waitPresent((current) => connectionCount(current, frame0) === 1);
				expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				expect(yield* Ref.get(connects)).toBe(1);
			}).pipe(Effect.provide(context));
		}),
	);

	it.scoped('opens a fresh client after the service worker restarts', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const context = yield* Layer.build(backgroundLayer(hub, handlers.layer));
			yield* Effect.gen(function* () {
				yield* openPresence(hub, frame0);
				yield* waitPresent((current) => HashMap.has(current, frame0));
				yield* Layer.build(contentServer(hub, handlers.layer, frame0));
				const clients = yield* openClients(hub, connects, context);
				const client = yield* clients.clientFor(frame0);
				expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				yield* TestClock.adjust('2 seconds');
				yield* settle;
				const hang = yield* client
					.Hang(undefined)
					.pipe(withPortErrors(), Effect.fork);
				yield* Deferred.await(handlers.started);
				yield* hub.stopServiceWorker;
				expectDisconnected(yield* Fiber.await(hang));
				yield* waitPresent((current) => HashMap.has(current, frame0));
				yield* settle;
				const again = yield* clients.clientFor(frame0);
				expect(yield* again.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				expect(yield* Ref.get(connects)).toBe(2);
			}).pipe(Effect.provide(context));
		}),
	);

	it.scoped('closes outgoing ports with its scope', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const connects = yield* Ref.make(0);
			const context = yield* Layer.build(backgroundLayer(hub, handlers.layer));
			const clientsScope = yield* Scope.make();
			yield* Effect.gen(function* () {
				yield* openPresence(hub, frame0);
				yield* waitPresent((current) => HashMap.has(current, frame0));
				yield* Layer.build(contentServer(hub, handlers.layer, frame0));
				const clients = yield* openClients(
					hub,
					connects,
					context,
					clientsScope,
				);
				const client = yield* clients.clientFor(frame0);
				expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
					'hi',
				);
				const before = yield* hub.openPorts;
				expect(before.some((port) => isBackground(port.from))).toBe(true);
				yield* Scope.close(clientsScope, Exit.void);
				const after = yield* hub.openPorts;
				expect(after.some((port) => isBackground(port.from))).toBe(false);
			}).pipe(Effect.provide(context));
		}),
	);
});
