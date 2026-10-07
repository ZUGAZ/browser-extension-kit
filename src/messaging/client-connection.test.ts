/* eslint-disable @typescript-eslint/no-deprecated -- the task requires it.scoped */
import { describe, expect, it } from '@effect/vitest';
import {
	Deferred,
	Effect,
	Exit,
	Layer,
	Option,
	Queue,
	Ref,
	Schedule,
	Schema,
	Scope,
	Stream,
	SubscriptionRef,
	TestClock,
} from 'effect';

import { ClientConnection } from './client-connection';
import { Disconnected } from './connection-errors';
import { Background, Content, Popup } from './endpoint';
import {
	countConnects,
	settle,
	silentLogger,
} from './example-rpcs.test-support';
import { layerPortServer, makeBackgroundClient } from './index';
import {
	CounterRpcs,
	CounterRef,
	makeCounterHandlers,
	CounterState,
} from './watch-state-example.test-support';
import { makeFakePageLifecycle, makeFakePortHub } from '../testing/index';

const rpcName = 'kit';

const counter = (count: number) =>
	new CounterState({ count, ownerTab: Option.none() });

const isDisconnected = Schema.is(Disconnected);

describe('client connection', () => {
	it.scoped('shows connected, then connecting, then connected again', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const lifecycle = yield* makeFakePageLifecycle;
			const ref = yield* SubscriptionRef.make(counter(0));
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
			const { client, connection } = yield* makeBackgroundClient(CounterRpcs, {
				name: rpcName,
				reconnectSchedule: Schedule.exponential('100 millis', 2),
			}).pipe(
				Effect.provide(hub.layerFor(new Popup())),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			const seen = yield* Queue.unbounded<ClientConnection>();
			yield* connection.pipe(
				Stream.runForEach((status) => Queue.offer(seen, status)),
				Effect.forkScoped,
			);
			yield* client
				.WatchCounter(undefined)
				.pipe(Stream.take(1), Stream.runDrain);
			const connected = yield* nextTagged(seen, 'Connected');
			expect(ClientConnection.$is('Connected')(connected)).toBe(true);

			yield* TestClock.adjust('2 seconds');
			yield* hub.stopServiceWorker;
			const connecting = yield* nextTagged(seen, 'Connecting');
			expect(ClientConnection.$is('Connecting')(connecting)).toBe(true);
			const restored = yield* nextTagged(seen, 'Connected');
			expect(ClientConnection.$is('Connected')(restored)).toBe(true);
		}),
	);

	it.scoped('ends at Terminated when the reconnect schedule is exhausted', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const lifecycle = yield* makeFakePageLifecycle;
			const ref = yield* SubscriptionRef.make(counter(0));
			const handlers = yield* makeCounterHandlers.pipe(
				Effect.provideService(CounterRef, ref),
			);
			yield* Deferred.succeed(handlers.hold, undefined);
			const serverScope = yield* Scope.make();
			yield* Layer.build(
				layerPortServer(CounterRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			).pipe(Effect.provideService(Scope.Scope, serverScope));
			const connects = yield* Ref.make(0);
			const { client, connection } = yield* makeBackgroundClient(CounterRpcs, {
				name: rpcName,
				reconnectSchedule: Schedule.recurs(2),
			}).pipe(
				Effect.provide(
					countConnects(connects).pipe(
						Layer.provide(hub.layerFor(new Content({ tabId: 1, frameId: 0 }))),
					),
				),
				Effect.provide(lifecycle.layer),
				Effect.provide(silentLogger),
			);
			yield* client
				.WatchCounter(undefined)
				.pipe(Stream.take(1), Stream.runDrain);
			const seen = yield* Queue.unbounded<ClientConnection>();
			yield* connection.pipe(
				Stream.runForEach((status) => Queue.offer(seen, status)),
				Effect.forkScoped,
			);
			yield* Scope.close(serverScope, Exit.void);
			yield* settle;
			const terminal = yield* nextTagged(seen, 'Terminated');
			expect(
				ClientConnection.$is('Terminated')(terminal) &&
					isDisconnected(terminal.error),
			).toBe(true);
		}),
	);
});

const nextTagged = (
	queue: Queue.Queue<ClientConnection>,
	tag: 'Connecting' | 'Connected' | 'Terminated',
) =>
	Effect.gen(function* () {
		for (let step = 0; step < 12; step++) {
			const status = yield* Queue.take(queue);
			if (ClientConnection.$is(tag)(status)) {
				return status;
			}
		}
		return yield* Effect.die(`missing ${tag}`);
	});
