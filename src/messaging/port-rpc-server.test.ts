/* eslint-disable @typescript-eslint/no-deprecated -- the task requires it.scoped */
import { describe, expect, it } from '@effect/vitest';
import {
	Cause,
	Context,
	Deferred,
	Effect,
	Equal,
	Exit,
	Fiber,
	HashMap,
	Layer,
	ManagedRuntime,
	Option,
	Schema,
	Scope,
	Stream,
} from 'effect';

import { EchoRejected } from './example-rpcs.test-support';
import {
	ExampleRpcs,
	makeExampleHandlers,
	settle,
	silentLogger,
} from './example-rpcs.test-support';
import {
	Background,
	Content,
	Disconnected,
	ExtensionPage,
	layerPortServer,
	makeBackgroundClient,
	makeContentClient,
	PageLifecycleNone,
	Popup,
	withPortErrors,
	withPortErrorsStream,
} from './index';
import { PortConnector } from './port-connector';
import { PortPeers } from './port-peers';
import { decodeFromServerFrame, isExitFrame } from './rpc-frames';
import { makeFakePortHub } from '../testing/index';

const rpcName = 'rpc';

const exitSchema = Schema.Exit({
	success: Schema.Unknown,
	failure: Schema.Unknown,
	defect: Schema.Unknown,
});

const request = (
	id: string,
	payload: unknown,
): {
	readonly _tag: string;
	readonly id: string;
	readonly tag: string;
	readonly payload: unknown;
	readonly headers: ReadonlyArray<readonly [string, string]>;
} => ({
	_tag: 'Request',
	id,
	tag: 'Echo',
	payload,
	headers: [],
});

const numbersThrough = (upTo: number): Array<number> =>
	Array.from({ length: upTo }, (_, index) => index + 1);

describe('port rpc server', () => {
	it.scoped('round trip', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const { client } = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
			}).pipe(
				Effect.provide(hub.layerFor(new Popup())),
				Effect.provide(PageLifecycleNone),
				Effect.provide(silentLogger),
			);
			expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
				'hi',
			);
		}),
	);

	it.scoped('typed handler error', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const { client } = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
			}).pipe(
				Effect.provide(hub.layerFor(new Popup())),
				Effect.provide(PageLifecycleNone),
				Effect.provide(silentLogger),
			);
			const error = yield* client
				.Echo({ text: 'reject' })
				.pipe(withPortErrors(), Effect.flip);
			expect(
				Equal.equals(error, new EchoRejected({ reason: 'rejected' })),
			).toBe(true);
		}),
	);

	it.scoped('streams past the ack buffer', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const { client } = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
			}).pipe(
				Effect.provide(hub.layerFor(new Popup())),
				Effect.provide(PageLifecycleNone),
				Effect.provide(silentLogger),
			);
			const values = yield* client
				.Counter({ upTo: 20 })
				.pipe(withPortErrorsStream, Stream.runCollect);
			expect(Array.from(values)).toEqual(numbersThrough(20));
		}),
	);

	it.scoped('caller is the sender endpoint', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const ask = (endpoint: Popup | ExtensionPage | Content) =>
				makeBackgroundClient(ExampleRpcs, { name: rpcName }).pipe(
					Effect.provide(hub.layerFor(endpoint)),
					Effect.provide(PageLifecycleNone),
					Effect.provide(silentLogger),
					Effect.flatMap(({ client }) =>
						client.WhoAmI(undefined).pipe(withPortErrors()),
					),
				);
			expect(Equal.equals(yield* ask(new Popup()), new Popup())).toBe(true);
			expect(
				Equal.equals(
					yield* ask(new ExtensionPage({ tabId: 5 })),
					new ExtensionPage({ tabId: 5 }),
				),
			).toBe(true);
			expect(
				Equal.equals(
					yield* ask(new Content({ tabId: 1, frameId: 0 })),
					new Content({ tabId: 1, frameId: 0 }),
				),
			).toBe(true);

			const frame = new Content({ tabId: 1, frameId: 0 });
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: 'to-content' }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(frame)),
					Layer.provide(silentLogger),
				),
			);
			const fromBackground = yield* makeContentClient(ExampleRpcs, {
				name: 'to-content',
				target: frame,
			}).pipe(
				Effect.provide(hub.layerFor(new Background())),
				Effect.provide(silentLogger),
				Effect.flatMap((client) =>
					client.WhoAmI(undefined).pipe(withPortErrors()),
				),
			);
			expect(Equal.equals(fromBackground, new Background())).toBe(true);
		}),
	);

	it.scoped('peers change as ports open and close', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const context = yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const peers = Context.get(context, PortPeers);
			const { client } = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
			}).pipe(
				Effect.provide(hub.layerFor(new Popup())),
				Effect.provide(PageLifecycleNone),
				Effect.provide(silentLogger),
			);
			expect(yield* client.Echo({ text: 'hi' }).pipe(withPortErrors())).toBe(
				'hi',
			);
			const updates = yield* peers.changes.pipe(
				Stream.take(2),
				Stream.runCollect,
				Effect.fork,
			);
			yield* settle;
			yield* hub.closePopup;
			const [connected, closed] = Array.from(yield* Fiber.join(updates));
			if (connected === undefined || closed === undefined) {
				return yield* Effect.die('expected two peer snapshots');
			}
			const endpoints = Array.from(HashMap.values(connected));
			expect(endpoints.length).toBe(1);
			const endpoint = endpoints[0];
			if (endpoint === undefined) {
				return yield* Effect.die('missing peer');
			}
			expect(Equal.equals(endpoint, new Popup())).toBe(true);
			expect(HashMap.size(closed)).toBe(0);
		}),
	);

	it.scoped('drops a bad frame and keeps the port open', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const handle = yield* PortConnector.pipe(
				Effect.flatMap((connector) =>
					connector.connect(new Background(), rpcName),
				),
				Effect.provide(hub.layerFor(new Popup())),
			);
			const responses = yield* handle.messages.pipe(
				Stream.mapEffect((raw) => {
					const frame = decodeFromServerFrame(raw);
					if (Option.isNone(frame) || !isExitFrame(frame.value)) {
						return Effect.die('expected an exit frame');
					}
					return Schema.decodeUnknown(exitSchema)(frame.value.exit);
				}),
				Stream.take(2),
				Stream.runCollect,
				Effect.fork,
			);
			yield* settle;
			yield* handle.send({ foo: 1 });
			yield* handle.send(request('1', { text: 1 }));
			yield* handle.send(request('2', { text: 'next' }));
			const exits = Array.from(yield* Fiber.join(responses));
			expect(exits.length).toBe(2);
			const sawDie = exits.some(
				(exit) => Exit.isFailure(exit) && Cause.isDie(exit.cause),
			);
			const sawSuccess = exits.some(
				(exit) => Exit.isSuccess(exit) && exit.value === 'next',
			);
			expect(sawDie).toBe(true);
			expect(sawSuccess).toBe(true);
		}),
	);

	it.scoped('a handler defect fails only that request', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			const { client } = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
			}).pipe(
				Effect.provide(hub.layerFor(new Popup())),
				Effect.provide(PageLifecycleNone),
				Effect.provide(silentLogger),
			);
			const hang = yield* client
				.Hang(undefined)
				.pipe(withPortErrors(), Effect.fork);
			yield* Deferred.await(handlers.started);
			const exploded = yield* client
				.Explode(undefined)
				.pipe(Effect.exit, Effect.fork);
			yield* settle;
			expect(Option.isSome(yield* Fiber.poll(exploded))).toBe(true);
			expect(Option.isNone(yield* Fiber.poll(hang))).toBe(true);
		}),
	);

	it.scoped('builds synchronously for service worker startup', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const runtime = ManagedRuntime.make(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			);
			expect(() => {
				runtime.runSync(Effect.void);
			}).not.toThrow();
			const answer = yield* Effect.promise(() =>
				runtime.runPromise(
					makeBackgroundClient(ExampleRpcs, { name: rpcName }).pipe(
						Effect.flatMap(({ client }) =>
							client.Echo({ text: 'hi' }).pipe(withPortErrors()),
						),
						Effect.provide(hub.layerFor(new Popup())),
						Effect.provide(PageLifecycleNone),
						Effect.scoped,
					),
				),
			);
			expect(answer).toBe('hi');
			const hang = runtime.runFork(
				makeBackgroundClient(ExampleRpcs, { name: rpcName }).pipe(
					Effect.flatMap(({ client }) =>
						client.Hang(undefined).pipe(withPortErrors()),
					),
					Effect.provide(hub.layerFor(new Popup())),
					Effect.provide(PageLifecycleNone),
					Effect.scoped,
				),
			);
			yield* Effect.promise(() =>
				runtime.runPromise(Deferred.await(handlers.started)),
			);
			yield* Effect.promise(() => runtime.dispose());
			const exit = yield* Fiber.await(hang);
			expect(Exit.isFailure(exit)).toBe(true);
			if (Exit.isFailure(exit)) {
				const error = Cause.failureOption(exit.cause);
				expect(
					Option.isSome(error) && Schema.is(Disconnected)(error.value),
				).toBe(true);
			}
		}),
	);

	it.scoped('scope close interrupts a running handler', () =>
		Effect.gen(function* () {
			const hub = yield* makeFakePortHub;
			const handlers = yield* makeExampleHandlers;
			const serverScope = yield* Scope.make();
			yield* Layer.build(
				layerPortServer(ExampleRpcs, { name: rpcName }).pipe(
					Layer.provide(handlers.layer),
					Layer.provide(hub.layerFor(new Background())),
					Layer.provide(silentLogger),
				),
			).pipe(Effect.provideService(Scope.Scope, serverScope));
			const { client } = yield* makeBackgroundClient(ExampleRpcs, {
				name: rpcName,
			}).pipe(
				Effect.provide(hub.layerFor(new Popup())),
				Effect.provide(PageLifecycleNone),
				Effect.provide(silentLogger),
			);
			const hang = yield* client
				.Hang(undefined)
				.pipe(withPortErrors(), Effect.fork);
			yield* Deferred.await(handlers.started);
			yield* Scope.close(serverScope, Exit.void);
			yield* Deferred.await(handlers.interrupted);
			yield* Fiber.interrupt(hang);
		}),
	);
});
