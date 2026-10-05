/* eslint-disable @typescript-eslint/no-deprecated -- the task requires it.scoped */
import { describe, expect, layer } from '@effect/vitest';
import {
	Chunk,
	Effect,
	Equal,
	Exit,
	Fiber,
	Logger,
	Option,
	Scope,
	Stream,
} from 'effect';

import {
	Disconnected,
	ExtensionContextInvalidated,
} from '../messaging/connection-errors';
import {
	Background,
	Content,
	ExtensionPage,
	Popup,
} from '../messaging/endpoint';
import {
	PortConnector,
	type AcceptedPort,
	type PortHandle,
} from '../messaging/port-connector';
import { makeFakePortHub, type FakePortHub } from './fake-port-hub';

const silentLogger = Logger.replace(Logger.defaultLogger, Logger.none);

const pullOne = <A, E>(stream: Stream.Stream<A, E>) =>
	stream.pipe(
		Stream.runHead,
		Effect.flatMap((value) =>
			Option.match(value, {
				onNone: () => Effect.die('expected a stream value'),
				onSome: (item) => Effect.succeed(item),
			}),
		),
	);

const collect = <A, E>(stream: Stream.Stream<A, E>, count: number) =>
	stream.pipe(Stream.take(count), Stream.runCollect, Effect.map(Chunk.toArray));

const expectFail = <A, E>(exit: Exit.Exit<A, E>, error: E) => {
	expect(Equal.equals(exit, Exit.fail(error))).toBe(true);
};

const connectorFor = (
	hub: FakePortHub,
	context: Parameters<FakePortHub['layerFor']>[0],
) => PortConnector.pipe(Effect.provide(hub.layerFor(context)));

describe('fake port hub', () => {
	layer(silentLogger)((it) => {
		it.scoped('delivers ordered messages and the content peer', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const content = yield* connectorFor(
					hub,
					new Content({ tabId: 1, frameId: 0 }),
				);
				const background = yield* connectorFor(hub, new Background());
				const incoming = yield* background.listen('channel');
				const client = yield* content.connect(new Background(), 'channel');
				const server = yield* pullOne(incoming);

				expect(
					Equal.equals(server.peer, new Content({ tabId: 1, frameId: 0 })),
				).toBe(true);
				yield* client.send('c1');
				yield* server.send('s1');
				yield* client.send('c2');
				yield* server.send('s2');

				expect(yield* collect(server.messages, 2)).toEqual(['c1', 'c2']);
				expect(yield* collect(client.messages, 2)).toEqual(['s1', 's2']);
			}),
		);

		it.scoped('classifies popup, extension page, and background peers', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const background = yield* connectorFor(hub, new Background());
				const popup = yield* connectorFor(hub, new Popup());
				const page = yield* connectorFor(hub, new ExtensionPage({ tabId: 5 }));
				const content = yield* connectorFor(
					hub,
					new Content({ tabId: 1, frameId: 0 }),
				);
				const incoming = yield* background.listen('channel');
				yield* popup.connect(new Background(), 'channel');
				yield* page.connect(new Background(), 'channel');
				const accepted = yield* collect(incoming, 2);

				expect(Equal.equals(accepted[0]?.peer, new Popup())).toBe(true);
				expect(
					Equal.equals(accepted[1]?.peer, new ExtensionPage({ tabId: 5 })),
				).toBe(true);

				const contentIncoming = yield* content.listen('channel');
				yield* background.connect(
					new Content({ tabId: 1, frameId: 0 }),
					'channel',
				);
				const contentPort = yield* pullOne(contentIncoming);
				expect(Equal.equals(contentPort.peer, new Background())).toBe(true);
			}),
		);

		it.scoped('ignores a connect on another name', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const background = yield* connectorFor(hub, new Background());
				const content = yield* connectorFor(
					hub,
					new Content({ tabId: 1, frameId: 0 }),
				);
				const incoming = yield* background.listen('a');
				const seen: Array<AcceptedPort> = [];
				yield* incoming.pipe(
					Stream.runForEach((port) =>
						Effect.sync(() => {
							seen.push(port);
						}),
					),
					Effect.forkScoped,
				);
				yield* Effect.yieldNow();
				const client = yield* content.connect(new Background(), 'b');
				yield* Effect.yieldNow();

				expect(seen).toEqual([]);
				expectFail(
					yield* Effect.exit(client.closed),
					new Disconnected({ detail: 'no receiver' }),
				);
			}),
		);

		it.scoped('buffers messages posted before the reader starts', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const content = yield* connectorFor(
					hub,
					new Content({ tabId: 1, frameId: 0 }),
				);
				const background = yield* connectorFor(hub, new Background());
				const incoming = yield* background.listen('channel');
				const client = yield* content.connect(new Background(), 'channel');
				yield* client.send('one');
				yield* client.send('two');
				const server = yield* pullOne(incoming);

				expect(yield* collect(server.messages, 2)).toEqual(['one', 'two']);
			}),
		);

		it.scoped('closing the client scope disconnects the server', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const content = yield* connectorFor(
					hub,
					new Content({ tabId: 1, frameId: 0 }),
				);
				const background = yield* connectorFor(hub, new Background());
				const incoming = yield* background.listen('channel');
				const scope = yield* Scope.make();
				const client = yield* content
					.connect(new Background(), 'channel')
					.pipe(Scope.extend(scope));
				const server = yield* pullOne(incoming);

				yield* Scope.close(scope, Exit.void);

				expect(
					Exit.isSuccess(yield* Effect.exit(Stream.runDrain(server.messages))),
				).toBe(true);
				expectFail(yield* Effect.exit(server.closed), new Disconnected({}));
				yield* client.closed;
				expectFail(
					yield* Effect.exit(client.send('late')),
					new Disconnected({ detail: 'closed locally' }),
				);
			}),
		);

		it.scoped('closing the server disconnects the client', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const pair = yield* openPair(hub);
				yield* pair.server.close;

				expect(
					Exit.isSuccess(
						yield* Effect.exit(Stream.runDrain(pair.client.messages)),
					),
				).toBe(true);
				expectFail(
					yield* Effect.exit(pair.client.closed),
					new Disconnected({}),
				);
				expectFail(
					yield* Effect.exit(pair.client.send('late')),
					new Disconnected({}),
				);
			}),
		);

		it.scoped('closePopup leaves an unrelated port open', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const background = yield* connectorFor(hub, new Background());
				const popup = yield* connectorFor(hub, new Popup());
				const content = yield* connectorFor(
					hub,
					new Content({ tabId: 1, frameId: 0 }),
				);
				const incoming = yield* background.listen('channel');
				const popupClient = yield* popup.connect(new Background(), 'channel');
				const contentClient = yield* content.connect(
					new Background(),
					'channel',
				);
				const accepted = yield* collect(incoming, 2);
				const popupServer = accepted.find((port) =>
					Equal.equals(port.peer, new Popup()),
				);
				if (popupServer === undefined) {
					return yield* Effect.die('missing popup port');
				}

				yield* hub.closePopup;

				expectFail(
					yield* Effect.exit(popupClient.closed),
					new Disconnected({}),
				);
				expectFail(
					yield* Effect.exit(popupServer.closed),
					new Disconnected({}),
				);
				const open = yield* hub.openPorts;
				expect(open).toHaveLength(1);
				expect(open[0]?.name).toBe('channel');
				expect(
					Equal.equals(open[0]?.from, new Content({ tabId: 1, frameId: 0 })),
				).toBe(true);
				expect(Equal.equals(open[0]?.to, new Background())).toBe(true);
				yield* contentClient.send('still-open');
			}),
		);

		it.scoped(
			'stopServiceWorker disconnects background ports and allows a fresh connect',
			() =>
				Effect.gen(function* () {
					const hub = yield* makeFakePortHub;
					const pair = yield* openPair(hub);
					yield* hub.stopServiceWorker;

					expectFail(
						yield* Effect.exit(pair.client.closed),
						new Disconnected({}),
					);
					expectFail(
						yield* Effect.exit(pair.server.closed),
						new Disconnected({}),
					);
					expect(yield* hub.openPorts).toEqual([]);

					const content = yield* connectorFor(
						hub,
						new Content({ tabId: 1, frameId: 0 }),
					);
					const fresh = yield* content.connect(new Background(), 'channel');
					yield* fresh.send('again');
					expect(yield* hub.openPorts).toHaveLength(1);
				}),
		);

		it.scoped('enterBackForwardCache leaves another frame open', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const frame0 = new Content({ tabId: 1, frameId: 0 });
				const frame1 = new Content({ tabId: 1, frameId: 1 });
				const background = yield* connectorFor(hub, new Background());
				const first = yield* connectorFor(hub, frame0);
				const second = yield* connectorFor(hub, frame1);
				const incoming = yield* background.listen('channel');
				const client0 = yield* first.connect(new Background(), 'channel');
				const client1 = yield* second.connect(new Background(), 'channel');
				const accepted = yield* collect(incoming, 2);
				const server0 = accepted.find((port) =>
					Equal.equals(port.peer, frame0),
				);
				if (server0 === undefined) {
					return yield* Effect.die('missing frame port');
				}

				yield* hub.enterBackForwardCache(frame0);

				expectFail(yield* Effect.exit(client0.closed), new Disconnected({}));
				expectFail(yield* Effect.exit(server0.closed), new Disconnected({}));
				const open = yield* hub.openPorts;
				expect(open).toHaveLength(1);
				expect(Equal.equals(open[0]?.from, frame1)).toBe(true);
				yield* client1.send('still-open');
			}),
		);

		it.scoped('navigateTab removes the frame listener', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const frame = new Content({ tabId: 1, frameId: 0 });
				const content = yield* connectorFor(hub, frame);
				const background = yield* connectorFor(hub, new Background());
				yield* content.listen('channel');
				const first = yield* background.connect(frame, 'channel');
				yield* first.send('up');

				yield* hub.navigateTab(1);

				const second = yield* background.connect(frame, 'channel');
				expectFail(
					yield* Effect.exit(second.closed),
					new Disconnected({ detail: 'no receiver' }),
				);
			}),
		);

		it.scoped(
			'invalidateExtensionContext fails the content end and later connects',
			() =>
				Effect.gen(function* () {
					const hub = yield* makeFakePortHub;
					const frame = new Content({ tabId: 1, frameId: 0 });
					const pair = yield* openPair(hub, frame);
					yield* hub.invalidateExtensionContext(frame);

					expectFail(
						yield* Effect.exit(pair.client.closed),
						new ExtensionContextInvalidated(),
					);
					expectFail(
						yield* Effect.exit(pair.server.closed),
						new Disconnected({}),
					);
					expectFail(
						yield* Effect.exit(pair.client.send('late')),
						new ExtensionContextInvalidated(),
					);
					const content = yield* connectorFor(hub, frame);
					expectFail(
						yield* Effect.exit(content.connect(new Background(), 'channel')),
						new ExtensionContextInvalidated(),
					);
				}),
		);

		it.scoped('closing the listen scope ends accepted ports', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const content = yield* connectorFor(
					hub,
					new Content({ tabId: 1, frameId: 0 }),
				);
				const background = yield* connectorFor(hub, new Background());
				const scope = yield* Scope.make();
				const incoming = yield* background
					.listen('channel')
					.pipe(Scope.extend(scope));
				const client = yield* content.connect(new Background(), 'channel');
				const drained = yield* Stream.runDrain(incoming).pipe(
					Effect.forkScoped,
				);
				yield* Effect.yieldNow();

				yield* Scope.close(scope, Exit.void);

				yield* Fiber.join(drained);
				expectFail(yield* Effect.exit(client.closed), new Disconnected({}));
			}),
		);

		it.scoped('round-trips payloads through JSON', () =>
			Effect.gen(function* () {
				const hub = yield* makeFakePortHub;
				const pair = yield* openPair(hub);
				yield* pair.client.send({ at: new Date(0), gone: undefined });
				const message = yield* pullOne(pair.server.messages);
				expect(message).toEqual({ at: '1970-01-01T00:00:00.000Z' });
			}),
		);
	});
});

const openPair = (
	hub: FakePortHub,
	frame: Content = new Content({ tabId: 1, frameId: 0 }),
) =>
	Effect.gen(function* () {
		const content = yield* connectorFor(hub, frame);
		const background = yield* connectorFor(hub, new Background());
		const incoming = yield* background.listen('channel');
		const client = yield* content.connect(new Background(), 'channel');
		const server = yield* pullOne(incoming);
		const pair: { readonly client: PortHandle; readonly server: AcceptedPort } =
			{ client, server };
		return pair;
	});
