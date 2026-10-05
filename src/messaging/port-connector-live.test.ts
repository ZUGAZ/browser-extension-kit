/* eslint-disable @typescript-eslint/no-deprecated -- the task requires it.scoped */
import { describe, expect, layer, vi } from '@effect/vitest';
import { Effect, Equal, Exit, Logger, Option, Scope, Stream } from 'effect';

import { Disconnected, ExtensionContextInvalidated } from './connection-errors';
import { Background, Content } from './endpoint';
import { PortConnector } from './port-connector';

const silentLogger = Logger.replace(Logger.defaultLogger, Logger.none);

interface MessageListener {
	(message: unknown): void;
}

interface DisconnectListener {
	(): void;
}

interface StubPort {
	readonly name: string;
	readonly sender?: object;
	readonly postMessage: (message: unknown) => void;
	readonly disconnect: () => void;
	readonly onMessage: {
		readonly addListener: (listener: MessageListener) => void;
		readonly removeListener: (listener: MessageListener) => void;
	};
	readonly onDisconnect: {
		readonly addListener: (listener: DisconnectListener) => void;
		readonly removeListener: (listener: DisconnectListener) => void;
	};
}

interface PortBundle {
	readonly port: StubPort;
	readonly postMessage: ReturnType<typeof vi.fn<(message: unknown) => void>>;
	readonly disconnect: ReturnType<typeof vi.fn<() => void>>;
	readonly messageListeners: Array<MessageListener>;
	readonly disconnectListeners: Array<DisconnectListener>;
}

const makePort = (name: string, sender?: object): PortBundle => {
	const messageListeners: Array<MessageListener> = [];
	const disconnectListeners: Array<DisconnectListener> = [];
	const postMessage = vi.fn<(message: unknown) => void>();
	const disconnect = vi.fn<() => void>();
	const port: StubPort = {
		name,
		...(sender === undefined ? {} : { sender }),
		postMessage,
		disconnect,
		onMessage: {
			addListener: (listener) => {
				messageListeners.push(listener);
			},
			removeListener: (listener) => {
				const index = messageListeners.indexOf(listener);
				if (index >= 0) {
					messageListeners.splice(index, 1);
				}
			},
		},
		onDisconnect: {
			addListener: (listener) => {
				disconnectListeners.push(listener);
			},
			removeListener: (listener) => {
				const index = disconnectListeners.indexOf(listener);
				if (index >= 0) {
					disconnectListeners.splice(index, 1);
				}
			},
		},
	};
	return {
		port,
		postMessage,
		disconnect,
		messageListeners,
		disconnectListeners,
	};
};

interface ChromeEnv {
	readonly runtimeConnect: ReturnType<
		typeof vi.fn<(info?: { readonly name?: string }) => StubPort>
	>;
	readonly tabsConnect: ReturnType<
		typeof vi.fn<
			(
				tabId: number,
				info?: { readonly name?: string; readonly frameId?: number },
			) => StubPort
		>
	>;
	readonly addConnectListener: ReturnType<
		typeof vi.fn<(listener: (port: StubPort) => void) => void>
	>;
	readonly removeConnectListener: ReturnType<
		typeof vi.fn<(listener: (port: StubPort) => void) => void>
	>;
	readonly created: Array<PortBundle>;
	readonly fireConnect: (port: StubPort) => void;
	readonly setExtensionId: (id: string | undefined) => void;
	readonly setLastError: (
		error: { readonly message?: string } | undefined,
	) => void;
}

const installChrome = (): ChromeEnv => {
	let extensionId: string | undefined = 'extension-id';
	let storedLastError: { readonly message?: string } | undefined;
	const created: Array<PortBundle> = [];
	const connectListeners: Array<(port: StubPort) => void> = [];
	const remember = (name: string, sender?: object): StubPort => {
		const bundle = makePort(name, sender);
		created.push(bundle);
		return bundle.port;
	};
	const runtimeConnect = vi.fn<(info?: { readonly name?: string }) => StubPort>(
		(info) => remember(info?.name ?? ''),
	);
	const tabsConnect = vi.fn<
		(
			tabId: number,
			info?: { readonly name?: string; readonly frameId?: number },
		) => StubPort
	>((_tabId, info) => remember(info?.name ?? ''));
	const addConnectListener = vi.fn<
		(listener: (port: StubPort) => void) => void
	>((listener) => {
		connectListeners.push(listener);
	});
	const removeConnectListener = vi.fn<
		(listener: (port: StubPort) => void) => void
	>((listener) => {
		const index = connectListeners.indexOf(listener);
		if (index >= 0) {
			connectListeners.splice(index, 1);
		}
	});

	Object.defineProperty(globalThis, 'chrome', {
		configurable: true,
		writable: true,
		value: {
			runtime: {
				get id() {
					return extensionId;
				},
				get lastError() {
					const current = storedLastError;
					storedLastError = undefined;
					return current;
				},
				connect: runtimeConnect,
				getURL: (path: string) => `chrome-extension://extension-id/${path}`,
				getManifest: () => ({
					background: { service_worker: 'background.js' },
				}),
				onConnect: {
					addListener: addConnectListener,
					removeListener: removeConnectListener,
				},
			},
			tabs: {
				connect: tabsConnect,
			},
		},
	});

	return {
		runtimeConnect,
		tabsConnect,
		addConnectListener,
		removeConnectListener,
		created,
		fireConnect: (port) => {
			for (const listener of connectListeners) {
				listener(port);
			}
		},
		setExtensionId: (id) => {
			extensionId = id;
		},
		setLastError: (error) => {
			storedLastError = error;
		},
	};
};

const loadLive = () => import('./port-connector-live');

const withLive = <A, E, R>(run: (env: ChromeEnv) => Effect.Effect<A, E, R>) =>
	Effect.gen(function* () {
		const env = installChrome();
		const live = yield* Effect.promise(loadLive);
		return yield* run(env).pipe(Effect.provide(live.PortConnectorLive));
	});

const expectFail = <A, E>(exit: Exit.Exit<A, E>, error: E) => {
	expect(Equal.equals(exit, Exit.fail(error))).toBe(true);
};

const bundleAt = (env: ChromeEnv, index: number) => {
	const bundle = env.created[index];
	if (bundle === undefined) {
		return Effect.die('missing port');
	}
	return Effect.succeed(bundle);
};

describe('PortConnectorLive', () => {
	layer(silentLogger)((it) => {
		it.effect('imports with no chrome global', () =>
			Effect.gen(function* () {
				Reflect.deleteProperty(globalThis, 'chrome');
				yield* Effect.promise(loadLive);
			}),
		);

		it.scoped('connects to the background and forwards messages', () =>
			withLive((env) =>
				Effect.gen(function* () {
					yield* Effect.scoped(
						Effect.gen(function* () {
							const connector = yield* PortConnector;
							const handle = yield* connector.connect(new Background(), 'n');
							expect(env.runtimeConnect).toHaveBeenCalledWith({ name: 'n' });
							yield* handle.send({ n: 1 });
							const bundle = yield* bundleAt(env, 0);
							expect(bundle.postMessage).toHaveBeenCalledWith({ n: 1 });
							const listener = bundle.messageListeners[0];
							if (listener === undefined) {
								return yield* Effect.die('missing message listener');
							}
							listener({ hello: true });
							const message = yield* Stream.runHead(handle.messages);
							if (Option.isNone(message)) {
								return yield* Effect.die('missing message');
							}
							expect(message.value).toEqual({ hello: true });
						}),
					);
					const bundle = yield* bundleAt(env, 0);
					expect(bundle.disconnect).toHaveBeenCalledTimes(1);
				}),
			),
		);

		it.scoped('connects to a content frame', () =>
			withLive((env) =>
				Effect.gen(function* () {
					const connector = yield* PortConnector;
					yield* connector.connect(new Content({ tabId: 3, frameId: 2 }), 'n');
					expect(env.tabsConnect).toHaveBeenCalledWith(3, {
						name: 'n',
						frameId: 2,
					});
					expect(env.runtimeConnect).not.toHaveBeenCalled();
				}),
			),
		);

		it.scoped('maps onDisconnect lastError into Disconnected', () =>
			withLive((env) =>
				Effect.gen(function* () {
					const connector = yield* PortConnector;
					const handle = yield* connector.connect(new Background(), 'n');
					env.setLastError({ message: 'x' });
					const bundle = yield* bundleAt(env, 0);
					const listener = bundle.disconnectListeners[0];
					if (listener === undefined) {
						return yield* Effect.die('missing disconnect listener');
					}
					listener();
					expectFail(
						yield* Effect.exit(handle.closed),
						new Disconnected({ detail: 'x' }),
					);
				}),
			),
		);

		it.scoped('maps an invalid runtime id on disconnect', () =>
			withLive((env) =>
				Effect.gen(function* () {
					const connector = yield* PortConnector;
					const handle = yield* connector.connect(new Background(), 'n');
					env.setExtensionId(undefined);
					const bundle = yield* bundleAt(env, 0);
					const listener = bundle.disconnectListeners[0];
					if (listener === undefined) {
						return yield* Effect.die('missing disconnect listener');
					}
					listener();
					expectFail(
						yield* Effect.exit(handle.closed),
						new ExtensionContextInvalidated(),
					);
				}),
			),
		);

		it.scoped('maps a throwing postMessage to Disconnected', () =>
			withLive((env) =>
				Effect.gen(function* () {
					const connector = yield* PortConnector;
					const handle = yield* connector.connect(new Background(), 'n');
					const bundle = yield* bundleAt(env, 0);
					bundle.postMessage.mockImplementation(() => {
						throw new Error('boom');
					});
					expectFail(
						yield* Effect.exit(handle.send('x')),
						new Disconnected({}),
					);
				}),
			),
		);

		it.scoped('fails connect when the runtime id is missing', () =>
			withLive((env) =>
				Effect.gen(function* () {
					env.setExtensionId(undefined);
					const connector = yield* PortConnector;
					expectFail(
						yield* Effect.exit(connector.connect(new Background(), 'n')),
						new ExtensionContextInvalidated(),
					);
					expect(env.runtimeConnect).not.toHaveBeenCalled();
				}),
			),
		);

		it.scoped(
			'listens for the named port and drops the listener with the scope',
			() =>
				withLive((env) =>
					Effect.gen(function* () {
						const connector = yield* PortConnector;
						const scope = yield* Scope.make();
						const incoming = yield* connector
							.listen('n')
							.pipe(Scope.extend(scope));
						expect(env.addConnectListener).toHaveBeenCalledTimes(1);

						const other = makePort('other');
						env.fireConnect(other.port);
						expect(other.disconnect).not.toHaveBeenCalled();

						const foreign = makePort('n', { id: 'foreign' });
						env.fireConnect(foreign.port);
						expect(foreign.disconnect).toHaveBeenCalledTimes(1);

						const content = makePort('n', {
							id: 'extension-id',
							url: 'https://example.com/library',
							frameId: 2,
							tab: { id: 4 },
						});
						env.fireConnect(content.port);
						const accepted = yield* Stream.runHead(incoming);
						if (Option.isNone(accepted)) {
							return yield* Effect.die('missing accepted port');
						}
						expect(
							Equal.equals(
								accepted.value.peer,
								new Content({ tabId: 4, frameId: 2 }),
							),
						).toBe(true);

						yield* Scope.close(scope, Exit.void);
						expect(env.removeConnectListener).toHaveBeenCalledTimes(1);
						expect(env.removeConnectListener).toHaveBeenCalledWith(
							env.addConnectListener.mock.calls[0]?.[0],
						);
					}),
				),
		);
	});
});
