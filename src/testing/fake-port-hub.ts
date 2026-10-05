import {
	Effect,
	Either,
	Equal,
	Layer,
	Option,
	Ref,
	Schema,
	type Scope,
} from 'effect';

import {
	Disconnected,
	ExtensionContextInvalidated,
	type PortClosedError,
} from '../messaging/connection-errors';
import {
	Content,
	isBackground,
	isContent,
	isExtensionPage,
	isPopup,
	type ConnectTarget,
	type Endpoint,
} from '../messaging/endpoint';
import {
	type ExtensionIdentity,
	type SenderInfo,
} from '../messaging/endpoint-from-sender';
import { openPort, acceptPorts, type RawPort } from '../messaging/port-handle';
import { PortConnector } from '../messaging/port-connector';

const extensionId = 'fake-extension-id';
const origin = `chrome-extension://${extensionId}`;
const serviceWorkerUrl = `${origin}/background.js`;

const identity: ExtensionIdentity = {
	id: extensionId,
	origin,
	serviceWorkerUrl: Option.some(serviceWorkerUrl),
};

export interface FakePortHub {
	readonly layerFor: (context: Endpoint) => Layer.Layer<PortConnector>;
	readonly closePopup: Effect.Effect<void>;
	readonly stopServiceWorker: Effect.Effect<void>;
	readonly enterBackForwardCache: (frame: Content) => Effect.Effect<void>;
	readonly navigateTab: (tabId: number) => Effect.Effect<void>;
	readonly invalidateExtensionContext: (frame: Content) => Effect.Effect<void>;
	readonly openPorts: Effect.Effect<
		ReadonlyArray<{
			readonly name: string;
			readonly from: Endpoint;
			readonly to: Endpoint;
		}>
	>;
}

interface FakeEnd {
	messageListeners: Array<(message: unknown) => void>;
	disconnectListeners: Array<(error: PortClosedError) => void>;
	pendingMessages: Array<unknown>;
	pendingDisconnect: PortClosedError | undefined;
	closedError: PortClosedError | undefined;
	peer: FakeEnd | undefined;
}

interface Listener {
	readonly context: Endpoint;
	readonly name: string;
	readonly onPort: (raw: RawPort, sender: SenderInfo) => void;
}

interface OpenLink {
	readonly name: string;
	readonly from: Endpoint;
	readonly to: Endpoint;
	readonly client: FakeEnd;
	readonly server: FakeEnd;
}

interface Registry {
	listeners: Array<Listener>;
	ports: Array<OpenLink>;
	invalidated: Array<Content>;
}

const decodeJson = Schema.decodeUnknownEither(Schema.parseJson());

const roundTrip = (message: unknown): Either.Either<unknown, Disconnected> =>
	Either.try({
		try: (): unknown => JSON.stringify(message),
		catch: () => new Disconnected({}),
	}).pipe(
		Either.flatMap((encoded) => {
			if (typeof encoded !== 'string') {
				return Either.left(new Disconnected({}));
			}
			return decodeJson(encoded).pipe(
				Either.mapLeft(() => new Disconnected({})),
			);
		}),
	);

const makeEnd = (): FakeEnd => ({
	messageListeners: [],
	disconnectListeners: [],
	pendingMessages: [],
	pendingDisconnect: undefined,
	closedError: undefined,
	peer: undefined,
});

const failEnd = (end: FakeEnd, error: PortClosedError): void => {
	if (end.closedError !== undefined) {
		return;
	}
	end.closedError = error;
	if (end.disconnectListeners.length === 0) {
		end.pendingDisconnect = error;
		return;
	}
	for (const listener of end.disconnectListeners) {
		listener(error);
	}
};

const deliver = (end: FakeEnd, message: unknown): void => {
	if (end.messageListeners.length === 0) {
		end.pendingMessages.push(message);
		return;
	}
	for (const listener of end.messageListeners) {
		listener(message);
	}
};

const toRaw = (end: FakeEnd, forget: () => void): RawPort => ({
	postMessage: (message) => {
		if (end.closedError !== undefined) {
			return Either.left(end.closedError);
		}
		const peer = end.peer;
		if (peer === undefined) {
			return Either.left(new Disconnected({}));
		}
		if (peer.closedError !== undefined) {
			return Either.left(peer.closedError);
		}
		return roundTrip(message).pipe(
			Either.map((decoded) => {
				deliver(peer, decoded);
			}),
		);
	},
	disconnect: () => {
		if (end.closedError !== undefined) {
			return;
		}
		end.closedError = new Disconnected({ detail: 'closed locally' });
		forget();
		const peer = end.peer;
		if (peer !== undefined) {
			failEnd(peer, new Disconnected({}));
		}
	},
	onMessage: (listener) => {
		end.messageListeners.push(listener);
		const pending = end.pendingMessages.splice(0, end.pendingMessages.length);
		for (const message of pending) {
			listener(message);
		}
	},
	onDisconnect: (listener) => {
		end.disconnectListeners.push(listener);
		if (end.pendingDisconnect !== undefined) {
			listener(end.pendingDisconnect);
		}
	},
});

const senderFor = (context: Endpoint): SenderInfo => {
	if (isBackground(context)) {
		return { id: extensionId, url: serviceWorkerUrl };
	}
	if (isPopup(context)) {
		return { id: extensionId, url: `${origin}/popup.html` };
	}
	if (isExtensionPage(context)) {
		return {
			id: extensionId,
			url: `${origin}/page.html`,
			tab: { id: context.tabId },
		};
	}
	return {
		id: extensionId,
		url: 'https://example.com/',
		tab: { id: context.tabId },
		frameId: context.frameId,
	};
};

const tabIdOf = (endpoint: Endpoint): Option.Option<number> => {
	if (isContent(endpoint) || isExtensionPage(endpoint)) {
		return Option.some(endpoint.tabId);
	}
	return Option.none();
};

const inTab = (endpoint: Endpoint, tabId: number): boolean =>
	Option.contains(tabIdOf(endpoint), tabId);

const disconnected = (): Disconnected => new Disconnected({});

export const makeFakePortHub: Effect.Effect<FakePortHub, never, Scope.Scope> =
	Effect.gen(function* () {
		const registry: Registry = {
			listeners: [],
			ports: [],
			invalidated: [],
		};
		const state = yield* Ref.make(registry);

		const forgetEnd = (end: FakeEnd): void => {
			registry.ports = registry.ports.filter(
				(port) => port.client !== end && port.server !== end,
			);
		};

		const dropPorts = (
			predicate: (port: OpenLink) => boolean,
			errorFor: (port: OpenLink, end: 'from' | 'to') => PortClosedError,
		): Effect.Effect<void> =>
			Ref.get(state).pipe(
				Effect.map((current) => {
					const matching = current.ports.filter(predicate);
					current.ports = current.ports.filter((port) => !predicate(port));
					for (const port of matching) {
						failEnd(port.client, errorFor(port, 'from'));
						failEnd(port.server, errorFor(port, 'to'));
					}
				}),
			);

		const linkRaw = (
			context: Endpoint,
			target: ConnectTarget,
			name: string,
		): RawPort => {
			const client = makeEnd();
			const server = makeEnd();
			client.peer = server;
			server.peer = client;
			const listener = registry.listeners.find(
				(candidate) =>
					candidate.name === name && Equal.equals(candidate.context, target),
			);
			const clientRaw = toRaw(client, () => {
				forgetEnd(client);
			});
			if (listener === undefined) {
				failEnd(client, new Disconnected({ detail: 'no receiver' }));
				return clientRaw;
			}
			registry.ports.push({
				name,
				from: context,
				to: target,
				client,
				server,
			});
			listener.onPort(
				toRaw(server, () => {
					forgetEnd(server);
				}),
				senderFor(context),
			);
			return clientRaw;
		};

		const layerFor = (context: Endpoint): Layer.Layer<PortConnector> =>
			Layer.succeed(PortConnector, {
				connect: (target, name) =>
					openPort(
						name,
						Ref.get(state).pipe(
							Effect.flatMap((current) => {
								if (
									current.invalidated.some((frame) =>
										Equal.equals(frame, context),
									)
								) {
									return Effect.fail(new ExtensionContextInvalidated());
								}
								return Effect.sync(() => linkRaw(context, target, name));
							}),
						),
					),
				listen: (name) =>
					acceptPorts(
						name,
						(onPort) => {
							const listener: Listener = { context, name, onPort };
							registry.listeners.push(listener);
							return () => {
								registry.listeners = registry.listeners.filter(
									(candidate) => candidate !== listener,
								);
							};
						},
						identity,
					),
			});

		yield* Effect.addFinalizer(() =>
			dropPorts(
				() => true,
				() => disconnected(),
			).pipe(
				Effect.andThen(
					Ref.get(state).pipe(
						Effect.map((current) => {
							current.listeners = [];
						}),
					),
				),
			),
		);

		return {
			layerFor,
			closePopup: dropPorts(
				(port) => isPopup(port.from) || isPopup(port.to),
				() => disconnected(),
			),
			stopServiceWorker: dropPorts(
				(port) => isBackground(port.from) || isBackground(port.to),
				() => disconnected(),
			),
			enterBackForwardCache: (frame) =>
				dropPorts(
					(port) =>
						Equal.equals(port.from, frame) || Equal.equals(port.to, frame),
					() => disconnected(),
				),
			navigateTab: (tabId) =>
				dropPorts(
					(port) => inTab(port.from, tabId) || inTab(port.to, tabId),
					() => disconnected(),
				).pipe(
					Effect.andThen(
						Ref.get(state).pipe(
							Effect.map((current) => {
								current.listeners = current.listeners.filter(
									(listener) => !inTab(listener.context, tabId),
								);
							}),
						),
					),
				),
			invalidateExtensionContext: (frame) =>
				Ref.get(state).pipe(
					Effect.andThen(
						Effect.sync(() => {
							registry.invalidated.push(frame);
						}),
					),
					Effect.andThen(
						dropPorts(
							(port) =>
								Equal.equals(port.from, frame) || Equal.equals(port.to, frame),
							(port, end) => {
								const endpoint = end === 'from' ? port.from : port.to;
								return Equal.equals(endpoint, frame)
									? new ExtensionContextInvalidated()
									: disconnected();
							},
						),
					),
				),
			openPorts: Ref.get(state).pipe(
				Effect.map((current) =>
					current.ports.map((port) => ({
						name: port.name,
						from: port.from,
						to: port.to,
					})),
				),
			),
		};
	});
