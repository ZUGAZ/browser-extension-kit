import { Effect, Either, Layer, Option, Schema, type Scope } from 'effect';

import {
	Disconnected,
	ExtensionContextInvalidated,
	type PortClosedError,
} from './connection-errors';
import { isBackground, isContent, type ConnectTarget } from './endpoint';
import type { ExtensionIdentity, SenderInfo } from './endpoint-from-sender';
import { acceptPorts, openPort, type RawPort } from './port-handle';
import { PortConnector, type PortHandle } from './port-connector';

const manifestServiceWorker = Schema.Struct({
	background: Schema.Struct({
		service_worker: Schema.String,
	}),
});

const readExtensionIdentity: Effect.Effect<ExtensionIdentity> = Effect.sync(
	() => {
		const origin = new URL(chrome.runtime.getURL('')).origin;
		const serviceWorkerUrl = Schema.decodeUnknownOption(manifestServiceWorker)(
			chrome.runtime.getManifest(),
		).pipe(
			Option.map((manifest) =>
				chrome.runtime.getURL(manifest.background.service_worker),
			),
		);
		return {
			id: chrome.runtime.id,
			origin,
			serviceWorkerUrl,
		};
	},
);

const isContextValid = (): boolean =>
	Option.isSome(
		Schema.decodeUnknownOption(Schema.NonEmptyString)(chrome.runtime.id),
	);

const closeError = (): PortClosedError => {
	const detail = chrome.runtime.lastError?.message;
	if (!isContextValid()) {
		return new ExtensionContextInvalidated();
	}
	if (detail === undefined) {
		return new Disconnected({});
	}
	return new Disconnected({ detail });
};

const fromChromePort = (port: chrome.runtime.Port): RawPort => ({
	postMessage: (message) =>
		Either.try({
			try: () => {
				port.postMessage(message);
			},
			catch: () => closeError(),
		}),
	disconnect: () => {
		port.disconnect();
	},
	onMessage: (listener) => {
		port.onMessage.addListener((message: unknown) => {
			listener(message);
		});
	},
	onDisconnect: (listener) => {
		port.onDisconnect.addListener(() => {
			listener(closeError());
		});
	},
});

const openChromePort = (
	target: ConnectTarget,
	name: string,
): chrome.runtime.Port => {
	if (isBackground(target)) {
		return chrome.runtime.connect({ name });
	}
	if (isContent(target)) {
		return chrome.tabs.connect(target.tabId, {
			name,
			frameId: target.frameId,
		});
	}
	return unexpectedTarget(target);
};

const unexpectedTarget = (target: never): never => {
	throw new Error(`Unexpected connect target ${JSON.stringify(target)}`);
};

const connect = (
	target: ConnectTarget,
	name: string,
): Effect.Effect<PortHandle, ExtensionContextInvalidated, Scope.Scope> =>
	openPort(
		name,
		Effect.suspend(() => {
			if (!isContextValid()) {
				return Effect.fail(new ExtensionContextInvalidated());
			}
			return Effect.try({
				try: () => fromChromePort(openChromePort(target, name)),
				catch: (cause) => cause,
			}).pipe(
				Effect.catchAll((cause) =>
					isContextValid()
						? Effect.die(cause)
						: Effect.fail(new ExtensionContextInvalidated()),
				),
			);
		}),
	);

const asSender = (
	sender: chrome.runtime.MessageSender | undefined,
): SenderInfo => sender ?? {};

const listen = (name: string) =>
	readExtensionIdentity.pipe(
		Effect.flatMap((identity) =>
			acceptPorts(
				name,
				(onPort) => {
					const listener = (port: chrome.runtime.Port) => {
						if (port.name !== name) {
							return;
						}
						onPort(fromChromePort(port), asSender(port.sender));
					};
					chrome.runtime.onConnect.addListener(listener);
					return () => {
						chrome.runtime.onConnect.removeListener(listener);
					};
				},
				identity,
			),
		),
	);

export const PortConnectorLive: Layer.Layer<PortConnector> = Layer.succeed(
	PortConnector,
	{
		connect,
		listen,
	},
);
