import { Context, Effect, type Scope, type Stream } from 'effect';

import type {
	ExtensionContextInvalidated,
	PortClosedError,
} from './connection-errors';
import type { ConnectTarget, Endpoint } from './endpoint';

export interface PortHandle {
	readonly name: string;
	/** Single consumer. Buffered from creation. Completes when the port closes. */
	readonly messages: Stream.Stream<unknown>;
	readonly send: (message: unknown) => Effect.Effect<void, PortClosedError>;
	/** Succeeds on a local close. Fails when the other side closes the port. */
	readonly closed: Effect.Effect<void, PortClosedError>;
	readonly close: Effect.Effect<void>;
}

export interface AcceptedPort extends PortHandle {
	readonly peer: Endpoint;
}

export class PortConnector extends Context.Tag(
	'browser-extension-kit/PortConnector',
)<
	PortConnector,
	{
		readonly connect: (
			target: ConnectTarget,
			name: string,
		) => Effect.Effect<PortHandle, ExtensionContextInvalidated, Scope.Scope>;
		/**
		 * Attaches the listener synchronously when this effect runs.
		 * Only one context per extension should listen on a given runtime port name.
		 * The stream ends and accepted ports close when the scope closes.
		 */
		readonly listen: (
			name: string,
		) => Effect.Effect<Stream.Stream<AcceptedPort>, never, Scope.Scope>;
	}
>() {}
