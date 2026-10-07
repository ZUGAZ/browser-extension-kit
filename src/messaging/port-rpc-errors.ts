import { Duration, Effect, Schema, Stream } from 'effect';
import { RpcClientError } from '@effect/rpc/RpcClientError';

import {
	Disconnected,
	ExtensionContextInvalidated,
	Timeout,
	type PortClosedError,
} from './connection-errors';

export const PortRpcError = Schema.Union(
	Disconnected,
	Timeout,
	ExtensionContextInvalidated,
);
export type PortRpcError = Schema.Schema.Type<typeof PortRpcError>;

const isPortClosedError = Schema.is(
	Schema.Union(Disconnected, ExtensionContextInvalidated),
);

export const toPortRpcError = (error: RpcClientError): PortRpcError =>
	isPortClosedError(error.cause)
		? error.cause
		: new Disconnected({ detail: error.message });

export const defaultRequestTimeout: Duration.Duration = Duration.seconds(10);

export const withPortErrors =
	(options?: { readonly timeout?: Duration.DurationInput }) =>
	<A, E, R>(self: Effect.Effect<A, E, R>) => {
		const duration = options?.timeout ?? defaultRequestTimeout;
		return self.pipe(
			Effect.timeoutFail({
				duration,
				onTimeout: () =>
					new Timeout({ afterMillis: Duration.toMillis(duration) }),
			}),
			Effect.catchAll((error): Effect.Effect<never, E | PortRpcError> => {
				if (Schema.is(RpcClientError)(error)) {
					return Effect.fail(toPortRpcError(error));
				}
				return Effect.fail(error);
			}),
		);
	};

export const withPortErrorsStream = <A, E, R>(self: Stream.Stream<A, E, R>) =>
	Stream.catchAll(self, (error): Stream.Stream<never, E | PortRpcError> => {
		if (Schema.is(RpcClientError)(error)) {
			return Stream.fail(toPortRpcError(error));
		}
		return Stream.fail(error);
	});

export const rpcClientError = (error: PortClosedError): RpcClientError =>
	new RpcClientError({
		reason: 'Protocol',
		message: error._tag,
		cause: error,
	});
