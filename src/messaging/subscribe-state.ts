import { Data, Effect, Mailbox, Option, Ref, Schema, Stream } from 'effect';
import { RpcClientError } from '@effect/rpc/RpcClientError';

import { ClientConnection } from './client-connection';
import {
	Disconnected,
	ExtensionContextInvalidated,
	Timeout,
	type PortClosedError,
} from './connection-errors';
import {
	PortRpcError,
	toPortRpcError,
	withPortErrorsStream,
} from './port-rpc-errors';

/**
 * Client-local status of a watch subscription.
 *
 * `Reconnecting` carries the last value. Before the first value the status
 * stays `Connecting`. `Terminated` covers an invalidated extension context
 * and a terminal disconnect.
 */
/* eslint-disable @typescript-eslint/no-empty-object-type -- variants carry no fields */
export type StateStatus<A> = Data.TaggedEnum<{
	Connecting: {};
	Live: { readonly value: A };
	Reconnecting: { readonly value: A };
	Terminated: {
		readonly error: PortClosedError;
		readonly value: Option.Option<A>;
	};
}>;
/* eslint-enable @typescript-eslint/no-empty-object-type */

interface StateStatusDefinition extends Data.TaggedEnum.WithGenerics<1> {
	readonly taggedEnum: StateStatus<this['A']>;
}

export const StateStatus = Data.taggedEnum<StateStatusDefinition>();

const isRpcClientError = Schema.is(RpcClientError);
const isPortRpcError = Schema.is(PortRpcError);
const isExtensionContextInvalidated = Schema.is(ExtensionContextInvalidated);
const isDisconnected = Schema.is(Disconnected);
const isTimeout = Schema.is(Timeout);

const identity = (value: boolean): boolean => value;

const transportCarrier = <E>(
	error: E,
): error is Extract<E, RpcClientError | PortRpcError> =>
	isRpcClientError(error) || isPortRpcError(error);

const withoutTransportErrors = <A, E, R, R2>(
	effect: Effect.Effect<A, E, R>,
	onPort: (error: PortRpcError) => Effect.Effect<A, never, R2>,
) =>
	effect.pipe(
		Effect.catchIf(transportCarrier, (error) => {
			if (isPortRpcError(error)) {
				return onPort(error);
			}
			if (isRpcClientError(error)) {
				return onPort(toPortRpcError(error));
			}
			return Effect.die('unrecognized transport error');
		}),
	);

/**
 * Turn a watch RPC stream into `StateStatus` values.
 *
 * Use it only for streams whose first element is the current state
 * (`watchState`). Resubscribing replaces missed changes with the fresh
 * current value.
 *
 * Pass `connection` from `makeBackgroundClient`. The output is latest-wins:
 * a slow reader skips intermediate statuses. Bind it in a viewmodel with
 * `Stream.runForEach`.
 */
export function subscribeState<A, E extends { readonly _tag: string }, R>(
	subscription: Stream.Stream<A, E, R>,
	connection: Stream.Stream<ClientConnection>,
): Stream.Stream<StateStatus<A>, Exclude<E, RpcClientError | PortRpcError>, R>;
export function subscribeState<A, E extends { readonly _tag: string }, R>(
	subscription: Stream.Stream<A, E, R>,
	connection: Stream.Stream<ClientConnection>,
) {
	const publish = <B>(
		output: Mailbox.Mailbox<StateStatus<A>, B>,
		status: StateStatus<A>,
	) => output.offer(status).pipe(Effect.orDie);

	return Stream.unwrapScoped(
		Effect.gen(function* () {
			const output = yield* Mailbox.make<StateStatus<A>, E | PortRpcError>({
				capacity: 1,
				strategy: 'sliding',
			});
			const last = yield* Ref.make(Option.none<A>());
			yield* publish(output, StateStatus.Connecting());

			const terminate = (error: PortClosedError) =>
				Ref.get(last).pipe(
					Effect.flatMap((value) =>
						publish(output, StateStatus.Terminated({ error, value })),
					),
					Effect.zipRight(Effect.logWarning('terminated')),
					Effect.as(false),
				);

			const awaitReconnect = Ref.get(last).pipe(
				Effect.flatMap(
					Option.match({
						onNone: () => Effect.void,
						onSome: (value) =>
							publish(output, StateStatus.Reconnecting({ value })),
					}),
				),
				Effect.zipRight(Effect.logDebug('resubscribing')),
				// Publish before the next request. Otherwise a sliding mailbox
				// replaces Reconnecting when the protocol reconnects immediately.
				Effect.zipRight(Effect.yieldNow()),
				Effect.as(true),
			);

			const afterDisconnect = Stream.runHead(connection).pipe(
				Effect.flatMap(
					Option.match({
						onNone: () => awaitReconnect,
						onSome: ClientConnection.$match({
							Terminated: ({ error }) => terminate(error),
							Connecting: () => awaitReconnect,
							Connected: () => awaitReconnect,
						}),
					}),
				),
			);

			const onTransportError = (error: PortRpcError) => {
				if (isExtensionContextInvalidated(error)) {
					return terminate(error);
				}
				if (isDisconnected(error) || isTimeout(error)) {
					return afterDisconnect;
				}
				return afterDisconnect;
			};

			const round = withoutTransportErrors(
				withPortErrorsStream(subscription).pipe(
					Stream.runForEach((value) =>
						Ref.set(last, Option.some(value)).pipe(
							Effect.zipRight(publish(output, StateStatus.Live({ value }))),
						),
					),
					Effect.as(false),
				),
				onTransportError,
			);

			yield* round.pipe(
				Effect.repeat({ while: identity }),
				Effect.asVoid,
				Mailbox.into(output),
				Effect.forkScoped,
			);
			return Mailbox.toStream(output);
		}).pipe(Effect.withLogSpan('subscribeState')),
	);
}
