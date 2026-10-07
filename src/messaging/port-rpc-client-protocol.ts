import {
	Cause,
	Clock,
	Data,
	Deferred,
	Duration,
	Effect,
	Exit,
	Fiber,
	Option,
	Schedule,
	Schema,
	Stream,
	SubscriptionRef,
	type Scope,
} from 'effect';
import type { FromClientEncoded } from '@effect/rpc/RpcMessage';
import * as rpcClient from '@effect/rpc/RpcClient';

import {
	Disconnected,
	ExtensionContextInvalidated,
	type PortClosedError,
} from './connection-errors';
import type { ConnectTarget } from './endpoint';
import { PageLifecycle } from './page-lifecycle';
import { PortConnector, type PortHandle } from './port-connector';
import { rpcClientError } from './port-rpc-errors';
import { decodeFromServerFrame, isRequestFrame } from './rpc-frames';

type ConnectionState =
	| { readonly _tag: 'Connecting' }
	| { readonly _tag: 'Connected'; readonly handle: PortHandle }
	| { readonly _tag: 'Terminated'; readonly error: PortClosedError };

const { $is, $match, Connected, Connecting, Terminated } =
	Data.taggedEnum<ConnectionState>();

const isDisconnected = Schema.is(Disconnected);
const isExtensionContextInvalidated = Schema.is(ExtensionContextInvalidated);

const droppedFrame = Effect.logWarning('dropped invalid frame');

type ReconnectStep = 'delayed' | 'exhausted' | 'restored';

const delayed: ReconnectStep = 'delayed';
const exhausted: ReconnectStep = 'exhausted';
const restored: ReconnectStep = 'restored';

const pollRelease = (releaseSignal: Deferred.Deferred<Disconnected>) =>
	Deferred.poll(releaseSignal).pipe(
		Effect.flatMap(
			Option.match({
				onNone: () => Effect.succeed(Option.none<Disconnected>()),
				onSome: (completed) => Effect.map(completed, Option.some),
			}),
		),
	);

export const makePortClientProtocolWithControl = (options: {
	readonly target: ConnectTarget;
	readonly name: string;
	readonly reconnect: Option.Option<{
		readonly schedule: Schedule.Schedule<unknown, Disconnected>;
		readonly stableAfter: Duration.DurationInput;
	}>;
}): Effect.Effect<
	{
		readonly protocol: rpcClient.Protocol['Type'];
		readonly release: (error: Disconnected) => Effect.Effect<void>;
		readonly awaitTerminated: Effect.Effect<void>;
		readonly isTerminated: Effect.Effect<boolean>;
	},
	never,
	PortConnector | PageLifecycle | Scope.Scope
> =>
	Effect.gen(function* () {
		const releaseSignal = yield* Deferred.make<Disconnected>();
		const state = yield* SubscriptionRef.make<ConnectionState>(Connecting());
		const protocol = yield* rpcClient.Protocol.make((writeResponse) =>
			Effect.gen(function* () {
				const connector = yield* PortConnector;
				const lifecycle = yield* PageLifecycle;
				const driver = yield* Option.match(options.reconnect, {
					onNone: () =>
						Effect.succeed(
							Option.none<Schedule.ScheduleDriver<unknown, Disconnected>>(),
						),
					onSome: ({ schedule }) =>
						Schedule.driver(schedule).pipe(Effect.map(Option.some)),
				});

				const failInFlight = (error: PortClosedError) =>
					writeResponse({
						_tag: 'ClientProtocolError',
						error: rpcClientError(error),
					});

				const terminate = (error: ExtensionContextInvalidated) =>
					Effect.gen(function* () {
						const current = yield* SubscriptionRef.get(state);
						if ($is('Terminated')(current)) {
							return;
						}
						yield* failInFlight(error);
						yield* SubscriptionRef.set(state, Terminated({ error }));
						yield* Effect.logWarning('extension context invalidated');
					});

				const sendOnHandle = (handle: PortHandle, request: FromClientEncoded) =>
					handle.send(request).pipe(
						Effect.mapError(rpcClientError),
						Effect.tapError((error) =>
							isExtensionContextInvalidated(error.cause)
								? terminate(error.cause)
								: Effect.void,
						),
					);

				const send = (request: FromClientEncoded) => {
					if (!isRequestFrame(request)) {
						return SubscriptionRef.get(state).pipe(
							Effect.flatMap((current) =>
								$is('Connected')(current)
									? sendOnHandle(current.handle, request)
									: Effect.void,
							),
						);
					}
					return state.changes.pipe(
						Stream.filter((current) => !$is('Connecting')(current)),
						Stream.take(1),
						Stream.runHead,
						Effect.flatMap((ready) =>
							Option.match(ready, {
								onNone: () => Effect.die('connection state ended'),
								onSome: (current) =>
									$match(current, {
										Connecting: () => Effect.die('still connecting'),
										Connected: ({ handle }) => sendOnHandle(handle, request),
										Terminated: ({ error }) =>
											Effect.fail(rpcClientError(error)),
									}),
							}),
						),
					);
				};

				// A port that is already closed when connect returns must not be
				// published, and must not fail calls that are still waiting to
				// send. A full stream mailbox suspends the reader, so close is
				// observed beside it and the reader is interrupted afterwards.
				const session = Effect.scoped(
					Effect.gen(function* () {
						const handle = yield* connector.connect(
							options.target,
							options.name,
						);
						const closeFiber = yield* handle.closed.pipe(
							Effect.exit,
							Effect.forkScoped,
						);
						yield* Effect.yieldNow();
						const immediate = yield* Fiber.poll(closeFiber);
						if (Option.isSome(immediate) && Exit.isSuccess(immediate.value)) {
							const alreadyClosed = immediate.value.value;
							if (Exit.isFailure(alreadyClosed)) {
								return yield* Effect.failCause(alreadyClosed.cause);
							}
							return;
						}
						yield* SubscriptionRef.set(state, Connected({ handle }));
						yield* Effect.logDebug('connected');
						const reader = yield* Stream.runForEach(handle.messages, (raw) =>
							Option.match(decodeFromServerFrame(raw), {
								onNone: () => droppedFrame,
								onSome: (frame) => writeResponse(frame),
							}),
						).pipe(Effect.forkScoped);
						const closed = yield* Fiber.join(closeFiber);
						// Fail in-flight calls before interrupting the reader.
						// Interrupting it first ends a stream mailbox with that
						// interrupt, and a later protocol error cannot replace it.
						if (Exit.isFailure(closed)) {
							yield* SubscriptionRef.set(state, Connecting());
							const failure = Cause.failureOption(closed.cause);
							if (Option.isSome(failure)) {
								yield* failInFlight(failure.value);
							}
						}
						yield* Fiber.interrupt(reader);
						if (Exit.isFailure(closed)) {
							return yield* Effect.failCause(closed.cause);
						}
					}),
				);

				// A local release takes the same path as a remote close. The
				// session scope closes the handle, then this loop fails in-flight
				// calls and marks the connection terminated.
				const sessionOrRelease = Effect.raceFirst(
					session,
					Deferred.await(releaseSignal).pipe(Effect.flip),
				);

				const stopForRelease = (error: Disconnected) =>
					failInFlight(error).pipe(
						Effect.zipRight(SubscriptionRef.set(state, Terminated({ error }))),
					);

				const waitUntilForeground = lifecycle.changes.pipe(
					Stream.filter((cached) => !cached),
					Stream.take(1),
					Stream.runDrain,
				);

				const loop = (): Effect.Effect<void> =>
					Effect.gen(function* () {
						const alreadyReleased = yield* pollRelease(releaseSignal);
						if (Option.isSome(alreadyReleased)) {
							yield* stopForRelease(alreadyReleased.value);
							return;
						}
						yield* waitUntilForeground;
						const startedAt = yield* Clock.currentTimeMillis;
						const closed = yield* Effect.flip(sessionOrRelease).pipe(
							Effect.option,
						);
						const released = yield* pollRelease(releaseSignal);
						if (Option.isSome(released)) {
							yield* stopForRelease(released.value);
							return;
						}
						yield* SubscriptionRef.set(state, Connecting());
						if (Option.isNone(closed)) {
							return;
						}
						const error = closed.value;
						if (isExtensionContextInvalidated(error)) {
							yield* terminate(error);
							return;
						}
						if (!isDisconnected(error)) {
							return;
						}
						if (Option.isNone(options.reconnect) || Option.isNone(driver)) {
							yield* failInFlight(error);
							yield* SubscriptionRef.set(state, Terminated({ error }));
							return;
						}
						const endedAt = yield* Clock.currentTimeMillis;
						const stableAfter = Duration.toMillis(
							options.reconnect.value.stableAfter,
						);
						if (endedAt - startedAt >= stableAfter) {
							yield* driver.value.reset;
							yield* Effect.logDebug('reconnecting immediately');
							yield* loop();
							return;
						}
						yield* Effect.logDebug('waiting to reconnect');
						const step = yield* Effect.raceFirst(
							driver.value.next(error).pipe(
								Effect.option,
								Effect.map((slept) =>
									Option.match(slept, {
										onNone: () => exhausted,
										onSome: () => delayed,
									}),
								),
							),
							lifecycle.changes.pipe(
								Stream.drop(1),
								Stream.filter((cached) => !cached),
								Stream.runHead,
								Effect.map((head) =>
									Option.match(head, {
										onNone: () => exhausted,
										onSome: () => restored,
									}),
								),
							),
						);
						if (step === exhausted) {
							yield* SubscriptionRef.set(state, Terminated({ error }));
							yield* Effect.logWarning('reconnect schedule exhausted');
							return;
						}
						if (step === restored) {
							yield* driver.value.reset;
							yield* Effect.logDebug('reconnecting after restore');
						}
						yield* loop();
					});

				yield* loop().pipe(
					Effect.interruptible,
					Effect.withLogSpan('PortRpcClient'),
					Effect.forkScoped,
				);

				return {
					send,
					supportsAck: true,
					supportsTransferables: false,
				};
			}),
		);

		const release = (error: Disconnected) =>
			Deferred.succeed(releaseSignal, error).pipe(Effect.asVoid);
		const awaitTerminated = state.changes.pipe(
			Stream.filter($is('Terminated')),
			Stream.runHead,
			Effect.asVoid,
		);
		const isTerminated = SubscriptionRef.get(state).pipe(
			Effect.map($is('Terminated')),
		);
		return { protocol, release, awaitTerminated, isTerminated };
	});

export const makePortClientProtocol = (options: {
	readonly target: ConnectTarget;
	readonly name: string;
	readonly reconnect: Option.Option<{
		readonly schedule: Schedule.Schedule<unknown, Disconnected>;
		readonly stableAfter: Duration.DurationInput;
	}>;
}): Effect.Effect<
	rpcClient.Protocol['Type'],
	never,
	PortConnector | PageLifecycle | Scope.Scope
> =>
	makePortClientProtocolWithControl(options).pipe(
		Effect.map((control) => control.protocol),
	);
