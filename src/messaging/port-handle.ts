import {
	Deferred,
	Effect,
	Either,
	Exit,
	Mailbox,
	Option,
	Runtime,
	Stream,
} from 'effect';
import type { Scope } from 'effect';

import {
	Disconnected,
	type PortClosedError,
	type ExtensionContextInvalidated,
} from './connection-errors';
import {
	endpointFromSender,
	type ExtensionIdentity,
	type SenderInfo,
} from './endpoint-from-sender';
import type { AcceptedPort, PortHandle } from './port-connector';

export interface RawPort {
	readonly postMessage: (
		message: unknown,
	) => Either.Either<void, PortClosedError>;
	readonly disconnect: () => void;
	readonly onMessage: (listener: (message: unknown) => void) => void;
	readonly onDisconnect: (listener: (error: PortClosedError) => void) => void;
}

interface CloseRecord {
	error: PortClosedError | undefined;
}

export const makePortHandle = (
	name: string,
	raw: RawPort,
): Effect.Effect<PortHandle> =>
	Effect.gen(function* () {
		const mailbox = yield* Mailbox.make<unknown>();
		const closed = yield* Deferred.make<undefined, PortClosedError>();
		const record: CloseRecord = { error: undefined };

		raw.onMessage((message) => {
			mailbox.unsafeOffer(message);
		});
		raw.onDisconnect((error) => {
			record.error = error;
			Deferred.unsafeDone(closed, Effect.fail(error));
			mailbox.unsafeDone(Exit.void);
		});

		const close: Effect.Effect<void> = Effect.gen(function* () {
			if (record.error !== undefined || (yield* Deferred.isDone(closed))) {
				return;
			}
			const locallyClosed = new Disconnected({ detail: 'closed locally' });
			record.error = locallyClosed;
			raw.disconnect();
			yield* Deferred.succeed(closed, undefined);
			yield* mailbox.end;
		});

		const send = (message: unknown): Effect.Effect<void, PortClosedError> =>
			Effect.gen(function* () {
				const recorded = record.error;
				if (recorded !== undefined) {
					return yield* Effect.fail(recorded);
				}
				if (yield* Deferred.isDone(closed)) {
					return yield* closed;
				}
				return yield* raw.postMessage(message).pipe(
					Either.match({
						onLeft: (error) => Effect.fail(error),
						onRight: () => Effect.void,
					}),
				);
			});

		return {
			name,
			messages: Mailbox.toStream(mailbox),
			send,
			closed,
			close,
		};
	});

export const openPort = (
	name: string,
	openRaw: Effect.Effect<RawPort, ExtensionContextInvalidated>,
): Effect.Effect<PortHandle, ExtensionContextInvalidated, Scope.Scope> =>
	Effect.acquireRelease(
		openRaw.pipe(Effect.flatMap((raw) => makePortHandle(name, raw))),
		(handle) => handle.close,
	);

export type SubscribeIncoming = (
	onPort: (raw: RawPort, sender: SenderInfo) => void,
) => () => void;

const rejectSender = Effect.logWarning(
	'ignored port from an unrecognized sender',
).pipe(Effect.withLogSpan('PortConnector'));

export const acceptPorts = (
	name: string,
	subscribe: SubscribeIncoming,
	identity: ExtensionIdentity,
): Effect.Effect<Stream.Stream<AcceptedPort>, never, Scope.Scope> =>
	Effect.gen(function* () {
		const runtime = yield* Effect.runtime();
		const accepted = yield* Mailbox.make<AcceptedPort>();
		const openHandles: Array<PortHandle> = [];

		const unsubscribe = subscribe((raw, sender) => {
			Option.match(endpointFromSender(sender, identity), {
				onNone: () => {
					raw.disconnect();
					Runtime.runSync(runtime, rejectSender);
				},
				onSome: (peer) => {
					const handle = Runtime.runSync(runtime, makePortHandle(name, raw));
					openHandles.push(handle);
					accepted.unsafeOffer({ ...handle, peer });
				},
			});
		});

		yield* Effect.addFinalizer(() =>
			Effect.gen(function* () {
				unsubscribe();
				yield* Effect.forEach(openHandles, (handle) => handle.close, {
					discard: true,
				});
				yield* accepted.end;
			}),
		);

		return Mailbox.toStream(accepted);
	});
