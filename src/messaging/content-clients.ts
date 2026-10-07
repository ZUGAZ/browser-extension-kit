import {
	Data,
	Effect,
	ExecutionStrategy,
	Exit,
	HashMap,
	Option,
	Ref,
	Scope,
	Stream,
	SynchronizedRef,
} from 'effect';
import type * as rpc from '@effect/rpc/Rpc';
import type { RpcClient } from '@effect/rpc/RpcClient';
import { RpcClientError } from '@effect/rpc/RpcClientError';
import type * as rpcGroup from '@effect/rpc/RpcGroup';

import { Disconnected, ReceiverUnavailable } from './connection-errors';
import type { Content } from './endpoint';
import { makeContentClientSession } from './port-rpc-client';
import { PortConnector } from './port-connector';
import { Presence } from './presence';

/* eslint-disable @typescript-eslint/no-empty-object-type -- variants carry no fields */
type ReleaseReason = Data.TaggedEnum<{
	PresenceLost: {};
	PortClosed: {};
}>;
/* eslint-enable @typescript-eslint/no-empty-object-type */

const ReleaseReason = Data.taggedEnum<ReleaseReason>();

interface ClientSession {
	readonly release: (error: Disconnected) => Effect.Effect<void>;
	readonly awaitTerminated: Effect.Effect<void>;
	readonly isTerminated: Effect.Effect<boolean>;
}

interface Entry<Rpcs extends rpc.Any> {
	readonly generation: number;
	readonly client: RpcClient<Rpcs, RpcClientError>;
	readonly session: ClientSession;
	readonly scope: Scope.CloseableScope;
}

type ClientCache<Rpcs extends rpc.Any> = HashMap.HashMap<Content, Entry<Rpcs>>;

const kept = <Rpcs extends rpc.Any>(
	client: RpcClient<Rpcs, RpcClientError>,
	entries: ClientCache<Rpcs>,
): readonly [RpcClient<Rpcs, RpcClientError>, ClientCache<Rpcs>] => [
	client,
	entries,
];

export interface ContentClients<Rpcs extends rpc.Any> {
	readonly clientFor: (
		endpoint: Content,
	) => Effect.Effect<RpcClient<Rpcs, RpcClientError>, ReceiverUnavailable>;
}

export const makeContentClients = <Rpcs extends rpc.Any>(
	group: rpcGroup.RpcGroup<Rpcs>,
	options: { readonly name: string },
): Effect.Effect<
	ContentClients<Rpcs>,
	never,
	Presence | PortConnector | Scope.Scope | rpc.MiddlewareClient<Rpcs>
> =>
	Effect.gen(function* () {
		const presence = yield* Presence;
		const parent = yield* Effect.scope;
		const context = yield* Effect.context<
			PortConnector | rpc.MiddlewareClient<Rpcs>
		>();
		const cache = yield* SynchronizedRef.make(
			HashMap.empty<Content, Entry<Rpcs>>(),
		);
		const nextGeneration = yield* Ref.make(0);

		const isPresent = (endpoint: Content) =>
			presence.get.pipe(
				Effect.map((current) => HashMap.has(current, endpoint)),
			);

		const watch = (endpoint: Content, entry: Entry<Rpcs>) =>
			Effect.gen(function* () {
				const presenceGone = presence.changes.pipe(
					Stream.filter((current) => !HashMap.has(current, endpoint)),
					Stream.runHead,
					Effect.as(ReleaseReason.PresenceLost()),
				);
				const reason = yield* Effect.raceFirst(
					presenceGone,
					entry.session.awaitTerminated.pipe(
						Effect.as(ReleaseReason.PortClosed()),
					),
				);
				yield* entry.session.release(
					new Disconnected({ detail: 'receiver left' }),
				);
				yield* entry.session.awaitTerminated;
				yield* Scope.close(entry.scope, Exit.void);
				yield* SynchronizedRef.update(cache, (current) =>
					Option.match(HashMap.get(current, endpoint), {
						onNone: () => current,
						onSome: (cached) =>
							cached.generation === entry.generation
								? HashMap.remove(current, endpoint)
								: current,
					}),
				);
				yield* Effect.logDebug('client released', reason._tag);
			}).pipe(Effect.withLogSpan('ContentClients'));

		const open = (endpoint: Content) =>
			Effect.gen(function* () {
				const scope = yield* Scope.fork(parent, ExecutionStrategy.sequential);
				const session = yield* makeContentClientSession(group, {
					name: options.name,
					target: endpoint,
				}).pipe(Scope.extend(scope), Effect.provide(context));
				const generation = yield* Ref.getAndUpdate(
					nextGeneration,
					(current) => current + 1,
				);
				const entry: Entry<Rpcs> = {
					generation,
					client: session.client,
					session,
					scope,
				};
				yield* watch(endpoint, entry).pipe(
					Effect.forkIn(parent),
					Effect.asVoid,
				);
				yield* Effect.logDebug('client opened');
				return entry;
			});

		const clientFor = (endpoint: Content) =>
			SynchronizedRef.modifyEffect(cache, (entries) =>
				Effect.gen(function* () {
					if (!(yield* isPresent(endpoint))) {
						return yield* Effect.fail(new ReceiverUnavailable({ endpoint }));
					}
					const cached = HashMap.get(entries, endpoint);
					if (
						Option.isSome(cached) &&
						!(yield* cached.value.session.isTerminated)
					) {
						return kept(cached.value.client, entries);
					}
					const entry = yield* open(endpoint);
					return kept(entry.client, HashMap.set(entries, endpoint, entry));
				}),
			).pipe(Effect.withLogSpan('ContentClients'));

		return { clientFor };
	});
