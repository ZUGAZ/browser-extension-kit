import {
	Context,
	Effect,
	HashMap,
	Layer,
	Option,
	Schema,
	Stream,
	SubscriptionRef,
} from 'effect';

import type { Endpoint } from './endpoint';
import { PortPeers } from './port-peers';

/** Number of open ports from this endpoint; ≥ 1 while present. */
export class PortInfo extends Schema.Class<PortInfo>('PortInfo')({
	connections: Schema.Int.pipe(Schema.positive()),
}) {}

export const presenceFromPeers = (
	peers: HashMap.HashMap<number, Endpoint>,
): HashMap.HashMap<Endpoint, PortInfo> => {
	const counts = HashMap.reduce(
		peers,
		HashMap.empty<Endpoint, number>(),
		(accumulator, endpoint) =>
			HashMap.modifyAt(accumulator, endpoint, (current) =>
				Option.some(
					Option.match(current, {
						onNone: () => 1,
						onSome: (count) => count + 1,
					}),
				),
			),
	);
	return HashMap.map(counts, (connections) => new PortInfo({ connections }));
};

export class Presence extends Context.Tag('browser-extension-kit/Presence')<
	Presence,
	{
		readonly get: Effect.Effect<HashMap.HashMap<Endpoint, PortInfo>>;
		readonly changes: Stream.Stream<HashMap.HashMap<Endpoint, PortInfo>>;
	}
>() {}

/**
 * Provide on top of `layerPortServer`:
 * `layerPresence.pipe(Layer.provideMerge(layerPortServer(group, { name })))`.
 * Safe under `ManagedRuntime.runSync`. The registry starts empty after a
 * service-worker restart. Only accepted ports count.
 */
export const layerPresence: Layer.Layer<Presence, never, PortPeers> =
	Layer.scoped(
		Presence,
		Effect.gen(function* () {
			const peers = yield* PortPeers;
			const ref = yield* SubscriptionRef.make(
				HashMap.empty<Endpoint, PortInfo>(),
			);
			yield* peers.changes.pipe(
				Stream.map(presenceFromPeers),
				Stream.changes,
				Stream.runForEach((next) =>
					SubscriptionRef.set(ref, next).pipe(
						Effect.zipRight(
							Effect.logDebug('presence changed', HashMap.size(next)),
						),
					),
				),
				Effect.withLogSpan('Presence'),
				Effect.interruptible,
				Effect.forkScoped,
			);
			return {
				get: SubscriptionRef.get(ref),
				changes: ref.changes.pipe(Stream.changes),
			};
		}),
	);

export const watchPresence: Stream.Stream<
	HashMap.HashMap<Endpoint, PortInfo>,
	never,
	Presence
> = Stream.unwrap(Effect.map(Presence, (presence) => presence.changes));
