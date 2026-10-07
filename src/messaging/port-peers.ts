import {
	Context,
	Effect,
	HashMap,
	Option,
	Stream,
	SubscriptionRef,
} from 'effect';

import type { Endpoint } from './endpoint';

export class PortPeers extends Context.Tag('browser-extension-kit/PortPeers')<
	PortPeers,
	{
		readonly get: (clientId: number) => Effect.Effect<Option.Option<Endpoint>>;
		readonly changes: Stream.Stream<HashMap.HashMap<number, Endpoint>>;
	}
>() {}

export const makePortPeers: Effect.Effect<{
	readonly service: PortPeers['Type'];
	readonly register: (clientId: number, peer: Endpoint) => Effect.Effect<void>;
	readonly unregister: (clientId: number) => Effect.Effect<void>;
}> = Effect.gen(function* () {
	const peers = yield* SubscriptionRef.make(HashMap.empty<number, Endpoint>());
	const service: PortPeers['Type'] = {
		get: (clientId) =>
			SubscriptionRef.get(peers).pipe(
				Effect.map((current) => HashMap.get(current, clientId)),
			),
		changes: peers.changes,
	};
	return {
		service,
		register: (clientId, peer) =>
			SubscriptionRef.update(peers, (current) =>
				HashMap.set(current, clientId, peer),
			),
		unregister: (clientId) =>
			SubscriptionRef.update(peers, (current) =>
				HashMap.remove(current, clientId),
			),
	};
});
