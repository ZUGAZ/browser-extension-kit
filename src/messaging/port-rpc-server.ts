import { Effect, Layer } from 'effect';
import type * as rpc from '@effect/rpc/Rpc';
import * as rpcGroup from '@effect/rpc/RpcGroup';
import * as rpcServer from '@effect/rpc/RpcServer';

import { PortCaller } from './caller';
import { PortPeers } from './port-peers';
import { makePortServerProtocol } from './port-rpc-server-protocol';
import type { PortConnector } from './port-connector';

/**
 * One server per context. Merge groups with `rpcGroup.merge` when a context
 * serves more than one. Add `.middleware(PortCaller)` on the group to read
 * `Caller`.
 *
 * Service workers must build this at module top level and attach the listener
 * synchronously:
 *
 * ```ts
 * const runtime = ManagedRuntime.make(layerPortServer(group, { name }).pipe(...))
 * runtime.runSync(Effect.void)
 * ```
 *
 * `runSync` throws if layer construction goes async, which would miss the
 * top-level listener deadline.
 */
export const layerPortServer = <Rpcs extends rpc.Any>(
	group: rpcGroup.RpcGroup<Rpcs>,
	options: {
		readonly name: string;
		readonly concurrency?: number | 'unbounded';
	},
): Layer.Layer<
	PortPeers,
	never,
	| PortConnector
	| rpc.ToHandler<Rpcs>
	| rpc.Context<Rpcs>
	| Exclude<rpc.Middleware<Rpcs>, PortCaller>
> =>
	Layer.scoped(
		PortPeers,
		Effect.gen(function* () {
			const { protocol, caller, peers } = yield* makePortServerProtocol(
				options.name,
			);
			const serverOptions =
				options.concurrency === undefined
					? {
							disableFatalDefects: true,
							spanPrefix: 'PortRpcServer',
						}
					: {
							disableFatalDefects: true,
							spanPrefix: 'PortRpcServer',
							concurrency: options.concurrency,
						};
			yield* rpcServer
				.make(group, serverOptions)
				.pipe(
					Effect.provideService(rpcServer.Protocol, protocol),
					Effect.provideService(PortCaller, caller),
					Effect.interruptible,
					Effect.forkScoped,
				);
			return peers;
		}),
	);
