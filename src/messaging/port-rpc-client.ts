import { Duration, Effect, Option, Schedule, type Scope } from 'effect';
import type * as rpc from '@effect/rpc/Rpc';
import * as rpcClient from '@effect/rpc/RpcClient';
import { RpcClientError } from '@effect/rpc/RpcClientError';
import * as rpcGroup from '@effect/rpc/RpcGroup';

import type { Disconnected } from './connection-errors';
import { Background, type Content } from './endpoint';
import { PageLifecycleNone } from './page-lifecycle';
import {
	makePortClientProtocol,
	makePortClientProtocolWithControl,
} from './port-rpc-client-protocol';
import type { PortConnector } from './port-connector';
import {
	defaultReconnectSchedule,
	defaultStableAfter,
} from './reconnect-schedule';

/**
 * Wrap each call in `withPortErrors` and each stream in `withPortErrorsStream`.
 * A dropped port ends in-flight effects and streams with `Disconnected`.
 * This client does not resubscribe.
 *
 * Reconnects while the page is in the foreground. Pauses in the back/forward
 * cache and reconnects when the page is restored. Stops for good on
 * `ExtensionContextInvalidated`.
 */
export const makeBackgroundClient = <Rpcs extends rpc.Any>(
	group: rpcGroup.RpcGroup<Rpcs>,
	options: {
		readonly name: string;
		readonly reconnectSchedule?: Schedule.Schedule<unknown, Disconnected>;
		readonly stableAfter?: Duration.DurationInput;
	},
) =>
	rpcClient.make(group, { spanPrefix: 'PortRpcClient' }).pipe(
		Effect.provideServiceEffect(
			rpcClient.Protocol,
			makePortClientProtocol({
				target: new Background(),
				name: options.name,
				reconnect: Option.some({
					schedule: options.reconnectSchedule ?? defaultReconnectSchedule,
					stableAfter: options.stableAfter ?? defaultStableAfter,
				}),
			}),
		),
	);

export const makeContentClientSession = <Rpcs extends rpc.Any>(
	group: rpcGroup.RpcGroup<Rpcs>,
	options: {
		readonly name: string;
		readonly target: Content;
	},
): Effect.Effect<
	{
		readonly client: rpcClient.RpcClient<Rpcs, RpcClientError>;
		readonly release: (error: Disconnected) => Effect.Effect<void>;
		readonly awaitTerminated: Effect.Effect<void>;
		readonly isTerminated: Effect.Effect<boolean>;
	},
	never,
	PortConnector | Scope.Scope | rpc.MiddlewareClient<Rpcs>
> =>
	Effect.gen(function* () {
		const control = yield* makePortClientProtocolWithControl({
			target: options.target,
			name: options.name,
			reconnect: Option.none(),
		}).pipe(Effect.provide(PageLifecycleNone));
		const client = yield* rpcClient
			.make(group, { spanPrefix: 'PortRpcClient' })
			.pipe(Effect.provideService(rpcClient.Protocol, control.protocol));
		return {
			client,
			release: control.release,
			awaitTerminated: control.awaitTerminated,
			isTerminated: control.isTerminated,
		};
	});

/**
 * Opens one port and never reconnects. When the port closes, later calls fail
 * with `Disconnected`. Wrap calls the same way as `makeBackgroundClient`.
 */
export const makeContentClient = <Rpcs extends rpc.Any>(
	group: rpcGroup.RpcGroup<Rpcs>,
	options: {
		readonly name: string;
		readonly target: Content;
	},
) =>
	makeContentClientSession(group, options).pipe(
		Effect.map((session) => session.client),
	);
