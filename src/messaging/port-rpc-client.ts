import { Duration, Effect, Option, Schedule } from 'effect';
import type * as rpc from '@effect/rpc/Rpc';
import * as rpcClient from '@effect/rpc/RpcClient';
import * as rpcGroup from '@effect/rpc/RpcGroup';

import type { Disconnected } from './connection-errors';
import { Background, type Content } from './endpoint';
import { PageLifecycleNone } from './page-lifecycle';
import { makePortClientProtocol } from './port-rpc-client-protocol';
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
	rpcClient.make(group, { spanPrefix: 'PortRpcClient' }).pipe(
		Effect.provideServiceEffect(
			rpcClient.Protocol,
			makePortClientProtocol({
				target: options.target,
				name: options.name,
				reconnect: Option.none(),
			}).pipe(Effect.provide(PageLifecycleNone)),
		),
	);
