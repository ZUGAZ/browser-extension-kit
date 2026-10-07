import { Effect, Mailbox, Option, Stream, type Scope } from 'effect';
import * as rpcServer from '@effect/rpc/RpcServer';

import type { PortCaller } from './caller';
import type { PortPeers } from './port-peers';
import { makePortPeers } from './port-peers';
import { decodeFromClientFrame } from './rpc-frames';
import { PortConnector, type AcceptedPort } from './port-connector';

const droppedFrame = Effect.logWarning('dropped invalid frame');

export const makePortServerProtocol = (
	name: string,
): Effect.Effect<
	{
		readonly protocol: rpcServer.Protocol['Type'];
		readonly caller: PortCaller['Type'];
		readonly peers: PortPeers['Type'];
	},
	never,
	PortConnector | Scope.Scope
> =>
	Effect.gen(function* () {
		const connector = yield* PortConnector;
		const accepted = yield* connector.listen(name);
		const disconnects = yield* Mailbox.make<number>();
		const peers = yield* makePortPeers;
		const handles = new Map<number, AcceptedPort>();
		let nextClientId = 0;

		const protocol = yield* rpcServer.Protocol.make((writeRequest) =>
			Effect.gen(function* () {
				const readPort = (port: AcceptedPort) => {
					const clientId = nextClientId;
					nextClientId += 1;
					handles.set(clientId, port);
					return Effect.gen(function* () {
						yield* peers.register(clientId, port.peer);
						yield* Effect.logDebug(`accepted ${port.peer._tag}`);
						yield* Stream.runForEach(port.messages, (raw) =>
							Option.match(decodeFromClientFrame(raw), {
								onNone: () => droppedFrame,
								onSome: (frame) =>
									writeRequest(clientId, frame).pipe(
										Effect.catchAllDefect(() => droppedFrame),
									),
							}),
						).pipe(
							Effect.zipRight(port.closed.pipe(Effect.ignore)),
							Effect.ensuring(
								Effect.sync(() => {
									handles.delete(clientId);
								}).pipe(
									Effect.zipRight(peers.unregister(clientId)),
									Effect.zipRight(disconnects.offer(clientId)),
									Effect.zipRight(Effect.logDebug(`closed ${port.peer._tag}`)),
								),
							),
						);
					});
				};

				yield* Stream.runForEach(accepted, (port) =>
					readPort(port).pipe(Effect.forkScoped, Effect.asVoid),
				).pipe(Effect.forkScoped, Effect.asVoid);

				return {
					disconnects,
					send: (clientId: number, response: unknown) => {
						const handle = handles.get(clientId);
						if (handle === undefined) {
							return Effect.void;
						}
						return handle
							.send(response)
							.pipe(Effect.catchAll((error) => Effect.logDebug(error)));
					},
					end: (clientId: number) => {
						const handle = handles.get(clientId);
						if (handle === undefined) {
							return Effect.void;
						}
						return handle.close;
					},
					clientIds: Effect.sync(() => new Set(handles.keys())),
					initialMessage: Effect.succeedNone,
					supportsAck: true,
					supportsTransferables: false,
					supportsSpanPropagation: true,
				};
			}),
		);

		const caller: PortCaller['Type'] = ({ clientId }) =>
			peers.service.get(clientId).pipe(
				Effect.flatMap((endpoint) =>
					Option.match(endpoint, {
						onNone: () => Effect.interrupt,
						onSome: Effect.succeed,
					}),
				),
			);

		return { protocol, caller, peers: peers.service };
	}).pipe(Effect.withLogSpan('PortRpcServer'));
