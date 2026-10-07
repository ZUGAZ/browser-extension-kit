import { Deferred, Effect, Layer, Logger, Ref, Schema, Stream } from 'effect';
import * as rpc from '@effect/rpc/Rpc';
import * as rpcGroup from '@effect/rpc/RpcGroup';

import { Caller, PortCaller } from './caller';
import { Endpoint } from './endpoint';
import { PortConnector } from './port-connector';

export class EchoRejected extends Schema.TaggedError<EchoRejected>()(
	'EchoRejected',
	{
		reason: Schema.String,
	},
) {}

export const Echo = rpc.make('Echo', {
	payload: { text: Schema.String },
	success: Schema.String,
	error: EchoRejected,
});

export const Counter = rpc.make('Counter', {
	payload: { upTo: Schema.Int },
	success: Schema.Int,
	stream: true,
});

export const WhoAmI = rpc.make('WhoAmI', {
	success: Endpoint,
});

export const Hang = rpc.make('Hang', {
	success: Schema.Void,
});

export const Explode = rpc.make('Explode', {
	success: Schema.Void,
});

export const ExampleRpcs = rpcGroup
	.make(Echo, Counter, WhoAmI, Hang, Explode)
	.middleware(PortCaller);

export const silentLogger = Logger.replace(Logger.defaultLogger, Logger.none);

const settleSteps = 64;

export const settle: Effect.Effect<void> = Effect.gen(function* () {
	for (let step = 0; step < settleSteps; step++) {
		yield* Effect.yieldNow();
	}
});

export const countConnects = (
	connects: Ref.Ref<number>,
): Layer.Layer<PortConnector, never, PortConnector> =>
	Layer.effect(
		PortConnector,
		Effect.gen(function* () {
			const connector = yield* PortConnector;
			return {
				connect: (target, name) =>
					Ref.update(connects, (count) => count + 1).pipe(
						Effect.andThen(connector.connect(target, name)),
					),
				listen: (name) => connector.listen(name),
			};
		}),
	);

export const makeExampleHandlers = Effect.gen(function* () {
	const started = yield* Deferred.make<undefined>();
	const interrupted = yield* Deferred.make<undefined>();
	const layer = ExampleRpcs.toLayer({
		Echo: ({ text }) =>
			text === 'reject'
				? Effect.fail(new EchoRejected({ reason: 'rejected' }))
				: Effect.succeed(text),
		// One value per chunk. A larger chunk stays queued in the client
		// mailbox offer, which hides a later disconnect from a short drain.
		Counter: ({ upTo }) => Stream.range(1, upTo).pipe(Stream.rechunk(1)),
		WhoAmI: () => Caller,
		Hang: () =>
			Deferred.succeed(started, undefined).pipe(
				Effect.uninterruptible,
				Effect.andThen(
					Effect.never.pipe(
						Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
					),
				),
			),
		Explode: () => Effect.die('handler defect'),
	});
	return { layer, started, interrupted };
});
