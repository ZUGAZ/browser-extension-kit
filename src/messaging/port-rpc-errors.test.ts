import { describe, expect, it } from '@effect/vitest';
import { Effect, Equal, Schema } from 'effect';
import { RpcClientError } from '@effect/rpc/RpcClientError';

import { Disconnected, ExtensionContextInvalidated } from './connection-errors';
import { toPortRpcError, withPortErrors } from './port-rpc-errors';

class Boom extends Schema.TaggedError<Boom>()('Boom', {}) {}

const protocolError = (message: string, cause?: unknown) =>
	new RpcClientError({
		reason: 'Protocol',
		message,
		...(cause === undefined ? {} : { cause }),
	});

describe('port rpc errors', () => {
	it.effect('maps client errors onto kit errors', () =>
		Effect.sync(() => {
			const disconnected = new Disconnected({ detail: 'gone' });
			expect(
				Equal.equals(
					toPortRpcError(protocolError('Disconnected', disconnected)),
					disconnected,
				),
			).toBe(true);

			const invalidated = new ExtensionContextInvalidated();
			expect(
				Equal.equals(
					toPortRpcError(
						protocolError('ExtensionContextInvalidated', invalidated),
					),
					invalidated,
				),
			).toBe(true);

			expect(
				Equal.equals(
					toPortRpcError(protocolError('lost', new Error('foreign'))),
					new Disconnected({ detail: 'lost' }),
				),
			).toBe(true);
		}),
	);

	it.effect('leaves a non-client tagged error untouched', () =>
		Effect.gen(function* () {
			const boom = new Boom();
			const error = yield* Effect.fail(boom).pipe(
				withPortErrors(),
				Effect.flip,
			);
			expect(Equal.equals(error, boom)).toBe(true);
		}),
	);
});
