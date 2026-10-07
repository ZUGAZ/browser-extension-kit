/* eslint-disable @typescript-eslint/no-deprecated -- the task requires it.scoped */
import { describe, expect, it } from '@effect/vitest';
import { Effect, Exit, Fiber, Scope, Stream } from 'effect';

import { settle } from './example-rpcs.test-support';
import { makePageLifecycle } from './page-lifecycle';

const persisted = (type: string, isPersisted: boolean) =>
	Object.assign(new Event(type), { persisted: isPersisted });

describe('page lifecycle', () => {
	it.scoped('tracks persisted page transitions only', () =>
		Effect.gen(function* () {
			const target = new EventTarget();
			const lifecycle = yield* makePageLifecycle(target);
			expect(yield* lifecycle.cached).toBe(false);

			target.dispatchEvent(persisted('pagehide', false));
			target.dispatchEvent(new Event('pagehide'));
			target.dispatchEvent(persisted('pageshow', true));
			expect(yield* lifecycle.cached).toBe(false);

			target.dispatchEvent(persisted('pagehide', true));
			expect(yield* lifecycle.cached).toBe(true);

			target.dispatchEvent(persisted('pageshow', false));
			target.dispatchEvent(new Event('pageshow'));
			expect(yield* lifecycle.cached).toBe(true);

			target.dispatchEvent(persisted('pageshow', true));
			expect(yield* lifecycle.cached).toBe(false);
		}),
	);

	it.scoped('emits the current cached value first', () =>
		Effect.gen(function* () {
			const target = new EventTarget();
			const lifecycle = yield* makePageLifecycle(target);
			const collected = yield* lifecycle.changes.pipe(
				Stream.take(2),
				Stream.runCollect,
				Effect.fork,
			);
			yield* settle;
			target.dispatchEvent(persisted('pagehide', true));
			expect(Array.from(yield* Fiber.join(collected))).toEqual([false, true]);
		}),
	);

	it.scoped('removes listeners when the scope closes', () =>
		Effect.gen(function* () {
			const target = new EventTarget();
			const scope = yield* Scope.make();
			const lifecycle = yield* makePageLifecycle(target).pipe(
				Effect.provideService(Scope.Scope, scope),
			);
			target.dispatchEvent(persisted('pagehide', true));
			expect(yield* lifecycle.cached).toBe(true);
			yield* Scope.close(scope, Exit.void);
			target.dispatchEvent(persisted('pageshow', true));
			expect(yield* lifecycle.cached).toBe(true);
		}),
	);
});
