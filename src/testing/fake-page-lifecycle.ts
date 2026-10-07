import { Effect, Layer, type Scope } from 'effect';

import { makePageLifecycle, PageLifecycle } from '../messaging/page-lifecycle';

export interface FakePageLifecycle {
	readonly layer: Layer.Layer<PageLifecycle>;
	readonly enterCache: Effect.Effect<void>;
	readonly restore: Effect.Effect<void>;
}

const dispatchPersisted = (target: EventTarget, type: string) =>
	Effect.sync(() => {
		target.dispatchEvent(Object.assign(new Event(type), { persisted: true }));
	});

export const makeFakePageLifecycle: Effect.Effect<
	FakePageLifecycle,
	never,
	Scope.Scope
> = Effect.gen(function* () {
	const target = new EventTarget();
	const lifecycle = yield* makePageLifecycle(target);
	return {
		layer: Layer.succeed(PageLifecycle, lifecycle),
		enterCache: dispatchPersisted(target, 'pagehide'),
		restore: dispatchPersisted(target, 'pageshow'),
	};
});
