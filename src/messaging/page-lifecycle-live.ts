import { Effect, Layer } from 'effect';

import { makePageLifecycle, PageLifecycle } from './page-lifecycle';

export const PageLifecycleLive: Layer.Layer<PageLifecycle> = Layer.scoped(
	PageLifecycle,
	Effect.suspend(() => makePageLifecycle(window)),
);
