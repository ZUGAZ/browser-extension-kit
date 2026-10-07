import {
	Context,
	Effect,
	Layer,
	Option,
	Runtime,
	Schema,
	Stream,
	SubscriptionRef,
	type Scope,
} from 'effect';

export class PageLifecycle extends Context.Tag(
	'browser-extension-kit/PageLifecycle',
)<
	PageLifecycle,
	{
		readonly cached: Effect.Effect<boolean>;
		readonly changes: Stream.Stream<boolean>;
	}
>() {}

const pageTransition = Schema.Struct({
	persisted: Schema.Boolean,
});

const decodePersisted = Schema.decodeUnknownOption(pageTransition);

/**
 * `pagehide` / `pageshow` with `persisted: true` are the back/forward cache
 * boundaries. Any other event is ignored. The listener bridge matches
 * `acceptPorts`: callbacks call `Runtime.runSync` on a captured runtime.
 */
export const makePageLifecycle = (
	target: EventTarget,
): Effect.Effect<PageLifecycle['Type'], never, Scope.Scope> =>
	Effect.gen(function* () {
		const runtime = yield* Effect.runtime();
		const cached = yield* SubscriptionRef.make(false);

		const publish = (next: boolean) => (event: Event) => {
			Option.match(decodePersisted(event), {
				onNone: () => undefined,
				onSome: (transition) => {
					if (transition.persisted) {
						Runtime.runSync(runtime, SubscriptionRef.set(cached, next));
					}
				},
			});
		};

		const onPageHide = publish(true);
		const onPageShow = publish(false);

		yield* Effect.acquireRelease(
			Effect.sync(() => {
				target.addEventListener('pagehide', onPageHide);
				target.addEventListener('pageshow', onPageShow);
			}),
			() =>
				Effect.sync(() => {
					target.removeEventListener('pagehide', onPageHide);
					target.removeEventListener('pageshow', onPageShow);
				}),
		);

		return {
			cached: SubscriptionRef.get(cached),
			changes: cached.changes,
		};
	});

export const PageLifecycleNone: Layer.Layer<PageLifecycle> = Layer.succeed(
	PageLifecycle,
	{
		cached: Effect.succeed(false),
		changes: Stream.concat(Stream.make(false), Stream.never),
	},
);
