import {
	Effect,
	Equal,
	Option,
	Stream,
	type Equivalence,
	type SubscriptionRef,
} from 'effect';

import { Caller } from './caller';
import type { Endpoint } from './endpoint';

/**
 * Options for {@link watchState}.
 *
 * `equivalence` overrides `Equal.equals` when consecutive projected values
 * should collapse. For plain structs, pass `Schema.equivalence(View)`.
 */
export interface WatchStateOptions<B> {
	readonly equivalence?: Equivalence.Equivalence<B>;
}

const latest = <A>(ref: SubscriptionRef.SubscriptionRef<A>) =>
	ref.changes.pipe(Stream.buffer({ capacity: 1, strategy: 'sliding' }));

const dedupe =
	<B>(equivalence: Equivalence.Equivalence<B> | undefined) =>
	<E, R>(self: Stream.Stream<B, E, R>) =>
		self.pipe(Stream.changesWith(equivalence ?? Equal.equals));

/**
 * Stream of a `SubscriptionRef` for a watch RPC handler.
 *
 * The first element is the state at subscription time, or a newer one if a
 * slow pull let the sliding buffer replace it. Later elements are distinct
 * changes. A slow consumer receives the latest state, not the backlog.
 *
 * Pass `project` to shape the value per caller. That reads `Caller`, so the
 * group needs `.middleware(PortCaller)`. `Caller` is required only then.
 *
 * A watch RPC is one line, named after the data, never the consumer:
 * `Rpc.make('WatchCounter', { success: CounterView, stream: true })`.
 *
 * Prefer `Schema.Class` values so the default equality is structural, or pass
 * `equivalence: Schema.equivalence(View)`.
 */
export function watchState<A>(
	ref: SubscriptionRef.SubscriptionRef<A>,
	options?: WatchStateOptions<A>,
): Stream.Stream<A>;
export function watchState<A, B>(
	ref: SubscriptionRef.SubscriptionRef<A>,
	options: WatchStateOptions<B> & {
		readonly project: (state: A, caller: Endpoint) => B;
	},
): Stream.Stream<B, never, Caller>;
export function watchState<A, B>(
	ref: SubscriptionRef.SubscriptionRef<A>,
	options?: {
		readonly equivalence?: Equivalence.Equivalence<A | B>;
		readonly project?: (state: A, caller: Endpoint) => B;
	},
): Stream.Stream<A | B, never, Caller> {
	const equivalence = options?.equivalence;
	const unprojected = (): Stream.Stream<A | B, never, Caller> =>
		latest(ref).pipe(dedupe(equivalence));
	const projected = (
		project: (state: A, caller: Endpoint) => B,
	): Stream.Stream<A | B, never, Caller> =>
		Stream.unwrap(
			Effect.map(Caller, (caller) =>
				latest(ref).pipe(
					Stream.map((state) => project(state, caller)),
					dedupe(equivalence),
				),
			),
		);
	return Option.match(Option.fromNullable(options?.project), {
		onNone: unprojected,
		onSome: projected,
	});
}
