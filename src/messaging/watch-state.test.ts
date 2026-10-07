/* eslint-disable @typescript-eslint/no-deprecated -- the task requires it.scoped */
import { describe, expect, it } from '@effect/vitest';
import * as fc from 'effect/FastCheck';
import {
	Effect,
	Equal,
	Fiber,
	Option,
	Queue,
	Ref,
	Stream,
	SubscriptionRef,
} from 'effect';

import { Caller } from './caller';
import { Content, Popup } from './endpoint';
import { settle } from './example-rpcs.test-support';
import {
	CounterState,
	CounterView,
	projectCounter,
} from './watch-state-example.test-support';
import { watchState } from './watch-state';

const counter = (
	count: number,
	ownerTab: Option.Option<number> = Option.none(),
) => new CounterState({ count, ownerTab });

const countsOf = (states: Iterable<CounterState>) =>
	Array.from(states, (state) => state.count);

describe('watchState', () => {
	it.effect('emits the current state first, lazily per run', () =>
		Effect.gen(function* () {
			const ref = yield* SubscriptionRef.make(counter(5));
			const first = countsOf(
				yield* watchState(ref).pipe(Stream.take(1), Stream.runCollect),
			);
			expect(first).toEqual([5]);

			yield* SubscriptionRef.set(ref, counter(6));
			const second = countsOf(
				yield* watchState(ref).pipe(Stream.take(1), Stream.runCollect),
			);
			expect(second).toEqual([6]);

			yield* SubscriptionRef.set(ref, counter(8));
			const third = countsOf(
				yield* watchState(ref).pipe(Stream.take(1), Stream.runCollect),
			);
			expect(third).toEqual([8]);
		}),
	);

	it.scoped('emits updates in order', () =>
		Effect.gen(function* () {
			const ref = yield* SubscriptionRef.make(counter(5));
			const queue = yield* Queue.unbounded<CounterState>();
			yield* watchState(ref).pipe(
				Stream.runForEach((state) => Queue.offer(queue, state)),
				Effect.forkScoped,
			);
			expect(countsOf([yield* Queue.take(queue)])).toEqual([5]);
			yield* SubscriptionRef.set(ref, counter(6));
			expect((yield* Queue.take(queue)).count).toBe(6);
			yield* SubscriptionRef.update(ref, (state) => counter(state.count + 1));
			expect((yield* Queue.take(queue)).count).toBe(7);
		}),
	);

	it.scoped('drops consecutive equals under the default equality', () =>
		Effect.gen(function* () {
			const ref = yield* SubscriptionRef.make(counter(5));
			const queue = yield* Queue.unbounded<CounterState>();
			yield* watchState(ref).pipe(
				Stream.drop(1),
				Stream.runForEach((state) => Queue.offer(queue, state)),
				Effect.forkScoped,
			);
			yield* settle;
			yield* SubscriptionRef.set(ref, counter(5));
			yield* settle;
			expect(yield* Queue.size(queue)).toBe(0);
			yield* SubscriptionRef.set(ref, counter(6));
			expect((yield* Queue.take(queue)).count).toBe(6);
		}),
	);

	it.scoped('uses a caller-supplied equivalence', () =>
		Effect.gen(function* () {
			const ref = yield* SubscriptionRef.make(counter(5, Option.some(1)));
			const queue = yield* Queue.unbounded<CounterState>();
			yield* watchState(ref, {
				equivalence: (left, right) => left.count === right.count,
			}).pipe(
				Stream.drop(1),
				Stream.runForEach((state) => Queue.offer(queue, state)),
				Effect.forkScoped,
			);
			yield* settle;
			yield* SubscriptionRef.set(ref, counter(5, Option.some(2)));
			yield* settle;
			expect(yield* Queue.size(queue)).toBe(0);
			yield* SubscriptionRef.set(ref, counter(6, Option.some(2)));
			expect((yield* Queue.take(queue)).count).toBe(6);
		}),
	);

	it.scoped('projects a distinct value per caller', () =>
		Effect.gen(function* () {
			const ref = yield* SubscriptionRef.make(counter(0, Option.some(1)));
			const popupQueue = yield* Queue.unbounded<CounterView>();
			const tabOneQueue = yield* Queue.unbounded<CounterView>();
			const tabTwoQueue = yield* Queue.unbounded<CounterView>();
			const watchFor = (
				caller: Popup | Content,
				queue: Queue.Queue<CounterView>,
			) =>
				watchState(ref, { project: projectCounter }).pipe(
					Stream.provideService(Caller, caller),
					Stream.runForEach((view) => Queue.offer(queue, view)),
					Effect.forkScoped,
				);
			yield* watchFor(new Popup(), popupQueue);
			yield* watchFor(new Content({ tabId: 1, frameId: 0 }), tabOneQueue);
			yield* watchFor(new Content({ tabId: 2, frameId: 0 }), tabTwoQueue);
			expect(
				Equal.equals(
					yield* Queue.take(popupQueue),
					new CounterView({ count: 0, isOwner: false }),
				),
			).toBe(true);
			expect(
				Equal.equals(
					yield* Queue.take(tabOneQueue),
					new CounterView({ count: 0, isOwner: true }),
				),
			).toBe(true);
			expect(
				Equal.equals(
					yield* Queue.take(tabTwoQueue),
					new CounterView({ count: 0, isOwner: false }),
				),
			).toBe(true);

			yield* SubscriptionRef.set(ref, counter(0, Option.some(2)));
			yield* settle;
			expect(yield* Queue.size(popupQueue)).toBe(0);
			expect(
				Equal.equals(
					yield* Queue.take(tabOneQueue),
					new CounterView({ count: 0, isOwner: false }),
				),
			).toBe(true);
			expect(
				Equal.equals(
					yield* Queue.take(tabTwoQueue),
					new CounterView({ count: 0, isOwner: true }),
				),
			).toBe(true);
		}),
	);

	it.scoped('keeps only the latest state for a slow consumer', () =>
		Effect.gen(function* () {
			const ref = yield* SubscriptionRef.make(counter(0));
			const pull = yield* Stream.toPull(watchState(ref));
			const first = Array.from(yield* pull);
			expect(countsOf(first)).toEqual([0]);
			for (let count = 1; count <= 50; count++) {
				yield* SubscriptionRef.set(ref, counter(count));
			}
			yield* settle;
			const next = Array.from(yield* pull);
			expect(countsOf(next)).toEqual([50]);
		}),
	);

	it.effect.prop(
		'keeps an order-preserving deduped subsequence ending at the final state',
		[fc.array(fc.integer({ min: 0, max: 6 }), { maxLength: 8 })],
		([updates]) =>
			Effect.scoped(
				Effect.gen(function* () {
					const initial = 0;
					const ref = yield* SubscriptionRef.make(counter(initial));
					const received = yield* Ref.make<Array<number>>([]);
					const consuming = yield* watchState(ref).pipe(
						Stream.runForEach((state) =>
							Ref.update(received, (previous) => [...previous, state.count]),
						),
						Effect.forkScoped,
					);
					yield* settle;
					for (const count of updates) {
						yield* SubscriptionRef.set(ref, counter(count));
						yield* settle;
					}
					yield* Fiber.interrupt(consuming);
					const sequence = yield* Ref.get(received);
					const expected = [initial, ...updates];
					let cursor = 0;
					for (const value of sequence) {
						let found = false;
						while (cursor < expected.length) {
							const candidate = expected[cursor];
							cursor += 1;
							if (candidate === value) {
								found = true;
								break;
							}
						}
						expect(found).toBe(true);
					}
					const last = sequence[sequence.length - 1];
					const finalState = expected[expected.length - 1];
					if (last === undefined || finalState === undefined) {
						return yield* Effect.die('missing final state');
					}
					expect(last).toBe(finalState);
					for (let index = 1; index < sequence.length; index++) {
						const current = sequence[index];
						const previous = sequence[index - 1];
						if (current === undefined || previous === undefined) {
							return yield* Effect.die('missing sequence entry');
						}
						expect(current).not.toBe(previous);
					}
				}),
			),
		{ fastCheck: { numRuns: 25 } },
	);
});
