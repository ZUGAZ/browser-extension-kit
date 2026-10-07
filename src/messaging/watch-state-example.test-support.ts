import {
	Context,
	Deferred,
	Effect,
	Option,
	Ref,
	Schema,
	Stream,
	type SubscriptionRef,
} from 'effect';
import * as rpc from '@effect/rpc/Rpc';
import * as rpcGroup from '@effect/rpc/RpcGroup';

import { PortCaller } from './caller';
import { isContent, type Endpoint } from './endpoint';
import { watchState } from './watch-state';

export class CounterState extends Schema.Class<CounterState>('CounterState')({
	count: Schema.Int,
	ownerTab: Schema.OptionFromNullOr(Schema.Int),
}) {}

export class CounterView extends Schema.Class<CounterView>('CounterView')({
	count: Schema.Int,
	isOwner: Schema.Boolean,
}) {}

export const projectCounter = (state: CounterState, caller: Endpoint) =>
	new CounterView({
		count: state.count,
		isOwner: isContent(caller) && Option.contains(state.ownerTab, caller.tabId),
	});

export const WatchCounter = rpc.make('WatchCounter', {
	success: CounterView,
	stream: true,
});

export const WatchCounterRaw = rpc.make('WatchCounterRaw', {
	success: CounterState,
	stream: true,
});

export const CounterRpcs = rpcGroup
	.make(WatchCounter, WatchCounterRaw)
	.middleware(PortCaller);

export class CounterRef extends Context.Tag('browser-extension-kit/CounterRef')<
	CounterRef,
	SubscriptionRef.SubscriptionRef<CounterState>
>() {}

const counted = <A, E, R>(
	active: Ref.Ref<number>,
	subscribed: Ref.Ref<number>,
	hold: Deferred.Deferred<undefined>,
	stream: Stream.Stream<A, E, R>,
) =>
	Stream.unwrap(
		Deferred.await(hold).pipe(
			Effect.zipRight(Ref.update(active, (count) => count + 1)),
			Effect.zipRight(Ref.update(subscribed, (count) => count + 1)),
			Effect.as(
				stream.pipe(Stream.ensuring(Ref.update(active, (count) => count - 1))),
			),
		),
	);

export const makeCounterHandlers = Effect.gen(function* () {
	const ref = yield* CounterRef;
	const active = yield* Ref.make(0);
	const subscribed = yield* Ref.make(0);
	const hold = yield* Deferred.make<undefined>();
	const layer = CounterRpcs.toLayer({
		WatchCounter: () =>
			counted(
				active,
				subscribed,
				hold,
				watchState(ref, { project: projectCounter }),
			),
		WatchCounterRaw: () => counted(active, subscribed, hold, watchState(ref)),
	});
	return { layer, active, subscribed, hold };
});
