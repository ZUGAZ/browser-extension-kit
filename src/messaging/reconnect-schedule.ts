import { Duration, Schedule } from 'effect';

import type { Disconnected } from './connection-errors';

export const defaultReconnectSchedule: Schedule.Schedule<
	unknown,
	Disconnected
> = Schedule.exponential('100 millis', 2).pipe(
	Schedule.union(Schedule.spaced('5 seconds')),
	Schedule.jittered,
);

export const defaultStableAfter: Duration.Duration = Duration.seconds(1);
