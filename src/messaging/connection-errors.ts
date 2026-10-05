import { Schema } from 'effect';

import { Endpoint } from './endpoint';

/** Diagnostic only. Never branch on `detail`. */
export class Disconnected extends Schema.TaggedError<Disconnected>()(
	'Disconnected',
	{
		detail: Schema.optional(Schema.String),
	},
) {}

/** Defined here. Produced by the presence registry. */
export class ReceiverUnavailable extends Schema.TaggedError<ReceiverUnavailable>()(
	'ReceiverUnavailable',
	{
		endpoint: Endpoint,
	},
) {}

/** Defined here. Produced by the RPC protocol. */
export class Timeout extends Schema.TaggedError<Timeout>()('Timeout', {
	afterMillis: Schema.NonNegative,
}) {}

export class ExtensionContextInvalidated extends Schema.TaggedError<ExtensionContextInvalidated>()(
	'ExtensionContextInvalidated',
	{},
) {}

export type PortClosedError = Disconnected | ExtensionContextInvalidated;
