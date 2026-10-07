import { Data } from 'effect';

import type { PortClosedError } from './connection-errors';

/**
 * State of a reconnecting client's port.
 *
 * `Terminated` is final. The stream emits the current value first.
 */
/* eslint-disable @typescript-eslint/no-empty-object-type -- variants carry no fields */
export type ClientConnection = Data.TaggedEnum<{
	Connecting: {};
	Connected: {};
	Terminated: { readonly error: PortClosedError };
}>;
/* eslint-enable @typescript-eslint/no-empty-object-type */

export const ClientConnection = Data.taggedEnum<ClientConnection>();
