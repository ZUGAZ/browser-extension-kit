import { Context } from 'effect';
import * as rpcMiddleware from '@effect/rpc/RpcMiddleware';

import type { Endpoint } from './endpoint';

export class Caller extends Context.Tag('browser-extension-kit/Caller')<
	Caller,
	Endpoint
>() {}

/**
 * Add with `group.middleware(PortCaller)`. Handlers then read {@link Caller}.
 * The value is the accepted port's sender, never the payload or the port name.
 */
export class PortCaller extends rpcMiddleware.Tag<PortCaller>()(
	'browser-extension-kit/PortCaller',
	{ provides: Caller },
) {}
