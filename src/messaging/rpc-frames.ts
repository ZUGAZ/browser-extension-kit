import { Schema } from 'effect';

import type {
	FromClientEncoded,
	FromServerEncoded,
} from '@effect/rpc/RpcMessage';

const headerPair = Schema.mutable(Schema.Tuple(Schema.String, Schema.String));

const RequestFrame = Schema.Struct({
	_tag: Schema.Literal('Request'),
	id: Schema.String,
	tag: Schema.String,
	payload: Schema.Unknown,
	headers: Schema.Array(headerPair),
	traceId: Schema.optional(Schema.String),
	spanId: Schema.optional(Schema.String),
	sampled: Schema.optional(Schema.Boolean),
});

const AckFrame = Schema.Struct({
	_tag: Schema.Literal('Ack'),
	requestId: Schema.String,
});

const InterruptFrame = Schema.Struct({
	_tag: Schema.Literal('Interrupt'),
	requestId: Schema.String,
});

const PingFrame = Schema.Struct({
	_tag: Schema.Literal('Ping'),
});

const EofFrame = Schema.Struct({
	_tag: Schema.Literal('Eof'),
});

const FromClientFrame = Schema.Union(
	RequestFrame,
	AckFrame,
	InterruptFrame,
	PingFrame,
	EofFrame,
);

const encodedExit = Schema.encodedSchema(
	Schema.Exit({
		success: Schema.Unknown,
		failure: Schema.Unknown,
		defect: Schema.Unknown,
	}),
);

const ChunkFrame = Schema.Struct({
	_tag: Schema.Literal('Chunk'),
	requestId: Schema.String,
	values: Schema.NonEmptyArray(Schema.Unknown),
});

const ExitFrame = Schema.Struct({
	_tag: Schema.Literal('Exit'),
	requestId: Schema.String,
	exit: encodedExit,
});

const DefectFrame = Schema.Struct({
	_tag: Schema.Literal('Defect'),
	defect: Schema.Unknown,
});

const PongFrame = Schema.Struct({
	_tag: Schema.Literal('Pong'),
});

const FromServerFrame = Schema.Union(
	ChunkFrame,
	ExitFrame,
	DefectFrame,
	PongFrame,
);

const identity = <A>(value: A): A => value;

export const clientFramesFitRpc: (
	frame: Schema.Schema.Type<typeof FromClientFrame>,
) => FromClientEncoded = identity;

export const serverFramesFitRpc: (
	frame: Schema.Schema.Type<typeof FromServerFrame>,
) => FromServerEncoded = identity;

export const decodeFromClientFrame =
	Schema.decodeUnknownOption(FromClientFrame);
export const decodeFromServerFrame =
	Schema.decodeUnknownOption(FromServerFrame);
export const isRequestFrame = Schema.is(RequestFrame);
export const isExitFrame = Schema.is(ExitFrame);
