import { describe, expect, layer } from '@effect/vitest';
import { Effect, Equal, Logger, Option } from 'effect';

import { Background, Content, ExtensionPage, Popup } from './endpoint';
import {
	endpointFromSender,
	type ExtensionIdentity,
	type SenderInfo,
} from './endpoint-from-sender';

const silentLogger = Logger.replace(Logger.defaultLogger, Logger.none);

const identity: ExtensionIdentity = {
	id: 'fake-extension-id',
	origin: 'chrome-extension://fake-extension-id',
	serviceWorkerUrl: Option.some(
		'chrome-extension://fake-extension-id/background.js',
	),
};

const cases: ReadonlyArray<{
	readonly name: string;
	readonly sender: SenderInfo;
	readonly expected: Option.Option<
		Background | Popup | ExtensionPage | Content
	>;
}> = [
	{
		name: 'service worker URL',
		sender: {
			id: identity.id,
			url: 'chrome-extension://fake-extension-id/background.js',
		},
		expected: Option.some(new Background()),
	},
	{
		name: 'extension URL with no tab',
		sender: {
			id: identity.id,
			url: 'chrome-extension://fake-extension-id/popup.html',
		},
		expected: Option.some(new Popup()),
	},
	{
		name: 'extension URL with a tab',
		sender: {
			id: identity.id,
			url: 'chrome-extension://fake-extension-id/page.html',
			tab: { id: 5 },
		},
		expected: Option.some(new ExtensionPage({ tabId: 5 })),
	},
	{
		name: 'web URL with a tab and frameId',
		sender: {
			id: identity.id,
			url: 'https://example.com/path',
			tab: { id: 1 },
			frameId: 0,
		},
		expected: Option.some(new Content({ tabId: 1, frameId: 0 })),
	},
	{
		name: 'foreign id',
		sender: {
			id: 'someone-else',
			url: 'chrome-extension://fake-extension-id/popup.html',
		},
		expected: Option.none(),
	},
	{
		name: 'web URL without a tab',
		sender: {
			id: identity.id,
			url: 'https://example.com/path',
			frameId: 0,
		},
		expected: Option.none(),
	},
	{
		name: 'tab without id',
		sender: {
			id: identity.id,
			url: 'https://example.com/path',
			tab: {},
			frameId: 0,
		},
		expected: Option.none(),
	},
	{
		name: 'content without frameId',
		sender: {
			id: identity.id,
			url: 'https://example.com/path',
			tab: { id: 1 },
		},
		expected: Option.none(),
	},
	{
		name: 'look-alike origin',
		sender: {
			id: identity.id,
			url: 'chrome-extension://fake-extension-id-evil/popup.html',
		},
		expected: Option.none(),
	},
];

describe('endpointFromSender', () => {
	layer(silentLogger)((it) => {
		for (const testCase of cases) {
			it.effect(testCase.name, () =>
				Effect.sync(() => {
					const actual = endpointFromSender(testCase.sender, identity);
					expect(Equal.equals(actual, testCase.expected)).toBe(true);
				}),
			);
		}
	});
});
