import { describe, expect, it } from 'vitest';

import { messagingPackage } from './messaging/index';
import { testingPackage } from './testing/index';

describe('package placeholders', () => {
	it('exports the messaging and testing markers', () => {
		expect(messagingPackage).toBe('browser-extension-kit/messaging');
		expect(testingPackage).toBe('browser-extension-kit/testing');
	});
});
