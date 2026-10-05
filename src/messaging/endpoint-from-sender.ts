import { Option } from 'effect';

import {
	Background,
	Content,
	ExtensionPage,
	Popup,
	type Endpoint,
} from './endpoint';

/**
 * Structural view of a runtime message sender. The Chrome sender type must stay
 * assignable to this interface.
 */
export interface SenderInfo {
	readonly id?: string | undefined;
	readonly url?: string | undefined;
	readonly frameId?: number | undefined;
	readonly tab?: { readonly id?: number | undefined } | undefined;
}

export interface ExtensionIdentity {
	readonly id: string;
	readonly origin: string;
	readonly serviceWorkerUrl: Option.Option<string>;
}

const contentPeer = (sender: SenderInfo): Option.Option<Endpoint> =>
	Option.all({
		tabId: Option.fromNullable(sender.tab).pipe(
			Option.flatMap((tab) => Option.fromNullable(tab.id)),
		),
		frameId: Option.fromNullable(sender.frameId),
	}).pipe(Option.map((fields) => new Content(fields)));

const pageOrPopup = (sender: SenderInfo): Option.Option<Endpoint> =>
	Option.fromNullable(sender.tab).pipe(
		Option.match({
			onNone: () => Option.some<Endpoint>(new Popup()),
			onSome: (tab) =>
				Option.fromNullable(tab.id).pipe(
					Option.map((tabId) => new ExtensionPage({ tabId })),
				),
		}),
	);

const onExtensionOrigin = (
	url: string,
	sender: SenderInfo,
	identity: ExtensionIdentity,
): Option.Option<Endpoint> =>
	identity.serviceWorkerUrl.pipe(
		Option.filter((serviceWorkerUrl) => serviceWorkerUrl === url),
		Option.match({
			onNone: () => pageOrPopup(sender),
			onSome: () => Option.some<Endpoint>(new Background()),
		}),
	);

export const endpointFromSender = (
	sender: SenderInfo,
	identity: ExtensionIdentity,
): Option.Option<Endpoint> => {
	if (sender.id !== identity.id) {
		return Option.none();
	}

	return Option.fromNullable(sender.url).pipe(
		Option.filter((url) => url.startsWith(`${identity.origin}/`)),
		Option.match({
			onNone: () => contentPeer(sender),
			onSome: (url) => onExtensionOrigin(url, sender, identity),
		}),
	);
};
