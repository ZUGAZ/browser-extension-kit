import { Schema } from 'effect';

export class Background extends Schema.TaggedClass<Background>()(
	'Background',
	{},
) {}

export class Popup extends Schema.TaggedClass<Popup>()('Popup', {}) {}

export class ExtensionPage extends Schema.TaggedClass<ExtensionPage>()(
	'ExtensionPage',
	{
		tabId: Schema.Int,
	},
) {}

export class Content extends Schema.TaggedClass<Content>()('Content', {
	tabId: Schema.Int,
	frameId: Schema.Int,
}) {}

export const Endpoint = Schema.Union(Background, Popup, ExtensionPage, Content);
export type Endpoint = Schema.Schema.Type<typeof Endpoint>;

export const isBackground = Schema.is(Background);
export const isPopup = Schema.is(Popup);
export const isExtensionPage = Schema.is(ExtensionPage);
export const isContent = Schema.is(Content);

export type ConnectTarget = Background | Content;
