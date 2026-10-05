import eslint from '@eslint/js';
import prettier from 'eslint-config-prettier';
import tseslint from 'typescript-eslint';

export default tseslint.config(
	{ ignores: ['node_modules/', 'coverage/', 'dist/'] },
	eslint.configs.recommended,
	...tseslint.configs.strictTypeChecked,
	prettier,
	{
		languageOptions: {
			parserOptions: {
				projectService: true,
				tsconfigRootDir: import.meta.dirname,
			},
		},
		rules: {
			'@typescript-eslint/no-confusing-void-expression': 'off',
			// `as const` is a type assertion. Literal annotations stay.
			'@typescript-eslint/prefer-as-const': 'off',
		},
	},
	{
		files: ['*.config.ts'],
		...tseslint.configs.disableTypeChecked,
	},
);
