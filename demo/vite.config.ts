import { defineConfig } from 'vite';

// Honor the env var the GitHub Pages workflow sets, so asset URLs resolve
// under https://<user>.github.io/<repo>/. Local `npm run dev` and previews
// fall back to '/'.
const base = process.env.GH_PAGES_BASE || '/';

export default defineConfig({
  base,
  server: { port: 5173 },
});
