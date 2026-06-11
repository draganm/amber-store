import { defineConfig } from 'vite';
import solid from 'vite-plugin-solid';

// The app is served by the amber-store binary under /admin/.
export default defineConfig({
  base: '/admin/',
  plugins: [solid()],
  build: { outDir: 'dist' },
});
