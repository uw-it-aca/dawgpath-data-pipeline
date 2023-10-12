import { fileURLToPath, URL } from "url";

import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

// https://vitejs.dev/config/
export default defineConfig({
  // MARK: start vite build config

  // vite creates a manifest and assets during the build process (local and prod)
  // django collectstatics will put assets in '/static/dawgpath_pipeline_admin/assets'
  // django will put the manifest in '/static/manifest.json'
  // vite manifest prefaces all files with the path 'dawgpath_pipeline_admin/assets/xxxx'
  build: {
    manifest: true,
    rollupOptions: {
      input: [
        // list all entry points
        "./dawgpath_pipeline_admin_vue/main.js",
      ],
    },
    outDir: "./dawgpath_pipeline_admin/static/", // relative path to django's static directory
    assetsDir: "dawgpath_pipeline_admin/assets", // default ('assets')... this is the namespaced subdirectory of outDir that vite uses
    emptyOutDir: false, // set to false to ensure favicon is not overwritten
  },
  base: "/static/", // allows for proper css url path creation during the build process

  // MARK: standard vite/vue plugin and resolver config
  plugins: [vue()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./dawgpath_pipeline_admin_vue", import.meta.url)),
    },
  },
});
