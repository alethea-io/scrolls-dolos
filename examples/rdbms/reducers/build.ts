import * as esbuild from "https://deno.land/x/esbuild@v0.17.11/mod.js";
import { denoPlugins } from "https://deno.land/x/esbuild_deno_loader@0.8.2/mod.ts";

Deno.mkdirSync("dist/core/libs/cardano_multiplatform_lib/", {
  recursive: true,
});
Deno.copyFileSync(
  "src/core/libs/cardano_multiplatform_lib/cardano_multiplatform_lib_bg.wasm",
  "dist/core/libs/cardano_multiplatform_lib/cardano_multiplatform_lib_bg.wasm",
);
Deno.copyFileSync(
  "src/core/libs/cardano_multiplatform_lib/cardano_multiplatform_lib.generated.js",
  "dist/core/libs/cardano_multiplatform_lib/cardano_multiplatform_lib.generated.js",
);

const importPathPlugin = {
  name: "core-import-path",
  setup(build: any) {
    build.onResolve({
      filter:
        /^\.\/libs\/cardano_multiplatform_lib\/cardano_multiplatform_lib.generated.js$/,
    }, (args: any) => {
      return {
        path:
          "./core/libs/cardano_multiplatform_lib/cardano_multiplatform_lib.generated.js",
        external: true,
      };
    });
  },
};

await esbuild.build({
  bundle: true,
  format: "esm",
  entryPoints: ["./src/mod.ts"],
  outfile: "./dist/mod.js",
  minify: false,
  plugins: [
    importPathPlugin,
    ...denoPlugins(),
  ],
});

esbuild.stop();
