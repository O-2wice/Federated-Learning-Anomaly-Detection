// Quarto lists the home page in sitemap.xml as .../index.html, while the site is served,
// verified and submitted for indexing as .../ (quarto-dev/quarto-cli#11365).
const outputDir = Deno.env.get("QUARTO_PROJECT_OUTPUT_DIR");
if (outputDir) {
  const path = `${outputDir}/sitemap.xml`;
  try {
    const xml = await Deno.readTextFile(path);
    const fixed = xml.replaceAll("/index.html</loc>", "/</loc>");
    if (fixed !== xml) await Deno.writeTextFile(path, fixed);
  } catch (e) {
    if (!(e instanceof Deno.errors.NotFound)) throw e;
  }
}
