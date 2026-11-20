---
version: 1
slug: "templates-index-html"
primary_target: "templates/index.html"
related_targets: []
---

# Surface: main app (templates/index.html)

Mode: Operate. Audience: non-technical clients building a contact list. Job: start a search, watch it, filter, download CSV.
Constraints: Flask template + static CSS/JS, no build step; 2 GB server, polling must stay light; no accounts yet (open).
User-pinned: professional standard components, best-practice layout, strict spacing. This pin superseded the dealt catalog challengers.

## Direction contract

THESIS: A calm work tool where the list is the product. Refuses the dark gradient "scraper dashboard" with exposed engine settings and raw logs.

OWN-WORLD: Zinc neutrals on white with a slightly grey sidebar; ink (#18181B) for primary buttons and selection; blue only for links and live state; green, amber and red only for status. An indigo palette with a dark indigo sidebar was tried at the user's request for more colour and rejected by the user; the colour direction is open. Inter, fixed rem scale, tabular numerals. 8px spacing grid, 8px control radius, 12px container radius, 1px hairline borders, soft offset shadows. Lucide-style 1.75 stroke icons. One loud element per screen: Find businesses on the form, Download on a search.

STORY: The visitor understands in one screen that they name a business type and a place and get a downloadable list; they believe it is working because businesses appear live with plain status; they filter and download.

FIRST VIEWPORT: 288px sidebar (brand, New search, recent searches with status dot and count). Main: 56px breadcrumb bar; then either the centred 720px new-search card (what, where, how many, email option, advanced disclosure, Find businesses) or the search page (title + status badge, Stop / Download CSV top right, four summary numbers, filter toggles + list search, results table).

FORM: Workspace + guided start, user-chosen from a safer-register hand of own grounded layouts; seed:f7e838aab7c75cce. Signature interaction: filter toggles carry live counts and the Download button always states exactly how many rows it will export. Motion: 150 to 200 ms state transitions only; new rows fade in without shifting the row being read.

FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, DESIGN.md, and every shipping raster carrying its provenance
