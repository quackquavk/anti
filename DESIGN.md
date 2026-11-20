---
name: Maps Lead List
description: A calm indigo-on-zinc work tool where the downloadable list is the product.
colors:
  brand: "#4f46e5"
  brand-hover: "#4338ca"
  brand-active: "#3730a3"
  brand-50: "#eef2ff"
  brand-100: "#e0e7ff"
  brand-200: "#c7d2fe"
  brand-300: "#a5b4fc"
  rail: "#1e1b4b"
  rail-text: "#eef2ff"
  rail-muted: "#b4bcfb"
  tint: "#f6f7fe"
  bg: "#ffffff"
  bg-subtle: "#fafafa"
  bg-muted: "#f4f4f5"
  border: "#e4e4e7"
  border-strong: "#d4d4d8"
  text: "#18181b"
  text-2: "#52525b"
  text-3: "#6b6b74"
  ok: "#15803d"
  ok-bg: "#f0fdf4"
  warn: "#b45309"
  warn-bg: "#fffbeb"
  warn-border: "#fde68a"
  danger: "#b91c1c"
  danger-solid: "#dc2626"
  danger-bg: "#fef2f2"
  danger-border: "#fecaca"
  star: "#d97706"
  stat-email: "#7c3aed"
  stat-phone: "#0d9488"
typography:
  display:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica Neue, Arial, sans-serif"
    fontSize: "1.875rem"
    fontWeight: 600
    lineHeight: "2.25rem"
    letterSpacing: "-0.025em"
  headline:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.5rem"
    fontWeight: 600
    lineHeight: "2rem"
    letterSpacing: "-0.02em"
  title:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.125rem"
    fontWeight: 600
    lineHeight: "1.75rem"
    letterSpacing: "-0.01em"
  body:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.875rem"
    fontWeight: 400
    lineHeight: "1.25rem"
  body-lg:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: "1.5rem"
  label:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.8125rem"
    fontWeight: 500
    lineHeight: "1.25rem"
  caption:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.75rem"
    fontWeight: 500
    lineHeight: "1rem"
rounded:
  sm: "6px"
  md: "8px"
  lg: "12px"
  xl: "16px"
  pill: "999px"
spacing:
  s-1: "4px"
  s-2: "8px"
  s-3: "12px"
  s-4: "16px"
  s-5: "20px"
  s-6: "24px"
  s-8: "32px"
  s-10: "40px"
  s-12: "48px"
  s-16: "64px"
components:
  button-primary:
    backgroundColor: "{colors.brand}"
    textColor: "{colors.bg}"
    rounded: "{rounded.md}"
    height: "36px"
    padding: "0 12px"
  button-primary-hover:
    backgroundColor: "{colors.brand-hover}"
  button-primary-active:
    backgroundColor: "{colors.brand-active}"
  button-secondary:
    backgroundColor: "{colors.bg}"
    textColor: "{colors.text}"
    rounded: "{rounded.md}"
    height: "36px"
    padding: "0 12px"
  button-ghost:
    backgroundColor: "transparent"
    textColor: "{colors.text-2}"
    rounded: "{rounded.md}"
    height: "36px"
  button-danger:
    backgroundColor: "{colors.danger-solid}"
    textColor: "{colors.bg}"
    rounded: "{rounded.md}"
    height: "36px"
  input:
    backgroundColor: "{colors.bg}"
    textColor: "{colors.text}"
    rounded: "{rounded.md}"
    height: "36px"
    padding: "0 12px"
  choice-selected:
    backgroundColor: "{colors.brand-50}"
    textColor: "{colors.brand-active}"
    rounded: "{rounded.md}"
    height: "44px"
  filter-pill:
    backgroundColor: "{colors.bg}"
    textColor: "{colors.text-2}"
    rounded: "{rounded.pill}"
    height: "32px"
    padding: "0 12px"
  filter-pill-pressed:
    backgroundColor: "{colors.brand-50}"
    textColor: "{colors.brand-active}"
  card:
    backgroundColor: "{colors.bg}"
    rounded: "{rounded.lg}"
    padding: "32px"
  sidebar:
    backgroundColor: "{colors.rail}"
    textColor: "{colors.rail-text}"
    width: "288px"
  table-head:
    backgroundColor: "{colors.tint}"
    textColor: "{colors.text-2}"
    height: "40px"
    padding: "0 16px"
  badge-live:
    backgroundColor: "{colors.brand-50}"
    textColor: "{colors.brand-hover}"
    rounded: "{rounded.pill}"
    height: "24px"
---

# Design System: Maps Lead List

## Overview

**Creative North Star: "The Quiet Ledger"**

A calm work tool where the list is the product. The system is deliberately standard: familiar components, hairline borders, a strict 8px spacing grid, one indigo brand on zinc neutrals. Nothing performs; the table of businesses and the Download button carry the screen. Colour was added at the user's request after a first monochrome pass read as too black-and-white, and it landed as a deep indigo shell plus a single indigo accent, not as a rainbow.

Density is moderate and professional: 14px body text, 36px controls, 32px page gutters, a 288px sidebar and a 720px centred form on a faint indigo tint. Depth is soft and low. Selection, live state and links share one hue so colour always means "this is the active or actionable thing".

The system rejects the dark-gradient "scraper dashboard": no exposed engine settings, no raw logs on the main path, no decorative gradients.

**Key Characteristics:**
- One indigo brand; deep indigo sidebar as the shell; zinc neutrals everywhere else.
- Status colours (green, amber, red) only for status; violet, teal and amber only as the stat icons.
- Inter throughout, fixed rem scale, tabular numerals for counts and table cells.
- 8px control radius, 12px container radius, 1px hairline borders, soft low shadows.
- One loud element per screen, checked by squint test: Find businesses on the form, Download CSV on a search.

## Colors

A restrained indigo-and-zinc palette: saturated colour appears only in the shell, the primary action, selection and live state.

### Primary
- **Working Indigo** (#4f46e5): primary buttons, focus ring, checked checkboxes, selected-state borders, the progress meter, caret and accent-color. Hover (#4338ca), pressed (#3730a3), also the link colour (#4338ca).
- **Indigo Wash** (#eef2ff, with #e0e7ff, #c7d2fe, #a5b4fc as stronger steps): selected choice and filter backgrounds, live badge, new-row fade, input focus halo (#c7d2fe), hover on chips and choices (border #a5b4fc).

### Secondary
- **Deep Indigo Rail** (#1e1b4b): the sidebar only. Text on it is #eef2ff, secondary text #b4bcfb, dividers white at 10%, hover white at 7%, current item white at 14%.

### Neutral
- **Paper White** (#ffffff): page, cards, inputs, table.
- **Indigo-Tinted Paper** (#f6f7fe): the new-search view background and table header row.
- **Zinc Wash** (#fafafa / #f4f4f5): subtle panels, table-row hover, disabled and muted fills.
- **Hairline** (#e4e4e7) and **Strong Hairline** (#d4d4d8): container borders and input borders.
- **Ink** (#18181b), **Slate Text** (#52525b), **Quiet Text** (#6b6b74): primary, secondary and tertiary text.

### Status (only for status)
- **Done Green** (#15803d on #f0fdf4), **Caution Amber** (#b45309 on #fffbeb, border #fde68a), **Error Red** (#b91c1c on #fef2f2, border #fecaca, solid #dc2626 for the destructive button). On the dark rail the dots use brighter steps (#4ade80, #fbbf24, #f87171); the live dot is white with a soft indigo halo so it is the brightest of them.
- **Stat icons** only: indigo (found), violet (#7c3aed, email), teal (#0d9488, phone), amber star (#d97706, rating).

### Named Rules
**The Meaning-of-Colour Rule.** Indigo means active, selected, live or primary action; green, amber and red mean status and nothing else. A colour that is not doing one of those jobs becomes zinc.
**The One Loud Thing Rule.** Each screen has exactly one solid indigo button. Selection is tinted (wash fill, indigo border), never solid, so it cannot compete with the submit action. On the rail, the primary button is demoted to a translucent white fill for the same reason.
**The Link Scarcity Rule.** Links take the link colour on hover only in the table; email alone is the link-coloured column, so the table does not read as a wall of blue.

## Typography

**Display Font:** Inter (self-hosted variable woff2, with ui-sans-serif, system-ui fallbacks)
**Body Font:** Inter
**Label/Mono Font:** Inter; log output only uses the system monospace stack at 12px.

**Character:** One neutral, highly legible sans at modest sizes. Hierarchy comes from weight (500 for labels, 600 for headings) and tight negative tracking on larger sizes, not from size jumps. Stylistic sets cv11 and ss03 are on.

### Hierarchy
- **Display** (600, 1.875rem/2.25rem, -0.025em): new-search page title only; drops to headline size under 40rem.
- **Headline** (600, 1.5rem/2rem, -0.02em): search page title, stat values, page-level empty-state title.
- **Title** (600, 1.125rem/1.75rem, -0.01em): dialog titles.
- **Body** (400, 0.875rem/1.25rem): everything by default, table cells, field labels (500).
- **Body-lg** (400, 1rem/1.5rem): lead paragraph, large inputs, segmented choices, empty-state title, mobile search input (16px prevents iOS zoom).
- **Label** (500, 0.8125rem/1.25rem): filters, chips, hints, breadcrumbs, small buttons.
- **Caption** (500, 0.75rem/1rem): table headers, badges, sidebar group label and meta. Sentence case, no tracking, never uppercase.

### Named Rules
**The Tabular Rule.** Numbers that change or align (stat values, table, counts, choices) use tabular numerals.
**The Weight-Not-Size Rule.** Step up a level with weight and tracking before adding a new size; the scale is fixed (12, 13, 14, 16, 18, 24, 30px).

## Layout

An app shell: a 288px sticky sidebar (brand, New search, recent searches with status dot and count) beside a flexible main column with a 56px sticky breadcrumb bar. Content views are centred: the new-search card at max 720px, the search page at max 1280px with 32px gutters (16px under 56rem).

Spacing is a strict token scale on a 4px base with 8px as the working step (4, 8, 12, 16, 20, 24, 32, 40, 48, 64). Every margin, padding and gap uses a scale token; off-scale values are only control heights and borders. Control heights are 32 (small), 36 (default), 44 (large and touch).

The search page stacks: header (title, status badge, actions right-aligned), a four-up stat strip, a toolbar (filter toggles left, search right), then the results table with a footer. Table layout is fixed with percentage column widths so columns never shift while rows stream in.

Responsive breakpoints: 64rem collapses stats to two columns; 56rem turns the sidebar into an off-canvas drawer with a scrim and a menu button; 40rem stacks the form, makes the primary action full width and first, and turns the table into a list of labelled records (4.5rem label column) instead of scrolling sideways.

## Elevation & Depth

Hybrid, mostly flat: hairline borders define structure; shadows are soft and low and reserved for things that float or are interactive. There are no hard offset shadows.

### Shadow Vocabulary
- **Control** (`0 1px 2px rgb(24 24 27 / 0.05)`): buttons, inputs, choice buttons.
- **Card** (`0 1px 2px rgb(24 24 27 / 0.06), 0 1px 3px rgb(24 24 27 / 0.08)`): the new-search form card.
- **Float** (`0 4px 6px -2px rgb(24 24 27 / 0.05), 0 12px 32px -8px rgb(24 24 27 / 0.18)`): dialog, toasts, mobile drawer.
- **Focus halo** (`0 0 0 3px #c7d2fe`, red variant #fecaca on error): input focus, with a 1px indigo border.

### Named Rules
**The Hairline-First Rule.** The stat strip, table card and advanced panel get a 1px border and no shadow; only the form card and floating layers lift. Pressed buttons drop their shadow.

## Shapes

Gently rounded, conventional. Controls, alerts, option rows and nav items use 8px; cards, stat strip, table card and dialog use 12px; small inner elements (table sort buttons, skeleton bars, brand focus) use 6px; chips, filters and badges are full pills; checkboxes are 5px. The brand mark is a 28px indigo (#6366f1) rounded square holding a white icon. Icons are inline SVG sprite, 1.75 stroke, round caps, 16px (14px small). Selected states double the border with an inset 1px indigo ring.

## Components

### Buttons
- **Shape:** 8px radius, 36px high (32 small, 44 large), 12px horizontal padding (20 large), 14px/500 text, 1px border, control shadow.
- **Primary:** indigo fill and border, white text; hover indigo-700, active indigo-800. On the rail it becomes translucent white (10% fill, 20% border).
- **Secondary (default):** white fill, hairline border; hover zinc wash and strong hairline.
- **Ghost:** transparent, slate text, wash on hover. **Danger:** solid red, used in the delete dialog; the page delete icon button turns red on hover only.
- **Disabled:** zinc fill, quiet text, no shadow. **Loading:** icon replaced by a 14px spinner.
- **Focus:** 2px indigo outline, 2px offset (light indigo on the rail).

### Chips and Filters
- **Example button:** 32px, 8px radius, white, hairline, with a leading search glyph so it reads as "run this search", not as a tag; hover indigo wash with indigo-300 border.
- **Filter toggle:** 32px, 8px radius, built like a checkbox and kept low-key: transparent with a hairline border and secondary-grey text, a 14px box, the label, then a live count. On: zinc-100 fill, slightly stronger border, primary text at weight 500, and the box filled in secondary grey with a check. No shadow, no ink fill, no ring, so a row of filters never competes with the list.

### Cards / Containers
- **Corner Style:** 12px. **Background:** white. **Border:** 1px hairline. **Shadow:** see Elevation. **Internal Padding:** 32px (20px under 40rem) for the form card; stat cells 16px 20px 20px.
- **Stat strip:** four cells divided by hairlines, a 3px progress meter inset at the bottom of the cell.

### Inputs / Fields
- **Style:** 36px (44 large), 8px radius, strong-hairline border, white, control shadow; leading icon at 12px with 36px left padding. Labels above at 14px/500, hints below at 13px quiet text.
- **Focus:** indigo border and 3px indigo-200 halo. **Error:** red border, red 3px-halo, red 13px/500 message replacing the hint.
- **Segmented choices:** real radios drawn as 44px buttons on a four-column grid; checked is tinted, not solid.
- **Checkbox option row:** 16px padding, zinc-subtle fill, 18px checkbox that fills indigo with a white tick.

### Navigation
Deep indigo rail. Items are two-line (title 500, meta 12px muted) with a left status dot (live indigo-300 with a breathing halo, green, amber, red). Hover is white 7%, current is white 14% with white text, no border or stripe. Breadcrumb bar is a translucent white 56px sticky bar with a hairline bottom. On mobile the rail is a drawer under a 40% zinc scrim.

### Table
Fixed layout, 40px header row on indigo-tinted paper with 12px/500 sortable headers, 12px/16px cell padding, rows separated by a muted hairline, row hover is zinc-subtle, new rows fade from indigo wash over 600ms. Missing values show a light zinc dash. Footer is a 48px zinc-subtle strip with the summary.

### Badges, Alerts, Toasts, Dialog
- **Status mark:** no pill. An 18px filled disc with a white glyph specific to the state (check for done, cross for failed, exclamation for interrupted, square for stopped, clock hands for waiting, dash for nothing found), then the word in the status colour. While searching or stopping the disc becomes a turning ring. State reads from shape as well as colour.
- **Notice:** two strengths. A note about a usable list (area too large, place not found, list incomplete) is one plain line: an 18px orange disc, the title at weight 500, then the explanation in secondary text, with no fill and no border. Only a failed search gets a panel: zinc-100 fill, 8px radius, red disc, bold title on its own line. Set by heat-map testing so a note never outranks the search title or the summary numbers.
- **Toast:** dark zinc (#18181b), white text, action in indigo-300, bottom right (full width on mobile).
- **Dialog:** 26rem, 12px radius, Float shadow, 40% zinc backdrop, 180ms pop-in; actions right-aligned (stacked, primary first, on mobile).

### Motion
State changes run 150ms on one expressive ease-out curve (`cubic-bezier(0.16, 1, 0.3, 1)`); dialog 180ms, toast 220ms, drawer 220ms, meter 400ms. Live indication is a slow 2s opacity breathe; skeletons pulse at 1.6s. All animation collapses under `prefers-reduced-motion`.

## Do's and Don'ts

### Do:
- **Do** take every margin, padding and gap from the spacing scale (4, 8, 12, 16, 20, 24, 32, 40, 48, 64px).
- **Do** keep exactly one solid indigo button per screen and show selection as an indigo wash with an indigo border.
- **Do** reserve green, amber and red for status, and use zinc for anything that is neither status nor active.
- **Do** use 8px radius for controls and 12px for containers. No pills: filters and example buttons are 8px controls, and status is a mark plus a word.
- **Do** keep table columns fixed-width and use tabular numerals so streaming rows never shift the row being read.
- **Do** state the row count in the Download button label and put live counts in filter toggles.
- **Do** use 44px targets and 16px input text on mobile.

### Don't:
- **Don't** use gradients, dark dashboard chrome or raw engine logs on the main path.
- **Don't** add a second accent hue; violet, teal and amber exist only as the stat icons.
- **Don't** use uppercase, tracked-out small labels or other eyebrow styling; group labels are sentence-case 12px.
- **Don't** make selected choices or filters solid indigo, and don't colour more than one table column as a link.
- **Don't** add hard offset shadows or side-stripe borders; lift with the soft shadow vocabulary only.
- **Don't** add text to the sidebar primary action that makes it louder than the page's own primary action.

## Themes (added after the first write-up)

The colour sections above describe an indigo palette that was tried and then reverted at the owner's request. What ships is neutral: zinc surfaces, ink (#18181b) for primary buttons and selection, blue only for links and live state. The colour direction is still open, so treat the indigo tokens above as historical until this file is regenerated.

Light and dark share every role token. Dark is set with `data-theme="dark"` on `<html>` and only re-points the tokens in `static/app.css`; no component has theme-specific rules beyond the toggle icon. The toggle sits at the right of the top bar, follows the device setting until someone picks by hand, and stores the choice in `localStorage` (`gms.theme`). An inline script in `<head>` applies it before first paint.

"System voice" surfaces (notices, toasts, the offline strip) use the `--sys-*` tokens and are always the inverse of the page: ink on a light page, near-white on a dark one.

## Buttons (updated)

Buttons follow a reference the owner supplied (Luke Dalton, "Button Designs" on Dribbble): built like small physical keys. Each has a faint top-to-bottom shade, a light inner top edge, a hairline outline and a short drop shadow, and sinks one pixel when pressed. Secondary keys are white to zinc-100; the primary key is zinc-700 to ink with a soft inner highlight; danger is red-500 to red-600. All layers are `--key-*` and `--pkey-*` tokens, re-pointed in dark mode. Ghost buttons stay flat. This is the only place gradients are used.
