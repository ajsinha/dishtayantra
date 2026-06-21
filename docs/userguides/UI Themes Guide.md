# UI Themes Guide

DishtaYantra's web UI ships several built-in colour themes. Pick one from the
theme menu in the top navigation bar (the icon next to the user menu). Your
choice is remembered in the browser (via `localStorage`) and reapplied on your
next visit; if you've never chosen, the UI follows your operating system's
light/dark preference.

## Available themes

| Theme | Family | Notes |
|-------|--------|-------|
| **Light** | light | Default. Deep-navy + brass accents on white. |
| **Dark** | dark | Low-light navy palette. |
| **Green** | dark | Dark-family with green accents. |
| **Ubuntu** | dark | Dark-family, Ubuntu-inspired aubergine/orange. |
| **Blue** | light | BMO-inspired: BMO Blue (`#0079c0`) on white, deep-navy header gradient, BMO red (`#e11b22`) accent. Added in v5.16.0. |

"Family" determines the Bootstrap base: light-family themes (Light, Blue) use
`data-bs-theme="light"`; the rest use the dark base. This matters only if you
build custom pages — see below.

## How it works

Each theme is a block of CSS custom properties (`--bg-card`, `--text-primary`,
`--primary-gradient`, `--accent-color`, ...) under a `[data-theme="<name>"]`
selector in `web/templates/base.html`. The theme switcher sets the
`data-theme` attribute on `<html>` (and the matching `data-bs-theme` base), and
all pages inherit the variables. There is no server round-trip; switching is
instant and per-browser.

## Building pages that respect themes

If you add custom templates, style them with the theme variables (e.g.
`color: var(--text-primary); background: var(--bg-card);`) rather than hard-coded
colours, so they adapt to every theme automatically.

One caveat for **intentionally dark elements under light-family themes** (Light,
Blue): Bootstrap paints table cells with `--bs-table-bg`, which is light under a
light base and will wash out light text on a dark card. If you have a dark table,
pin its Bootstrap table tokens (`--bs-table-bg: transparent; --bs-table-color:
<light>;` plus the striped/hover variants) so it renders correctly under every
theme. The comparison page's `.cmp-table` is the reference example.

## Adding a new theme

1. Add a `[data-theme="<name>"] { ... }` variable block in `base.html`, mirroring
   an existing theme of the same family.
2. Register the name in the switcher JS (`THEMES`, `ICONS`, and the
   light/dark base mapping in `setTheme`).
3. Add a menu entry in the theme dropdown.

Keep changes additive so existing themes are unaffected.
