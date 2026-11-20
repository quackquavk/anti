# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Clients and customers: outside people, not the developer. They are not technical and have never heard of grid cells, zoom levels, or scraper engines. They arrive wanting a list of businesses of one kind in one place (for example "dentists in Austin") so they can contact them. They use it occasionally, on a laptop, and judge it by whether they got a usable list.

## Product Purpose

Turn "a kind of business + a place" into a contact list (name, phone, email, website, address, rating) pulled from Google Maps. Success: a first-time user starts a search without reading instructions, understands what is happening while it runs, narrows the list to the businesses worth contacting, and downloads it as a CSV.

## Operating Context

- A search is a background job. It takes minutes, longer when emails are collected because each business website is visited.
- Results arrive one by one while the job runs; partial results are kept if a job is stopped or interrupted.
- After a search, users filter first (has email, has phone, rating) and then export CSV to a CRM, spreadsheet, or email tool. The on-screen table is a preview and a filter surface, not a place to work leads.
- Users run several searches over time and come back to old ones.

## Capabilities and Constraints

- Flask app, one HTML template, vanilla JS, no build step. MongoDB stores jobs with results embedded. One gunicorn worker.
- Scraping is done by gosom/google-maps-scraper as a subprocess; the server geocodes the location and picks search mode, zoom, and radius itself.
- A single search returns about 120 places. Above that the server tiles the area into a grid; very large areas (countries) cannot be gridded and fall back to one search.
- Job statuses: pending, running, stopping, stopped, completed, error, interrupted.
- Grid cell size and zoom level stay available but only under Advanced, with defaults.
- Server and job model may change where it makes the product easier to use.
- Open: there are no accounts. Every visitor sees every job. `user_id` exists on the API but the UI does not send it.

## Evidence on Hand

- Real result fields: name, category, address, city, phone, email, website, rating, review_count, location_link.
- No testimonials, pricing, customer names, or usage numbers exist. Do not invent any.

## Product Principles

1. Ask only what the user knows: what, where, how many.
2. Say what is happening and what will happen, in the user's words, never the scraper's.
3. The list is the product. Everything on screen serves getting to a clean, exportable list.
4. Never lose work: partial results are results; destructive actions can be undone or are confirmed in place.
5. Machinery stays out of sight unless something the user can act on went wrong.
