(() => {
  "use strict";

  // ------------------------------------------------------------------ setup

  const $ = (id) => document.getElementById(id);
  const APP_NAME = "Google Maps Scraper";
  const ACTIVE = new Set(["pending", "running", "stopping"]);
  const MAX_TOTAL = 10000;
  // Anything this high means "as many as possible", so a percentage or an
  // "x of y" would be noise.
  const OPEN_ENDED_FROM = 9999;
  const PAGE_SIZE = 200;
  const PREFS_KEY = "gms.prefs.v1";

  const state = {
    jobs: null, // sidebar list, without results
    route: { name: "new", id: null },
    job: null, // the open search
    results: [],
    seen: 0, // rows already shown once, so only new ones highlight
    filters: { email: false, phone: false, website: false, rated: false },
    query: "",
    sort: { key: null, dir: 1 },
    limit: PAGE_SIZE,
    shown: null, // when set, only this many results are listed (see heldCount)
    prevStatus: new Map(),
    finishedWhileAway: null,
    firstRoute: true,
  };

  const cache = { list: "", table: "", rows: "", log: "", suggestions: "" };
  let pollTimer = null;
  let detailBusy = false;

  // ---------------------------------------------------------------- helpers

  // Business names and addresses come from scraped pages: untrusted input.
  function esc(value) {
    if (value === null || value === undefined) return "";
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  const fmt = (n) => Number(n || 0).toLocaleString("en-US");
  const plural = (n, one, many) => (n === 1 ? one : many);
  const businesses = (n) => `${fmt(n)} ${plural(n, "business", "businesses")}`;

  function safeUrl(value) {
    if (!value) return null;
    let s = String(value).trim();
    if (!/^https?:\/\//i.test(s)) {
      if (!/^[\w-]+(\.[\w-]+)+/.test(s)) return null;
      s = `https://${s}`;
    }
    try {
      const url = new URL(s);
      return url.protocol === "http:" || url.protocol === "https:" ? url.href : null;
    } catch {
      return null;
    }
  }

  function hostOf(url) {
    try {
      return new URL(url).hostname.replace(/^www\./, "");
    } catch {
      return url;
    }
  }

  function titleOf(job) {
    const q = (job?.config?.search_query || "").trim();
    const loc = (job?.config?.location || "").trim();
    const label = q ? q.charAt(0).toUpperCase() + q.slice(1) : "Untitled search";
    return loc ? `${label} in ${loc}` : label;
  }

  const targetOf = (job) => Number(job?.config?.total) || 0;
  const isOpenEnded = (job) => targetOf(job) >= OPEN_ENDED_FROM;

  function ago(dateStr) {
    const then = new Date(dateStr).getTime();
    if (!then) return "";
    const mins = Math.round((Date.now() - then) / 60000);
    if (mins < 1) return "just now";
    if (mins < 60) return `${mins} min ago`;
    const hours = Math.round(mins / 60);
    if (hours < 24) return `${hours} ${plural(hours, "hour", "hours")} ago`;
    const days = Math.round(hours / 24);
    if (days === 1) return "yesterday";
    if (days < 30) return `${days} days ago`;
    return new Date(then).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  }

  function duration(fromStr) {
    const from = new Date(fromStr).getTime();
    if (!from) return "";
    const mins = Math.max(0, Math.round((Date.now() - from) / 60000));
    if (mins < 1) return "less than a minute";
    if (mins < 60) return `${mins} min`;
    const h = Math.floor(mins / 60);
    const m = mins % 60;
    return m ? `${h} h ${m} min` : `${h} h`;
  }

  function setOffline(isOffline) {
    $("offline").hidden = !isOffline;
  }

  async function api(path, options) {
    let res;
    try {
      res = await fetch(path, options);
    } catch (err) {
      setOffline(true);
      err.network = true;
      throw err;
    }
    setOffline(false);
    let data = null;
    try {
      data = await res.json();
    } catch {
      /* non-JSON body */
    }
    if (!res.ok) {
      const err = new Error((data && data.error) || `Request failed (${res.status})`);
      err.status = res.status;
      err.fields = data && data.fields;
      throw err;
    }
    return data;
  }

  function toast(message, action) {
    const el = document.createElement("div");
    el.className = "toast";
    const text = document.createElement("span");
    text.textContent = message;
    el.append(text);
    if (action) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "toast__action";
      btn.textContent = action.label;
      btn.addEventListener("click", () => {
        action.run();
        dismiss();
      });
      el.append(btn);
    }
    const close = document.createElement("button");
    close.type = "button";
    close.className = "toast__close";
    close.setAttribute("aria-label", "Dismiss");
    close.innerHTML = '<svg class="icon icon--sm" aria-hidden="true"><use href="#i-x" /></svg>';
    close.addEventListener("click", dismiss);
    el.append(close);
    $("toasts").append(el);

    // The reader sets the pace: the countdown stops while the pointer or
    // keyboard focus is on the message and starts over when it leaves.
    const life = action ? 9000 : 5000;
    let timer = setTimeout(dismiss, life);
    const hold = () => clearTimeout(timer);
    const release = () => {
      clearTimeout(timer);
      timer = setTimeout(dismiss, life);
    };
    el.addEventListener("mouseenter", hold);
    el.addEventListener("mouseleave", release);
    el.addEventListener("focusin", hold);
    el.addEventListener("focusout", release);

    function dismiss() {
      clearTimeout(timer);
      el.classList.add("is-leaving");
      setTimeout(() => el.remove(), 220);
    }
  }

  // ---------------------------------------------------------------- sidebar

  function metaOf(job) {
    const n = job.results_count || 0;
    switch (job.status) {
      case "pending":
        return "Waiting to start";
      case "running":
        if (!n) return "Searching";
        return isOpenEnded(job) ? `Searching · ${fmt(n)} found` : `Searching · ${fmt(n)} of ${fmt(targetOf(job))}`;
      case "stopping":
        return "Stopping";
      case "completed":
        return `${n ? businesses(n) : "Nothing found"} · ${ago(job.finished_at || job.updated_at)}`;
      case "stopped":
        return `Stopped · ${fmt(n)} kept`;
      case "interrupted":
        return `Interrupted · ${fmt(n)} kept`;
      case "error":
        return n ? `Did not finish · ${fmt(n)} kept` : "Did not finish";
      default:
        return job.status || "";
    }
  }

  function dotOf(status) {
    if (status === "running" || status === "stopping") return "dot--live";
    if (status === "completed") return "dot--ok";
    if (status === "interrupted") return "dot--warn";
    if (status === "error") return "dot--danger";
    return "";
  }

  function renderList() {
    const list = $("searchList");
    if (state.jobs === null) return;

    let html;
    if (!state.jobs.length) {
      html = '<li class="nav-empty">Your searches will appear here.</li>';
    } else {
      html = state.jobs
        .map((job) => {
          const current = state.route.name === "search" && state.route.id === job._id;
          return `<li><a class="nav-item" href="#/s/${esc(job._id)}"${current ? ' aria-current="page"' : ""}>
            <span class="dot ${job.status === "completed" && !job.results_count ? "" : dotOf(job.status)}" aria-hidden="true"></span>
            <span><span class="nav-item__title">${esc(titleOf(job))}</span>
            <span class="nav-item__meta">${esc(metaOf(job))}</span></span></a></li>`;
        })
        .join("");
    }
    if (html === cache.list) return;
    cache.list = html;

    renderSuggestions();

    // Keep keyboard users where they were across a re-render.
    const focusedHref = document.activeElement?.closest?.("#searchList a")?.getAttribute("href");
    list.innerHTML = html;
    if (focusedHref) list.querySelector(`a[href="${focusedHref}"]`)?.focus();
  }

  async function refreshList() {
    const jobs = await api("/api/jobs");
    for (const job of jobs) {
      const before = state.prevStatus.get(job._id);
      if (before && ACTIVE.has(before) && !ACTIVE.has(job.status)) announceFinished(job);
      state.prevStatus.set(job._id, job.status);
    }
    state.jobs = jobs;
    renderList();
  }

  function announceFinished(job) {
    const viewing = state.route.name === "search" && state.route.id === job._id;
    const n = job.results_count || 0;
    const title = titleOf(job);

    if (document.hidden) {
      state.finishedWhileAway = title;
      document.title = `Done: ${title}`;
    }
    if (viewing && !document.hidden) return;

    const message =
      job.status === "completed"
        ? `${title} is done. ${businesses(n)} found.`
        : job.status === "error"
        ? `${title} did not finish.`
        : `${title} has stopped. ${businesses(n)} kept.`;
    toast(message, viewing ? null : { label: "View", run: () => (location.hash = `#/s/${job._id}`) });
  }

  // ------------------------------------------------------------ search view

  const BADGES = {
    pending: ["Waiting", "", "i-hands"],
    running: ["Searching", "status--live", "i-check"],
    stopping: ["Stopping", "status--busy", "i-check"],
    completed: ["Done", "status--ok", "i-check"],
    stopped: ["Stopped", "", "i-square"],
    interrupted: ["Interrupted", "status--warn", "i-bang"],
    error: ["Did not finish", "status--danger", "i-x"],
  };

  function subOf(job, n) {
    const kept = n ? ` The ${businesses(n)} found ${plural(n, "is", "are")} kept.` : "";
    switch (job.status) {
      case "pending":
        return "Another search is running. This one starts on its own as soon as there is room.";
      case "running":
        if (!n) return "You can close this page; the search keeps going.";
        return `Businesses appear below as they are found. Running for ${duration(job.started_at || job.created_at)}. You can close this page; the search keeps going.`;
      case "stopping":
        return "Finishing up. Everything found so far is kept.";
      case "completed": {
        const when = `Finished ${ago(job.finished_at || job.updated_at)}.`;
        const target = targetOf(job);
        if (n && n < target && !isOpenEnded(job) && !job.notice) {
          return `${when} You asked for ${fmt(target)}; these ${fmt(n)} are every match we found in this area.`;
        }
        return when;
      }
      case "stopped":
        return `You stopped this search.${kept}`;
      case "interrupted":
        return `This search was cut short.${kept}`;
      case "error":
        return `This search ended early.${kept}`;
      default:
        return "";
    }
  }

  function noticeOf(job, n) {
    if (job.status === "error") {
      return {
        danger: true,
        title: "This search did not finish",
        text: `Something went wrong on our side.${n ? ` The ${businesses(n)} found so far ${plural(n, "is", "are")} safe to download.` : ""} Use Search again to retry.`,
      };
    }
    if (job.status === "interrupted") {
      return {
        title: n ? "This list is incomplete" : "This search was interrupted",
        text: "A restart on our side cut the search short. Use Search again to get the full list.",
      };
    }
    if (job.notice) {
      return { title: job.notice_title || "About this list", text: job.notice };
    }
    return null;
  }

  function matches(r) {
    const f = state.filters;
    if (f.email && !r.email) return false;
    if (f.phone && !r.phone) return false;
    if (f.website && !r.website) return false;
    if (f.rated && !((r.rating || 0) >= 4)) return false;
    if (state.query) {
      const hay = `${r.name || ""} ${r.category || ""} ${r.city || ""} ${r.address || ""} ${r.phone || ""} ${r.email || ""} ${r.website || ""}`.toLowerCase();
      if (!hay.includes(state.query)) return false;
    }
    return true;
  }

  const filtersActive = () => Object.values(state.filters).some(Boolean) || Boolean(state.query);

  // While a search is still running and the list is sorted, new businesses
  // would land in the middle and push the row being read out from under the
  // eye. They are held back instead, and the reader lets them in.
  const heldCount = () => (state.shown === null ? 0 : Math.max(0, state.results.length - state.shown));

  function visibleRows() {
    const base = state.shown === null ? state.results : state.results.slice(0, state.shown);
    let rows = base.map((r, i) => ({ r, i })).filter(({ r }) => matches(r));
    const { key, dir } = state.sort;
    if (key) {
      const value = (r) => {
        const v = r[key];
        if (v === null || v === undefined || v === "") return null;
        return key === "rating" ? Number(v) : String(v).toLowerCase();
      };
      rows.sort((a, b) => {
        const x = value(a.r);
        const y = value(b.r);
        if (x === null && y === null) return a.i - b.i;
        if (x === null) return 1; // blanks always last
        if (y === null) return -1;
        if (x < y) return -dir;
        if (x > y) return dir;
        return a.i - b.i;
      });
    }
    return rows;
  }

  const NONE = '<span class="cell-none" aria-label="None">–</span>';

  function rowHtml({ r, i }) {
    const mapUrl = safeUrl(r.location_link);
    const name = esc(r.name || "Unnamed business");
    const meta = [r.category, r.city].filter(Boolean).join(" · ");
    const site = safeUrl(r.website);
    const fresh = i >= state.seen ? ' class="is-new"' : "";

    const nameCell = mapUrl
      ? `<a class="cell-name__title" href="${esc(mapUrl)}" target="_blank" rel="noopener noreferrer" title="${name} (opens in Google Maps)">${name}</a>`
      : `<span class="cell-name__title" title="${name}">${name}</span>`;

    const rating = r.rating
      ? `<span><svg class="icon icon--sm" aria-hidden="true"><use href="#i-star" /></svg><span class="cell-rating__num">${esc(Number(r.rating).toFixed(1))}</span><span class="cell-rating__count">(${fmt(r.review_count)})</span></span>`
      : NONE;

    return `<tr${fresh}>
      <td class="cell-name" data-label="Business">${nameCell}${meta ? `<span class="cell-name__meta" title="${esc(meta)}">${esc(meta)}</span>` : ""}</td>
      <td class="cell-nowrap" data-label="Phone">${r.phone ? esc(r.phone) : NONE}</td>
      <td class="cell-clip" data-label="Email">${r.email ? `<a href="mailto:${esc(r.email)}" title="${esc(r.email)}">${esc(r.email)}</a>` : NONE}</td>
      <td class="cell-clip cell-site" data-label="Website">${site ? `<a href="${esc(site)}" target="_blank" rel="noopener noreferrer" title="${esc(hostOf(site))}">${esc(hostOf(site))}</a>` : NONE}</td>
      <td class="cell-rating" data-label="Rating">${rating}</td>
    </tr>`;
  }

  function skeletonRows(count) {
    const widths = ["62%", "48%", "70%", "55%", "40%"];
    let html = "";
    for (let i = 0; i < count; i += 1) {
      html += `<tr class="skeleton-row" aria-hidden="true">${widths
        .map((w) => `<td><span class="skeleton-bar" style="width:${w}"></span></td>`)
        .join("")}</tr>`;
    }
    return html;
  }

  function setEmpty(title, text, action) {
    const box = $("emptyState");
    box.hidden = !title;
    if (!title) return;
    $("emptyTitle").textContent = title;
    $("emptyText").textContent = text;
    const btn = $("emptyAction");
    btn.hidden = !action;
    if (action) {
      btn.textContent = action.label;
      // When the page has nothing else to offer, the way forward is the one
      // solid button on it.
      btn.className = action.primary ? "btn btn--primary" : "btn btn--sm";
      btn.onclick = action.run;
    }
  }

  function renderTable() {
    const job = state.job;
    const body = $("resultsBody");
    const scroll = document.querySelector(".table-scroll");
    const foot = $("tableFoot");
    const n = state.results.length;

    // Still fetching this search: show the shape of what is coming.
    if (!job) {
      if (cache.table !== "loading") {
        cache.table = "loading";
        $("stats").hidden = true;
        body.innerHTML = skeletonRows(6);
        scroll.hidden = false;
        foot.hidden = true;
        $("toolbar").hidden = true;
        setEmpty(null);
      }
      renderDownload(0, 0);
      return;
    }

    $("toolbar").hidden = n === 0;
    $("stats").hidden = n === 0;
    // No emails were asked for: a column of dashes and a filter that can only
    // empty the list would both be noise.
    const emailOff = job.config && job.config.email === false;
    $("resultsTable").classList.toggle("no-email", Boolean(emailOff));
    document.querySelector('.filter[data-filter="email"]').hidden = Boolean(emailOff);

    if (n === 0) {
      cache.table = `empty:${job.status}`;
      body.innerHTML = "";
      scroll.hidden = true;
      foot.hidden = true;
      const again = { label: "Edit and search again", primary: true, run: () => searchAgain(job) };
      if (job.status === "pending") {
        setEmpty("Waiting for its turn", "Another search is running. This one starts on its own as soon as there is room.");
      } else if (ACTIVE.has(job.status)) {
        // The table's own shape, pulsing, says "rows are coming" better than
        // a paragraph does.
        cache.table = "looking";
        body.innerHTML = skeletonRows(5);
        scroll.hidden = false;
        foot.hidden = false;
        $("showMore").hidden = true;
        $("listSummary").textContent = "Looking for the first businesses. This can take a few minutes.";
        setEmpty(null);
      } else if (job.status === "completed") {
        setEmpty(
          "No businesses found",
          "We found nothing for this search. Check the spelling, or try a broader kind of business or a nearby city.",
          again
        );
      } else {
        setEmpty("Nothing was found before this search ended", "Run it again to get a list.", again);
      }
      renderDownload(0, 0);
      return;
    }

    // The hold ends when the sort is cleared, or once the search is over and
    // nothing is left waiting.
    if (state.shown !== null && (!state.sort.key || (!ACTIVE.has(job.status) && state.shown >= n))) state.shown = null;
    const held = heldCount();
    const rows = visibleRows();
    const key = [n, held, JSON.stringify(state.filters), state.query, state.sort.key, state.sort.dir, state.limit, job.status].join("|");
    if (key !== cache.table) {
      cache.table = key;
      const shown = rows.slice(0, state.limit);
      const html = shown.map(rowHtml).join("");
      if (html !== cache.rows) {
        // Untouched rows are left alone, so a text selection or a hover
        // survives a poll that brought nothing new into view.
        cache.rows = html;
        body.innerHTML = html;
      }
      state.seen = n;

      const newRows = $("newRows");
      newRows.hidden = held === 0;
      if (held) newRows.textContent = `Show ${fmt(held)} new ${plural(held, "business", "businesses")}`;

      scroll.hidden = rows.length === 0;
      foot.hidden = rows.length === 0;

      if (rows.length === 0) {
        setEmpty("No businesses match these filters", "Try removing a filter or changing your search text.", {
          label: "Clear filters",
          run: clearFilters,
        });
      } else {
        setEmpty(null);
      }

      const visibleTotal = n - held;
      let summary = filtersActive() ? `${fmt(rows.length)} of ${businesses(visibleTotal)} match your filters` : businesses(visibleTotal);
      if (shown.length < rows.length) summary += ` · first ${fmt(shown.length)} shown`;
      $("listSummary").textContent = summary;

      const more = $("showMore");
      const remaining = rows.length - shown.length;
      more.hidden = remaining <= 0;
      if (remaining > 0) more.textContent = `Show ${fmt(Math.min(PAGE_SIZE, remaining))} more`;
    }

    // The file always holds everything found, including rows held back above.
    const matching = held ? state.results.filter(matches).length : rows.length;
    renderDownload(matching, n);
  }

  function renderDownload(matching, total) {
    const btn = $("downloadBtn");
    const label = $("downloadLabel");
    if (!state.job || matching === 0) {
      btn.setAttribute("aria-disabled", "true");
      btn.removeAttribute("href");
      btn.removeAttribute("download");
      label.textContent = "Download CSV";
      $("downloadAll").hidden = true;
      return;
    }
    const params = new URLSearchParams({ excel: "1" });
    if (state.filters.email) params.set("has_email", "1");
    if (state.filters.phone) params.set("has_phone", "1");
    if (state.filters.website) params.set("has_website", "1");
    if (state.filters.rated) params.set("min_rating", "4");
    if (state.query) params.set("q", state.query);
    btn.href = `/api/jobs/${state.job._id}/export?${params}`;
    btn.setAttribute("download", "");
    btn.setAttribute("aria-disabled", "false");
    // The button always says how many rows the file will hold.
    label.textContent = `Download ${fmt(matching)} as CSV`;
    btn.title = matching < total ? "Downloads only the businesses that match your filters" : "";

    // Filtering the list should never trap someone into a filtered file.
    const all = $("downloadAll");
    all.hidden = matching >= total;
    if (matching < total) {
      all.href = `/api/jobs/${state.job._id}/export?excel=1`;
      all.textContent = `Download all ${fmt(total)} instead`;
    }
  }

  function renderStats() {
    const job = state.job;
    const res = state.results;
    const n = job ? Math.max(job.results_count || 0, res.length) : 0;
    let email = 0;
    let phone = 0;
    let website = 0;
    let rated4 = 0;
    let rated = 0;
    let ratingSum = 0;
    for (const r of res) {
      if (r.email) email += 1;
      if (r.phone) phone += 1;
      if (r.website) website += 1;
      if (r.rating) {
        rated += 1;
        ratingSum += Number(r.rating);
        if (r.rating >= 4) rated4 += 1;
      }
    }
    const pct = (k) => (res.length ? `${Math.round((k / res.length) * 100)}%` : "");

    $("stFound").textContent = fmt(n);
    const meter = $("stMeter");
    if (!job || isOpenEnded(job)) {
      $("stFoundOf").textContent = job && ACTIVE.has(job.status) ? "so far" : "";
      meter.hidden = true;
    } else {
      const target = targetOf(job);
      $("stFoundOf").textContent = target ? `of ${fmt(target)}` : "";
      // Progress is only meaningful while the search is still going. On a
      // finished one a full bar beside "14 of 100" would contradict the number.
      const value = target ? Math.min(100, Math.round((n / target) * 100)) : 0;
      meter.hidden = !ACTIVE.has(job.status);
      meter.setAttribute("aria-valuenow", String(value));
      $("stMeterFill").style.width = `${value}%`;
    }

    const emailOff = job && job.config && job.config.email === false;
    $("stEmail").textContent = emailOff ? "–" : fmt(email);
    $("stEmailPct").textContent = emailOff ? "not collected" : pct(email);
    $("stPhone").textContent = fmt(phone);
    $("stPhonePct").textContent = pct(phone);
    $("stRating").textContent = rated ? (ratingSum / rated).toFixed(1) : "–";
    $("stRatingOf").textContent = rated ? `${fmt(rated)} rated` : "";

    $("fcEmail").textContent = fmt(email);
    $("fcPhone").textContent = fmt(phone);
    $("fcWebsite").textContent = fmt(website);
    $("fcRated").textContent = fmt(rated4);
  }

  function renderHead() {
    // Before the full record arrives, the sidebar entry already knows the title.
    const job = state.job || (state.jobs || []).find((j) => j._id === state.route.id);
    const title = job ? titleOf(job) : "Loading search";
    $("sTitle").textContent = title;
    $("crumbHere").textContent = title;

    const badge = $("sBadge");
    const stop = $("stopBtn");
    const again = $("againBtn");
    const notice = $("sNotice");

    if (!job) {
      badge.hidden = true;
      $("sPlace").hidden = true;
      $("sSub").textContent = "";
      stop.hidden = true;
      again.hidden = true;
      notice.hidden = true;
      document.title = APP_NAME;
      return;
    }

    const n = Math.max(job.results_count || 0, state.job ? state.results.length : 0);
    const nothing = job.status === "completed" && !n;
    const [label, cls, icon] = nothing ? ["Nothing found", "", "i-minus"] : BADGES[job.status] || [job.status, "", "i-minus"];
    badge.hidden = false;
    const badgeKey = `${label}|${cls}`;
    if (badge.dataset.key !== badgeKey) {
      // Only on change, so the turning ring is not restarted by every poll.
      badge.dataset.key = badgeKey;
      badge.className = `status ${cls}`.trim();
      badge.innerHTML = `<span class="status__mark" aria-hidden="true"><svg class="icon"><use href="#${icon}" /></svg></span>${esc(label)}`;
    }
    $("sSub").textContent = subOf(job, n);

    // Which place the search actually landed on, so "Springfield" going to
    // the wrong state is caught in the first minute and not after an hour.
    const place = $("sPlace");
    place.hidden = !job.matched_place;
    if (job.matched_place) {
      place.textContent = ACTIVE.has(job.status)
        ? `Searching in: ${job.matched_place}. Wrong place? Stop the search and try a more specific location.`
        : `Area searched: ${job.matched_place}.`;
    }

    const active = ACTIVE.has(job.status);
    stop.hidden = !active;
    again.hidden = active;
    if (active) {
      const stopping = job.status === "stopping";
      stop.disabled = stopping;
      stop.classList.toggle("is-loading", stopping);
      stop.querySelector(".btn__label").textContent = stopping ? "Stopping" : job.status === "pending" ? "Cancel search" : "Stop search";
    }

    const note = state.job ? noticeOf(job, n) : null;
    notice.hidden = !note;
    if (note) {
      notice.classList.toggle("notice--danger", Boolean(note.danger));
      $("sNoticeIcon").setAttribute("href", note.danger ? "#i-x" : "#i-bang");
      $("sNoticeTitle").textContent = note.title;
      $("sNoticeText").textContent = note.text;
    }

    if (!document.hidden) {
      document.title = `${job.status === "running" && n ? `(${fmt(n)}) ` : ""}${title} · ${APP_NAME}`;
    }
  }

  function renderLog() {
    const job = state.job;
    const logs = (job && job.logs) || [];
    const key = `${job ? job._id : ""}:${logs.length}:${logs[logs.length - 1] || ""}:${job ? job.error : ""}`;
    if (key === cache.log) return;
    cache.log = key;

    const err = $("techError");
    err.hidden = !(job && job.error);
    err.textContent = job && job.error ? `Error: ${job.error}` : "";

    const el = $("log");
    // Follow new lines only if the reader is already at the bottom.
    const following = el.scrollHeight - el.scrollTop - el.clientHeight < 24;
    el.innerHTML = logs.length ? logs.map((line) => `<li>${esc(line)}</li>`).join("") : "<li></li>";
    if (following) el.scrollTop = el.scrollHeight;
  }

  function renderSearch() {
    renderHead();
    renderStats();
    renderTable();
    renderLog();
    renderSortHeaders();
    for (const btn of document.querySelectorAll(".filter")) {
      btn.setAttribute("aria-pressed", String(state.filters[btn.dataset.filter]));
    }
    $("clearFilters").hidden = !filtersActive();
    saveView();
  }

  // How someone left a list is how they find it: filters, sort and search
  // text are kept per search for the browser session.
  const viewKey = (id) => `gms.view.${id}`;

  function saveView() {
    if (state.route.name !== "search" || !state.job) return;
    try {
      sessionStorage.setItem(viewKey(state.route.id), JSON.stringify({ filters: state.filters, query: state.query, sort: state.sort }));
    } catch {
      /* storage unavailable: the view simply resets next time */
    }
  }

  function loadView(id) {
    try {
      const v = JSON.parse(sessionStorage.getItem(viewKey(id)));
      if (v && v.filters && v.sort) return v;
    } catch {
      /* ignore */
    }
    return null;
  }

  function renderSortHeaders() {
    for (const th of document.querySelectorAll("th[data-sort]")) {
      const on = state.sort.key === th.dataset.sort;
      const use = th.querySelector("use");
      if (!on) {
        th.removeAttribute("aria-sort");
        use.setAttribute("href", "#i-sort");
      } else {
        const asc = state.sort.dir === 1;
        th.setAttribute("aria-sort", asc ? "ascending" : "descending");
        use.setAttribute("href", asc ? "#i-sort-up" : "#i-sort-down");
      }
    }
  }

  async function refreshDetail(force) {
    if (state.route.name !== "search" || detailBusy) return;
    if (!force && state.job && !ACTIVE.has(state.job.status)) return;

    const id = state.route.id;
    const since = state.job ? state.results.length : 0;
    detailBusy = true;
    try {
      const job = await api(`/api/jobs/${id}?slim=1&since=${since}`);
      if (state.route.name !== "search" || state.route.id !== id) return; // user moved on

      const incoming = job.results || [];
      delete job.results;
      const first = !state.job;

      if ((job.results_count || 0) < since) {
        // The list shrank under us (re-run on the server): start over.
        state.job = null;
        state.results = [];
        state.seen = 0;
        detailBusy = false;
        return refreshDetail(true);
      }
      if ((job.results_offset || 0) === state.results.length) {
        for (const r of incoming) state.results.push(r);
      }
      state.job = job;
      if (first) state.seen = state.results.length; // nothing is "new" on open
      state.prevStatus.set(job._id, job.status);
      renderSearch();
    } catch (err) {
      if (err.status === 404 || err.status === 400) {
        if (state.route.id === id) showMissing();
      }
    } finally {
      detailBusy = false;
    }
  }

  // --------------------------------------------------------------- polling

  async function tick() {
    clearTimeout(pollTimer);
    const tasks = [refreshList()];
    if (!document.hidden) tasks.push(refreshDetail(false));
    await Promise.allSettled(tasks);

    if (state.route.name === "search" && state.job) renderHead(); // keeps "running for" current

    const busy =
      (state.jobs || []).some((j) => ACTIVE.has(j.status)) || (state.job && ACTIVE.has(state.job.status));
    const delay = document.hidden ? 15000 : busy ? 3000 : 20000;
    pollTimer = setTimeout(tick, delay);
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) return;
    state.finishedWhileAway = null;
    if (state.route.name === "new") document.title = `New search · ${APP_NAME}`;
    tick();
  });

  // --------------------------------------------------------------- actions

  function clearFilters() {
    state.filters = { email: false, phone: false, website: false, rated: false };
    state.query = "";
    state.limit = PAGE_SIZE;
    $("listSearch").value = "";
    renderSearch();
  }

  for (const btn of document.querySelectorAll(".filter")) {
    btn.addEventListener("click", () => {
      const name = btn.dataset.filter;
      state.filters[name] = !state.filters[name];
      state.limit = PAGE_SIZE;
      renderSearch();
    });
  }

  $("clearFilters").addEventListener("click", clearFilters);

  let searchDebounce = null;
  $("listSearch").addEventListener("input", (e) => {
    clearTimeout(searchDebounce);
    const value = e.target.value.trim().toLowerCase();
    searchDebounce = setTimeout(() => {
      state.query = value;
      state.limit = PAGE_SIZE;
      renderSearch();
    }, 120);
  });

  $("showMore").addEventListener("click", () => {
    state.limit += PAGE_SIZE;
    renderSearch();
  });

  for (const th of document.querySelectorAll("th[data-sort]")) {
    th.querySelector("button").addEventListener("click", () => {
      const key = th.dataset.sort;
      // Rating reads best high to low; text columns read best A to Z.
      const firstDir = key === "rating" ? -1 : 1;
      if (state.sort.key !== key) state.sort = { key, dir: firstDir };
      else if (state.sort.dir === firstDir) state.sort = { key, dir: -firstDir };
      else state.sort = { key: null, dir: 1 };
      const live = state.job && ACTIVE.has(state.job.status);
      state.shown = state.sort.key && live ? state.results.length : null;
      renderSearch();
    });
  }

  $("newRows").addEventListener("click", () => {
    state.shown = state.job && ACTIVE.has(state.job.status) && state.sort.key ? state.results.length : null;
    renderSearch();
  });

  // One confirmation dialog for the two things that cannot be taken back.
  const confirmDialog = $("deleteDialog");
  let onConfirm = null;

  function confirmAction({ title, text, confirmLabel, cancelLabel }, run) {
    $("deleteTitle").textContent = title;
    $("deleteText").textContent = text;
    $("confirmDelete").textContent = confirmLabel;
    $("cancelDialog").textContent = cancelLabel;
    onConfirm = run;
    confirmDialog.returnValue = "";
    confirmDialog.showModal();
  }

  confirmDialog.addEventListener("close", () => {
    const run = onConfirm;
    onConfirm = null;
    if (confirmDialog.returnValue === "delete" && run) run();
  });

  // Click on the backdrop closes the dialog, like every native sheet.
  confirmDialog.addEventListener("click", (e) => {
    if (e.target === confirmDialog) confirmDialog.close("cancel");
  });

  $("stopBtn").addEventListener("click", () => {
    const job = state.job;
    if (!job) return;
    const n = Math.max(job.results_count || 0, state.results.length);
    const ranFor = Date.now() - new Date(job.started_at || job.created_at).getTime();
    // A stopped search cannot be resumed. Past the first couple of minutes a
    // stray click would throw away real waiting time, so ask first.
    if (job.status === "running" && (ranFor > 120000 || n >= 20)) {
      confirmAction(
        {
          title: "Stop this search?",
          text: `A stopped search cannot be picked up again. The ${businesses(n)} found so far ${plural(n, "is", "are")} kept.`,
          confirmLabel: "Stop search",
          cancelLabel: "Keep searching",
        },
        stopSearch
      );
    } else {
      stopSearch();
    }
  });

  async function stopSearch() {
    const job = state.job;
    if (!job) return;
    const btn = $("stopBtn");
    btn.disabled = true;
    try {
      await api(`/api/jobs/${job._id}/stop`, { method: "POST" });
      job.status = job.status === "pending" ? "stopped" : "stopping";
      renderSearch();
      tick();
    } catch (err) {
      btn.disabled = false;
      if (err.status === 400) tick(); // it finished on its own a moment ago
      else toast("We could not stop the search. Check your connection and try again.");
    }
  }

  $("againBtn").addEventListener("click", () => state.job && searchAgain(state.job));

  $("deleteBtn").addEventListener("click", () => {
    const job = state.job;
    if (!job) return;
    const n = Math.max(job.results_count || 0, state.results.length);
    const list = n ? `its list of ${businesses(n)}` : "it";
    confirmAction(
      {
        title: "Delete this search?",
        text: ACTIVE.has(job.status)
          ? `This search is still running. Deleting it stops it and removes ${list} for good.`
          : `This removes “${titleOf(job)}” and ${list} for good. Download the CSV first if you still need it.`,
        confirmLabel: "Delete search",
        cancelLabel: "Keep it",
      },
      deleteSearch
    );
  });

  async function deleteSearch() {
    if (!state.job) return;
    const job = state.job;
    try {
      await api(`/api/jobs/${job._id}`, { method: "DELETE" });
    } catch (err) {
      if (err.status !== 404) {
        toast("We could not delete the search. Check your connection and try again.");
        return;
      }
    }
    state.jobs = (state.jobs || []).filter((j) => j._id !== job._id);
    state.prevStatus.delete(job._id);
    try {
      sessionStorage.removeItem(viewKey(job._id));
    } catch {
      /* ignore */
    }
    toast("Search deleted.");
    location.hash = "#/new";
    renderList();
  }

  // --------------------------------------------------------------- the form

  const form = $("searchForm");
  const radios = () => Array.from(form.querySelectorAll('input[name="total"]'));

  function chosenTotal() {
    const choice = radios().find((r) => r.checked)?.value || "100";
    if (choice === "max") return MAX_TOTAL;
    if (choice === "other") {
      const raw = $("customTotal").value.trim();
      return /^\d+$/.test(raw) ? Number(raw) : NaN;
    }
    return Number(choice);
  }

  function updateTotalHint() {
    const choice = radios().find((r) => r.checked)?.value;
    $("customTotalWrap").hidden = choice !== "other";
    const n = chosenTotal();
    let hint;
    if (choice === "other" && !n) {
      hint = "Any number from 1 to 10,000.";
    } else if (n <= 120) {
      hint = "Usually takes 5 to 15 minutes, sometimes longer.";
    } else if (n <= 500) {
      hint = "Larger searches cover the area piece by piece and can take an hour or more. You can close this page; the search keeps running.";
    } else if (choice === "max") {
      hint = "Keeps going until it runs out of businesses, which can take many hours in a big city. You can close this page, and stop any time without losing what was found.";
    } else {
      hint = "This can take many hours in a big city. You can close this page, and stop any time without losing what was found.";
    }
    $("totalHint").textContent = hint;
  }

  function setError(fieldId, errorId, message) {
    const field = $(fieldId);
    const error = $(errorId);
    field.classList.toggle("has-error", Boolean(message));
    error.hidden = !message;
    error.textContent = message || "";
    const input = field.querySelector("input[type=text], input[type=number]");
    if (input) input.setAttribute("aria-invalid", String(Boolean(message)));
  }

  function clearErrors() {
    setError("fieldQuery", "qError", "");
    setError("fieldLocation", "locError", "");
    setError("fieldTotal", "totalError", "");
    $("formStatus").hidden = true;
  }

  function loadPrefs() {
    try {
      return JSON.parse(localStorage.getItem(PREFS_KEY)) || {};
    } catch {
      return {};
    }
  }

  function savePrefs() {
    try {
      localStorage.setItem(
        PREFS_KEY,
        JSON.stringify({
          total: radios().find((r) => r.checked)?.value,
          custom: $("customTotal").value,
          email: $("email").checked,
          grid: $("gridCell").value,
          zoom: $("zoom").value,
        })
      );
    } catch {
      /* private mode: preferences just do not persist */
    }
  }

  function setTotalChoice(total) {
    const n = Number(total);
    const preset = radios().find((r) => r.value === String(n));
    if (n >= OPEN_ENDED_FROM) radios().find((r) => r.value === "max").checked = true;
    else if (preset) preset.checked = true;
    else {
      radios().find((r) => r.value === "other").checked = true;
      $("customTotal").value = n || "";
    }
    updateTotalHint();
  }

  function applyPrefs() {
    const p = loadPrefs();
    if (p.total && radios().some((r) => r.value === p.total)) {
      radios().find((r) => r.value === p.total).checked = true;
      if (p.total === "other" && p.custom) $("customTotal").value = p.custom;
    }
    if (typeof p.email === "boolean") $("email").checked = p.email;
    if (p.grid) $("gridCell").value = p.grid;
    if (p.zoom) $("zoom").value = p.zoom;
    if (($("gridCell").value !== "1" || $("zoom").value !== "15") && (p.grid || p.zoom)) $("advanced").open = true;
    updateTotalHint();
  }

  function searchAgain(job) {
    const c = job.config || {};
    $("q").value = c.search_query || "";
    $("loc").value = c.location || "";
    setTotalChoice(c.total || 100);
    $("email").checked = c.email !== false;
    if (c.grid_cell_km) $("gridCell").value = c.grid_cell_km;
    if (c.zoom_level) $("zoom").value = c.zoom_level;
    clearErrors();
    state.focusSubmit = true;
    location.hash = "#/new";
  }

  form.addEventListener("change", (e) => {
    if (e.target.name === "total") {
      updateTotalHint();
      setError("fieldTotal", "totalError", "");
      if (e.target.value === "other") $("customTotal").focus();
    }
  });
  $("customTotal").addEventListener("input", () => {
    updateTotalHint();
    setError("fieldTotal", "totalError", "");
  });
  $("q").addEventListener("input", () => setError("fieldQuery", "qError", ""));
  $("loc").addEventListener("input", () => setError("fieldLocation", "locError", ""));

  $("resetAdvanced").addEventListener("click", () => {
    $("gridCell").value = "1";
    $("zoom").value = "15";
    $("gridCell").focus();
  });

  // Under the form: the person's own recent searches, ready to run again or
  // tweak. The three stock examples only show to someone with no history.
  const EXAMPLES_HTML = $("examples").innerHTML;

  function renderSuggestions() {
    const box = $("examples");
    const seen = new Set();
    const recent = [];
    for (const job of state.jobs || []) {
      const c = job.config || {};
      const q = (c.search_query || "").trim();
      const loc = (c.location || "").trim();
      const key = `${q.toLowerCase()}|${loc.toLowerCase()}`;
      if (!q || seen.has(key)) continue;
      seen.add(key);
      recent.push({ q, loc, total: c.total || 100, email: c.email !== false, label: titleOf(job) });
      if (recent.length === 4) break;
    }
    const html = recent.length
      ? `<span class="examples__label">Search again:</span>${recent
          .map(
            (r) =>
              `<button class="chip" type="button" data-q="${esc(r.q)}" data-loc="${esc(r.loc)}" data-total="${esc(r.total)}" data-email="${r.email ? "1" : "0"}"><svg class="icon icon--sm" aria-hidden="true"><use href="#i-refresh" /></svg>${esc(r.label)}</button>`
          )
          .join("")}`
      : EXAMPLES_HTML;
    if (html !== cache.suggestions) {
      cache.suggestions = html;
      box.innerHTML = html;
    }
  }

  // Picking one fills the form and stops there: the person presses the button.
  $("examples").addEventListener("click", (e) => {
    const chip = e.target.closest(".chip");
    if (!chip) return;
    $("q").value = chip.dataset.q;
    $("loc").value = chip.dataset.loc;
    if (chip.dataset.total) setTotalChoice(chip.dataset.total);
    if (chip.dataset.email) $("email").checked = chip.dataset.email === "1";
    clearErrors();
    $("submitBtn").focus();
  });

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const btn = $("submitBtn");
    if (btn.disabled) return;
    clearErrors();

    const query = $("q").value.trim();
    const loc = $("loc").value.trim();
    const total = chosenTotal();
    let firstBad = null;

    if (!query) {
      setError("fieldQuery", "qError", "Enter the kind of business you are looking for.");
      firstBad = firstBad || $("q");
    }
    if (!loc) {
      setError("fieldLocation", "locError", "Enter a city or neighbourhood.");
      firstBad = firstBad || $("loc");
    }
    if (!Number.isInteger(total) || total < 1 || total > MAX_TOTAL) {
      setError("fieldTotal", "totalError", "Enter a whole number from 1 to 10,000.");
      firstBad = firstBad || $("customTotal");
    }
    if (firstBad) {
      firstBad.focus();
      return;
    }

    const config = { search_query: query, location: loc, total, email: $("email").checked };
    const grid = parseFloat($("gridCell").value);
    const zoom = parseInt($("zoom").value, 10);
    if (grid >= 0.2 && grid <= 20) config.grid_cell_km = grid;
    if (zoom >= 10 && zoom <= 21) config.zoom_level = zoom;

    btn.disabled = true;
    btn.classList.add("is-loading");
    btn.querySelector(".btn__label").textContent = "Starting search";
    try {
      const data = await api("/api/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config),
      });
      savePrefs();
      const now = new Date().toUTCString();
      const optimistic = { _id: data.job_id, config, status: "pending", results_count: 0, created_at: now, updated_at: now };
      state.jobs = [optimistic, ...(state.jobs || [])];
      state.prevStatus.set(data.job_id, "pending");
      // What was typed stays in the form: the next search is usually a small
      // change to this one (same trade, next town).
      location.hash = `#/s/${data.job_id}`;
    } catch (err) {
      const fields = err.fields || {};
      if (fields.search_query) setError("fieldQuery", "qError", fields.search_query);
      if (fields.location) setError("fieldLocation", "locError", fields.location);
      if (fields.total) setError("fieldTotal", "totalError", fields.total);
      if (!Object.keys(fields).length) {
        const status = $("formStatus");
        status.hidden = false;
        status.textContent = err.network
          ? "We could not reach the server. Check your connection and try again."
          : "We could not start the search. Please try again in a moment.";
      }
    } finally {
      btn.disabled = false;
      btn.classList.remove("is-loading");
      btn.querySelector(".btn__label").textContent = "Find businesses";
    }
  });

  // ---------------------------------------------------------------- routing

  function parseRoute() {
    const match = location.hash.match(/^#\/s\/([a-f0-9]{24})$/i);
    return match ? { name: "search", id: match[1] } : { name: "new", id: null };
  }

  function show(viewId) {
    for (const id of ["viewNew", "viewSearch", "viewMissing"]) $(id).hidden = id !== viewId;
  }

  function showMissing() {
    show("viewMissing");
    $("crumbHere").textContent = "Not found";
    document.title = APP_NAME;
    $("missingTitle").focus();
  }

  function onRoute() {
    const next = parseRoute();
    const changed = next.name !== state.route.name || next.id !== state.route.id;
    state.route = next;
    closeSidebar();

    if (next.name === "search") {
      if (changed || !state.job) {
        state.job = null;
        state.results = [];
        state.seen = 0;
        state.limit = PAGE_SIZE;
        state.shown = null;
        const saved = loadView(next.id);
        state.sort = saved ? saved.sort : { key: null, dir: 1 };
        state.filters = saved ? { email: false, phone: false, website: false, rated: false, ...saved.filters } : { email: false, phone: false, website: false, rated: false };
        state.query = saved ? String(saved.query || "") : "";
        $("listSearch").value = state.query;
        $("tech").open = false;
        cache.table = "";
        cache.rows = "";
        cache.log = "";
      }
      show("viewSearch");
      renderSearch();
      if (!state.firstRoute) $("sTitle").focus();
      window.scrollTo(0, 0);
    } else {
      show("viewNew");
      $("crumbHere").textContent = "New search";
      document.title = `New search · ${APP_NAME}`;
      window.scrollTo(0, 0);
      if (state.focusSubmit) {
        state.focusSubmit = false;
        $("submitBtn").focus();
      } else if (window.matchMedia("(hover: hover) and (min-width: 56rem)").matches) {
        // Desktop: the first field is ready to type into. Skipped on touch
        // screens, where focusing would throw the keyboard over the page.
        $("q").focus({ preventScroll: true });
        $("q").select(); // typing replaces the last search; arrow keys edit it
      } else if (!state.firstRoute) {
        $("newTitle").focus();
      }
    }

    state.firstRoute = false;
    renderList();
    tick();
  }

  window.addEventListener("hashchange", onRoute);

  // --------------------------------------------------- mobile sidebar drawer

  const sidebar = $("sidebar");
  const menuBtn = $("menuBtn");

  function openSidebar() {
    sidebar.classList.add("is-open");
    $("scrim").hidden = false;
    menuBtn.setAttribute("aria-expanded", "true");
    $("sidebarClose").focus();
  }

  function closeSidebar() {
    if (!sidebar.classList.contains("is-open")) return;
    sidebar.classList.remove("is-open");
    $("scrim").hidden = true;
    menuBtn.setAttribute("aria-expanded", "false");
  }

  menuBtn.addEventListener("click", openSidebar);
  $("sidebarClose").addEventListener("click", () => {
    closeSidebar();
    menuBtn.focus();
  });
  $("scrim").addEventListener("click", closeSidebar);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && sidebar.classList.contains("is-open")) {
      closeSidebar();
      menuBtn.focus();
    }
  });
  // Tapping the entry you are already on should still close the drawer.
  sidebar.addEventListener("click", (e) => {
    if (e.target.closest("a")) closeSidebar();
  });

  // ------------------------------------------------------------------ theme

  function applyTheme(theme, remember) {
    document.documentElement.dataset.theme = theme;
    const next = theme === "dark" ? "light" : "dark";
    const btn = $("themeBtn");
    btn.setAttribute("aria-label", `Switch to ${next} mode`);
    btn.title = `Switch to ${next} mode`;
    $("themeColor").setAttribute("content", theme === "dark" ? "#111113" : "#fafafa");
    if (remember) {
      try {
        localStorage.setItem("gms.theme", theme);
      } catch {
        /* private mode: the choice lasts for this visit only */
      }
    }
  }

  $("themeBtn").addEventListener("click", () => {
    applyTheme(document.documentElement.dataset.theme === "dark" ? "light" : "dark", true);
  });

  // Until someone picks a theme by hand, follow the device setting live.
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", (e) => {
    let saved = null;
    try {
      saved = localStorage.getItem("gms.theme");
    } catch {
      /* ignore */
    }
    if (!saved) applyTheme(e.matches ? "dark" : "light", false);
  });

  applyTheme(document.documentElement.dataset.theme || "light", false);

  // ------------------------------------------------------------------ start

  applyPrefs();
  onRoute();
})();
