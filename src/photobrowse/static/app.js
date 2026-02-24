(() => {
  const root = document.documentElement;

  const qs = (sel, el = document) => el.querySelector(sel);
  const qsa = (sel, el = document) => Array.from(el.querySelectorAll(sel));

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  const thumbCfg = {
    step: 10,
    mobileBreakpoint: 980,
    regular: { min: 140, max: 360, default: 200, storageKey: "pb_thumbSize" },
    compact: { min: 90, max: 220, default: 110, storageKey: "pb_thumbSize_mobile" },
  };
  let currentThumbSize = null;
  let currentThumbMode = null;

  function isCompactViewport() {
    try {
      return window.matchMedia(`(max-width: ${thumbCfg.mobileBreakpoint}px)`).matches;
    } catch (_) {
      return window.innerWidth <= thumbCfg.mobileBreakpoint;
    }
  }

  function getThumbMode() {
    return isCompactViewport() ? "compact" : "regular";
  }

  function getThumbProfile() {
    const mode = getThumbMode();
    if (currentThumbMode && currentThumbMode !== mode) currentThumbSize = null;
    currentThumbMode = mode;
    return thumbCfg[mode];
  }

  function getThumbSize() {
    if (typeof currentThumbSize === "number") return currentThumbSize;
    const profile = getThumbProfile();
    let v = profile.default;
    try {
      const saved = Number(localStorage.getItem(profile.storageKey));
      if (saved && !Number.isNaN(saved)) v = saved;
    } catch (_) {}
    v = Math.round(v / thumbCfg.step) * thumbCfg.step;
    v = clamp(v, profile.min, profile.max);
    currentThumbSize = v;
    return v;
  }

  function setThumbSize(px) {
    const profile = getThumbProfile();
    const v = clamp(Math.round(px / thumbCfg.step) * thumbCfg.step, profile.min, profile.max);
    currentThumbSize = v;
    root.style.setProperty("--thumbSize", `${v}px`);
    const label = qs("[data-zoom-label]");
    if (label) label.textContent = `${v}px`;
    try {
      localStorage.setItem(profile.storageKey, String(v));
    } catch (_) {}
    try {
      window.dispatchEvent(new CustomEvent("pb:thumbsize", { detail: { px: v } }));
    } catch (_) {
      // ignore
    }
  }

  function initZoomControls() {
    setThumbSize(getThumbSize());
    let prevMode = currentThumbMode || getThumbMode();
    window.addEventListener(
      "resize",
      () => {
        const nextMode = getThumbMode();
        if (nextMode === prevMode) return;
        prevMode = nextMode;
        currentThumbSize = null;
        setThumbSize(getThumbSize());
      },
      { passive: true },
    );
  }

  function toast(msg) {
    let el = qs("[data-toast]");
    if (!el) return;
    el.textContent = msg;
    el.classList.add("show");
    window.clearTimeout(toast._t);
    toast._t = window.setTimeout(() => el.classList.remove("show"), 1800);
  }
  toast._t = 0;

  function viewportContainRect(imgNaturalW, imgNaturalH, padding = 24) {
    const vw = window.innerWidth - padding * 2;
    const vh = window.innerHeight - padding * 2 - 64;
    const scale = Math.min(vw / imgNaturalW, vh / imgNaturalH, 1);
    const w = Math.round(imgNaturalW * scale);
    const h = Math.round(imgNaturalH * scale);
    const left = Math.round((window.innerWidth - w) / 2);
    const top = Math.round((window.innerHeight - h) / 2);
    return { left, top, width: w, height: h };
  }

  function containRectForAspect(aspect, padding = 24) {
    const vw = window.innerWidth - padding * 2;
    const vh = window.innerHeight - padding * 2 - 64;
    const a = aspect && Number.isFinite(aspect) ? clamp(aspect, 0.2, 5) : 1.5;
    let width = vw;
    let height = Math.round(width / a);
    if (height > vh) {
      height = vh;
      width = Math.round(height * a);
    }
    const left = Math.round((window.innerWidth - width) / 2);
    const top = Math.round((window.innerHeight - height) / 2);
    return { left, top, width, height };
  }

  function initViewer() {
    const viewer = qs("[data-viewer]");
    if (!viewer) return;

    const backdrop = qs("[data-viewer-backdrop]", viewer);
    const stage = qs("[data-viewer-stage]", viewer);
    const caption = qs("[data-viewer-caption]", viewer);
    const openOriginal = qs("[data-viewer-open]", viewer);
    const openFolder = qs("[data-viewer-folder]", viewer);

    const btnClose = qs("[data-viewer-close]", viewer);
    const btnZoomIn = qs("[data-viewer-zoom-in]", viewer);
    const btnZoomOut = qs("[data-viewer-zoom-out]", viewer);
    const btnReset = qs("[data-viewer-reset]", viewer);

    let currentTile = null;
    let tiles = [];
    let currentIndex = -1;
    let currentSrc = "";
    let currentCaption = "";

    let photoEl = null;
    let photoImg = null;

    let s = 1;
    let tx = 0;
    let ty = 0;
    let dragging = false;
    let dragStart = null;
    let openToken = 0;
    let animDone = false;

    function applyTransform() {
      if (!photoImg) return;
      photoImg.style.setProperty("--s", String(s));
      photoImg.style.setProperty("--tx", `${tx}px`);
      photoImg.style.setProperty("--ty", `${ty}px`);
    }

    function resetTransform() {
      s = 1;
      tx = 0;
      ty = 0;
      applyTransform();
    }

    function ensurePhotoEl() {
      if (photoEl && photoImg) return;
      photoEl = document.createElement("div");
      photoEl.className = "viewer-photo";
      photoImg = document.createElement("img");
      photoEl.appendChild(photoImg);
      viewer.appendChild(photoEl);
      // Prevent browser drag ghost.
      photoImg.addEventListener("dragstart", (e) => e.preventDefault());
    }

    function setPhotoRect(rect) {
      ensurePhotoEl();
      photoEl.style.left = `${rect.left}px`;
      photoEl.style.top = `${rect.top}px`;
      photoEl.style.width = `${rect.width}px`;
      photoEl.style.height = `${rect.height}px`;
    }

    function setCaptionAndLink(tile) {
      const title = tile.getAttribute("data-caption") || "";
      const full = tile.getAttribute("data-full") || "";
      const absPath = tile.getAttribute("data-path") || "";
      currentCaption = title;
      currentSrc = full;
      caption.textContent = title;
      openOriginal.setAttribute("href", full);
      const slash = Math.max(absPath.lastIndexOf("/"), absPath.lastIndexOf("\\"));
      const folder = slash > 0 ? absPath.slice(0, slash) : "";
      if (openFolder) {
        if (folder) {
          openFolder.setAttribute("href", `/?folder=${encodeURIComponent(folder)}`);
          openFolder.removeAttribute("aria-disabled");
          openFolder.style.pointerEvents = "";
          openFolder.style.opacity = "";
        } else {
          openFolder.setAttribute("href", "#");
          openFolder.setAttribute("aria-disabled", "true");
          openFolder.style.pointerEvents = "none";
          openFolder.style.opacity = "0.5";
        }
      }
    }

    function closeViewer({ animate = true } = {}) {
      if (!viewer.classList.contains("open")) return;
      document.body.style.overflow = "";
      viewer.classList.remove("open");

      currentTile = null;
      tiles = [];
      currentIndex = -1;
      currentSrc = "";
      currentCaption = "";
      openToken += 1;
      animDone = false;
      dragging = false;
      dragStart = null;
      if (photoImg) photoImg.classList.remove("dragging");
      if (photoEl) photoEl.remove();
      photoEl = null;
      photoImg = null;
      if (openFolder) {
        openFolder.setAttribute("href", "#");
        openFolder.setAttribute("aria-disabled", "true");
        openFolder.style.pointerEvents = "none";
        openFolder.style.opacity = "0.5";
      }
      resetTransform();
    }

    function setViewerToTile(tile, { animate = true } = {}) {
      const full = tile.getAttribute("data-full") || "";
      const title = tile.getAttribute("data-caption") || "";
      const thumb = qs("img", tile);
      if (!thumb || !full) return;

      currentTile = tile;
      currentSrc = full;
      currentCaption = title;
      currentIndex = tiles.indexOf(tile);

      setCaptionAndLink(tile);

      ensurePhotoEl();
      resetTransform();

      const thumbRect = thumb.getBoundingClientRect();
      const aspect =
        (thumb.naturalWidth && thumb.naturalHeight && thumb.naturalWidth / thumb.naturalHeight) ||
        (thumbRect.width && thumbRect.height && thumbRect.width / thumbRect.height) ||
        1.5;

      viewer.classList.add("open");
      document.body.style.overflow = "hidden";

      openToken += 1;
      const token = openToken;
      animDone = false;
      const isCurrent = () => token === openToken && currentTile === tile;

      // Start with the thumbnail (always visible), then swap to full-res once loaded.
      let thumbSrc = thumb.currentSrc || thumb.src;
      const pendingThumb = thumb.getAttribute("data-src");
      if (pendingThumb && (!thumbSrc || thumbSrc.startsWith("data:"))) {
        thumb.setAttribute("src", pendingThumb);
        thumb.removeAttribute("data-src");
        thumbSrc = pendingThumb;
      }
      photoImg.src = thumbSrc;
      photoImg.dataset.wantFull = full;
      photoImg.style.objectFit = "cover";
      photoImg.style.opacity = "1";

      // Animate the photo box into a viewport "contain" rect.
      const target = containRectForAspect(aspect, 24);
      if (animate) {
        setPhotoRect(thumbRect);

        const anim = photoEl.animate(
          [
            {
              left: `${thumbRect.left}px`,
              top: `${thumbRect.top}px`,
              width: `${thumbRect.width}px`,
              height: `${thumbRect.height}px`,
              borderRadius: "14px",
            },
            {
              left: `${target.left}px`,
              top: `${target.top}px`,
              width: `${target.width}px`,
              height: `${target.height}px`,
              borderRadius: "12px",
            },
          ],
          { duration: 260, easing: "cubic-bezier(0.2, 0.0, 0.2, 1.0)" },
        );

        anim.onfinish = () => {
          if (!isCurrent()) return;
          // Commit final rect.
          setPhotoRect(target);
          photoEl.style.borderRadius = "12px";
          photoImg.style.objectFit = "contain";
          animDone = true;
        };
      } else {
        setPhotoRect(target);
        photoEl.style.borderRadius = "12px";
        photoImg.style.objectFit = "contain";
        animDone = true;
      }

      // Load full-res in the background and "snap" swap once ready (no visible animation).
      let fullReady = false;
      let fullOk = false;
      const fullLoader = new Image();
      fullLoader.src = full;
      (async () => {
        try {
          if (typeof fullLoader.decode === "function") {
            await fullLoader.decode();
          } else {
            await new Promise((resolve) => {
              fullLoader.onload = () => resolve();
              fullLoader.onerror = () => resolve();
            });
          }
          fullReady = true;
        } catch (_) {
          fullReady = true;
        }
        fullOk = !!(fullLoader.naturalWidth && fullLoader.naturalHeight);
        // Swap only after the open animation has finished, so it doesn't look "busy".
        const maybeSwap = () => {
          if (!isCurrent()) return;
          if (!animDone) {
            window.setTimeout(maybeSwap, 30);
            return;
          }
          if (!fullReady) return;
          if (!fullOk) return;
          // Snap to full-res.
          if (!isCurrent()) return;
          if (photoImg.dataset.wantFull !== full) return;
          photoImg.src = full;
        };
        maybeSwap();
      })();
    }

    // Grid click handling.
    const grid = qs("[data-grid]");
    grid?.addEventListener("click", (e) => {
      const tile = e.target.closest("[data-tile]");
      if (!tile) return;
      e.preventDefault();
      tiles = qsa("[data-tile]", grid);
      currentIndex = tiles.indexOf(tile);
      setViewerToTile(tile, { animate: true });
    });

    // If user hits Enter while in a filter box, reset paging to the first page.
    const controlsForm = qs("form.controls");
    controlsForm?.addEventListener("submit", () => {
      const offset = qs('input[name="offset"]', controlsForm);
      if (offset) offset.value = "0";
    });

    // Trackpad pinch-to-zoom (often comes through as ctrl+wheel on macOS browsers).
    grid?.addEventListener(
      "wheel",
      (e) => {
        if (!(e.ctrlKey || e.altKey)) return;
        e.preventDefault();
        const dir = e.deltaY > 0 ? -1 : 1;
        const next = getThumbSize() + dir * thumbCfg.step;
        setThumbSize(next);
        toast("Pinch to zoom grid");
      },
      { passive: false },
    );

    // Safari gesture events.
    let gestureBase = null;
    grid?.addEventListener("gesturestart", (e) => {
      gestureBase = getThumbSize();
      e.preventDefault();
    });
    grid?.addEventListener("gesturechange", (e) => {
      if (gestureBase == null) return;
      e.preventDefault();
      const next = Math.round(gestureBase * (e.scale || 1));
      setThumbSize(next);
    });
    grid?.addEventListener("gestureend", () => {
      gestureBase = null;
    });

    // Close actions.
    backdrop?.addEventListener("click", () => closeViewer());
    btnClose?.addEventListener("click", () => closeViewer());
    window.addEventListener("keydown", (e) => {
      if (!viewer.classList.contains("open")) return;
      if (e.key === "Escape") closeViewer();
      if (e.key === "ArrowLeft") {
        e.preventDefault();
        const prev = currentIndex > 0 ? tiles[currentIndex - 1] : null;
        if (prev) {
          currentIndex -= 1;
          setViewerToTile(prev, { animate: false });
        }
      }
      if (e.key === "ArrowRight") {
        e.preventDefault();
        const next = currentIndex >= 0 && currentIndex < tiles.length - 1 ? tiles[currentIndex + 1] : null;
        if (next) {
          currentIndex += 1;
          setViewerToTile(next, { animate: false });
        }
      }
    });
    stage?.addEventListener("click", (e) => {
      // Clicking outside the image closes.
      if (e.target === stage) closeViewer();
    });

    // Zoom controls in viewer.
    function zoomAt(delta, originX, originY) {
      const prev = s;
      const next = clamp(prev * delta, 1, 6);
      if (next === prev) return;

      // Adjust translation so zoom feels anchored at pointer.
      if (!photoEl) return;
      const rect = photoEl.getBoundingClientRect();
      const cx = originX - rect.left - rect.width / 2;
      const cy = originY - rect.top - rect.height / 2;
      const factor = next / prev;
      tx = tx - cx * (factor - 1);
      ty = ty - cy * (factor - 1);
      s = next;
      applyTransform();
    }

    btnZoomIn?.addEventListener("click", () => zoomAt(1.2, window.innerWidth / 2, window.innerHeight / 2));
    btnZoomOut?.addEventListener("click", () => zoomAt(1 / 1.2, window.innerWidth / 2, window.innerHeight / 2));
    btnReset?.addEventListener("click", () => resetTransform());

    viewer?.addEventListener(
      "wheel",
      (e) => {
        if (!viewer.classList.contains("open")) return;
        if (photoEl && e.target instanceof Node && !photoEl.contains(e.target) && e.target !== stage) return;
        e.preventDefault();
        const delta = e.deltaY > 0 ? 1 / 1.12 : 1.12;
        zoomAt(delta, e.clientX, e.clientY);
        if (s > 1) toast("Scroll to zoom, drag to pan, Esc to close");
      },
      { passive: false },
    );

    // Pan.
    const beginPan = (e) => {
      if (!viewer.classList.contains("open")) return;
      if (s <= 1) return;
      if (!photoEl) return;
      if (!(e.target instanceof Node)) return;
      if (!photoEl.contains(e.target)) return;
      e.preventDefault();
      dragging = true;
      if (photoImg) photoImg.classList.add("dragging");
      try {
        viewer.setPointerCapture(e.pointerId);
      } catch (_) {
        // ignore
      }
      dragStart = { x: e.clientX, y: e.clientY, tx, ty };
    };
    const movePan = (e) => {
      if (!dragging || !dragStart) return;
      tx = dragStart.tx + (e.clientX - dragStart.x);
      ty = dragStart.ty + (e.clientY - dragStart.y);
      applyTransform();
    };
    const endPan = () => {
      dragging = false;
      if (photoImg) photoImg.classList.remove("dragging");
      dragStart = null;
    };
    viewer?.addEventListener("pointerdown", beginPan);
    window.addEventListener("pointermove", movePan);
    window.addEventListener("pointerup", endPan);
    window.addEventListener("pointercancel", endPan);

    // Double click to zoom.
    viewer?.addEventListener("dblclick", (e) => {
      if (!viewer.classList.contains("open")) return;
      if (photoEl && e.target instanceof Node && !photoEl.contains(e.target)) return;
      if (s <= 1) zoomAt(2.2, e.clientX, e.clientY);
      else resetTransform();
    });
  }

  window.addEventListener("DOMContentLoaded", () => {
    initZoomControls();
    initRelevantSections();
    const thumbCtl = initThumbLoading();
    const statusCtl = initStatus();
    const rootsCtl = initRootsLive();
    const searchForm = qs("form.searchbar");
    searchForm?.addEventListener("submit", () => {
      // Free up the browser's per-origin connection pool so the next navigation isn't
      // blocked behind thumbnail/SSE requests.
      thumbCtl?.cancel?.();
      statusCtl?.shutdown?.();
      rootsCtl?.shutdown?.();
    });
    window.addEventListener(
      "pagehide",
      () => {
        thumbCtl?.cancel?.();
        statusCtl?.shutdown?.();
        rootsCtl?.shutdown?.();
      },
      { once: true },
    );
    initViewer();
  });

  function initRelevantSections() {
    const mostSection = qs("[data-most-section]");
    const moreSection = qs("[data-more-section]");
    if (!mostSection || !moreSection) return;
    const mostGrid = qs("[data-most-grid]", mostSection);
    const moreGrid = qs("[data-more-grid]", moreSection);
    if (!mostGrid || !moreGrid) return;

    const tilesOrdered = [...qsa("[data-tile]", mostGrid), ...qsa("[data-tile]", moreGrid)];
    if (!tilesOrdered.length) return;

    const mostPill = qs("[data-most-pill]", mostSection);
    const morePill = qs("[data-more-pill]", moreSection);

    const measureColumns = () => {
      const tiles = qsa("[data-tile]", mostGrid);
      if (!tiles.length) return 1;
      const top0 = tiles[0].offsetTop;
      let cols = 0;
      for (const t of tiles) {
        if (Math.abs(t.offsetTop - top0) > 2) break;
        cols += 1;
      }
      return Math.max(1, cols);
    };

    const assign = (mostCount) => {
      const n = Math.max(0, Math.min(tilesOrdered.length, mostCount));
      for (let i = 0; i < tilesOrdered.length; i++) {
        const tile = tilesOrdered[i];
        if (i < n) mostGrid.appendChild(tile);
        else moreGrid.appendChild(tile);
      }
      const moreCount = tilesOrdered.length - n;
      moreSection.hidden = moreCount <= 0;
      if (mostPill) mostPill.textContent = `Top ${n}`;
      if (morePill) morePill.textContent = moreCount > 0 ? `${moreCount} more` : "";
    };

    const reflow = () => {
      // Seed enough tiles into the first grid so we can measure columns reliably.
      const seed = Math.min(tilesOrdered.length, 64);
      for (let i = 0; i < tilesOrdered.length; i++) {
        const tile = tilesOrdered[i];
        if (i < seed) mostGrid.appendChild(tile);
        else moreGrid.appendChild(tile);
      }
      const cols = measureColumns();
      // User request: cap "Most relevant" to two rows.
      const desired = Math.min(tilesOrdered.length, cols * 2);
      assign(desired);
    };

    let raf1 = 0;
    let raf2 = 0;
    const schedule = () => {
      window.cancelAnimationFrame(raf1);
      window.cancelAnimationFrame(raf2);
      raf1 = window.requestAnimationFrame(() => {
        raf2 = window.requestAnimationFrame(reflow);
      });
    };

    schedule();
    window.addEventListener("resize", schedule, { passive: true });
    window.addEventListener("pb:thumbsize", schedule);
  }

  function initThumbLoading() {
    const imgs = qsa('img[data-src]');
    if (!imgs.length) return;

    const placeholderSrc = "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==";

    const loadOne = (img) => {
      const src = img.getAttribute('data-src');
      if (!src) return;
      img.setAttribute('src', src);
      img.removeAttribute('data-src');
    };

    // No IntersectionObserver: just load everything (browser will still apply its own limits).
    if (typeof window.IntersectionObserver !== "function") {
      imgs.forEach(loadOne);
      return { cancel: () => {} };
    }

    // Prefetch a few rows ahead/behind the viewport, but throttle actual loading
    // so we don't kick off hundreds of image fetches/decodes at once.
    const gapPx = 10;
    const prefetchRows = 2;
    const getPrefetchPx = () => {
      let thumbSizePx = 200;
      try {
        const v = getComputedStyle(root).getPropertyValue("--thumbSize");
        const n = parseInt(String(v).trim().replace("px", ""), 10);
        if (Number.isFinite(n) && n > 0) thumbSizePx = n;
      } catch (_) {}
      return Math.max(400, (thumbSizePx + gapPx) * prefetchRows);
    };

    // Keep this low: browsers limit concurrent connections per origin (often ~6),
    // and the status SSE stream also uses one.
    const maxInflight = 3;
    let inflight = 0;
    const queue = [];
    const queued = new Set();
    const pending = new Set();
    const inFlightSrc = new WeakMap();
    let canceled = false;

    const isNearViewport = (img) => {
      const prefetchPx = getPrefetchPx();
      const r = img.getBoundingClientRect();
      const topOk = r.bottom >= -prefetchPx;
      const botOk = r.top <= window.innerHeight + prefetchPx;
      return topOk && botOk;
    };

    const pruneQueue = () => {
      const prefetchPx = getPrefetchPx();
      const keepPx = prefetchPx * 2;
      const maxQ = 500;
      if (queue.length <= maxQ) return;
      const kept = [];
      for (const img of queue) {
        if (!img || !img.isConnected) continue;
        const r = img.getBoundingClientRect();
        const topOk = r.bottom >= -keepPx;
        const botOk = r.top <= window.innerHeight + keepPx;
        if (topOk && botOk) kept.push(img);
      }
      queue.length = 0;
      queue.push(...kept);
    };

    const pump = () => {
      if (canceled) return;
      pruneQueue();
      while (inflight < maxInflight && queue.length) {
        let img = null;
        // Prefer items near the viewport; avoid loading far-off thumbnails just because
        // the user scrolled quickly at some point.
        for (let i = 0; i < queue.length; i++) {
          const cand = queue[i];
          if (!cand || !cand.isConnected) continue;
          if (!isNearViewport(cand)) continue;
          img = cand;
          queue.splice(i, 1);
          break;
        }
        if (!img) break;
        if (!img || !img.isConnected) continue;
        const src = img.getAttribute("data-src");
        if (!src) continue;
        inFlightSrc.set(img, src);
        inflight += 1;
        pending.add(img);
        img.setAttribute("src", src);
        img.removeAttribute("data-src");
        const done = () => {
          if (!pending.has(img)) return;
          pending.delete(img);
          inflight = Math.max(0, inflight - 1);
          pump();
        };
        // Use once listeners so repeated events don't double-decrement.
        img.addEventListener("load", done, { once: true });
        img.addEventListener("error", done, { once: true });
      }
    };

    const io = new IntersectionObserver(
      (entries) => {
        for (const ent of entries) {
          if (!ent.isIntersecting) continue;
          const img = ent.target;
          if (!queued.has(img)) {
            queued.add(img);
            queue.push(img);
          }
          io.unobserve(img);
        }
        pump();
      },
      { root: null, rootMargin: `${getPrefetchPx()}px 0px ${getPrefetchPx()}px 0px`, threshold: 0.01 },
    );

    imgs.forEach((img) => io.observe(img));
    window.addEventListener("scroll", pump, { passive: true });
    window.addEventListener("resize", pump);
    return {
      cancel: () => {
        canceled = true;
        try {
          io.disconnect();
        } catch (_) {}
        window.removeEventListener("scroll", pump);
        window.removeEventListener("resize", pump);
        queue.length = 0;
        queued.clear();
        // Cancel in-flight image requests by swapping to a tiny placeholder.
        for (const img of Array.from(pending)) {
          try {
            const src = inFlightSrc.get(img);
            if (src) img.setAttribute("data-src", src);
            img.setAttribute("src", placeholderSrc);
          } catch (_) {}
        }
        pending.clear();
        inflight = 0;
      },
    };
  }

  function initStatus() {
    const el = qs("[data-status]");
    if (!el) return { shutdown: () => {} };
    const text = qs("[data-status-text]", el);
    const spinner = qs("[data-status-spinner]", el);
    const dot = qs("[data-status-dot]", el);
    const diagActivity = qs("[data-diag-activity]");
    const diagKpis = qs("[data-diag-kpis]");

    let lastStatusAt = 0;
    let stopped = false;
    let es = null;
    let stallInterval = null;
    let pollInterval = null;
    let pollTimeout = null;
    let pollAbort = null;

    const shutdown = () => {
      if (stopped) return;
      stopped = true;
      try {
        es?.close?.();
      } catch (_) {}
      es = null;
      if (stallInterval) window.clearInterval(stallInterval);
      if (pollInterval) window.clearInterval(pollInterval);
      if (pollTimeout) window.clearTimeout(pollTimeout);
      stallInterval = null;
      pollInterval = null;
      pollTimeout = null;
      try {
        pollAbort?.abort?.();
      } catch (_) {}
      pollAbort = null;
    };

    function applyStatus(s) {
      if (stopped) return;
      if (s && typeof s === "object" && s.error) {
        dot?.classList.toggle("live", false);
        if (text) text.textContent = "Status error";
        spinner?.classList.remove("on");
        return;
      }
      lastStatusAt = Date.now();
      const busy =
        (s.scan_queue_size || 0) > 0 ||
        (s.ingest_queue_size || 0) > 0 ||
        !!s.active_scan_root_id ||
        !!s.active_ingest_path;

      spinner?.classList.toggle("on", busy);
      dot?.classList.toggle("live", true);
      el.classList.toggle("busy", busy);
      const failedTotal = Number(s.failed_total || 0);
      const hasFailure = failedTotal > 0 && !!(s.last_failed_path || s.last_failed_error);
      const nowTs = Number(s.now || 0);
      const lastWaveEnded = Number(s.last_scan_wave_ended_at || 0);
      const lastWaveFound = Number(s.last_scan_wave_found || 0);
      const lastWaveRoots = Math.max(1, Number(s.last_scan_wave_roots || 0));
      const lastWaveHadErrors = !!s.last_scan_wave_had_errors;
      const lastWaveAgeSec = lastWaveEnded > 0 && nowTs > 0 ? Math.max(0, Math.round(nowTs - lastWaveEnded)) : null;
      const showSummary = !busy && !hasFailure && lastWaveEnded > 0 && (lastWaveAgeSec == null || lastWaveAgeSec <= 180);
      el.classList.toggle("has-failure", hasFailure);
      el.classList.toggle("has-summary", showSummary);

      const scan = s.active_scan_root_path ? `Scanning ${s.active_scan_root_path}` : `Scan queue ${s.scan_queue_size}`;
      const indexing = s.active_ingest_path
        ? `Indexing ${s.active_ingest_path.split("/").slice(-2).join("/")}`
        : `Index queue ${s.ingest_queue_size}`;
      const titleParts = [
        `${s.photos_total} photos`,
        `${s.photos_indexed || 0} indexed`,
        hasFailure ? `${failedTotal} failed` : "",
        `${s.roots_online}/${s.roots_total} roots online`,
        busy ? scan : "Idle",
        busy ? indexing : "",
        showSummary ? `Last scan: ${lastWaveFound} files across ${lastWaveRoots} root${lastWaveRoots === 1 ? "" : "s"}` : "",
        showSummary && lastWaveAgeSec != null ? `${lastWaveAgeSec}s ago` : "",
        showSummary && lastWaveHadErrors ? "with warnings" : "",
        hasFailure ? (s.last_failed_error || "Last failure recorded") : "",
      ].filter(Boolean);
      el.setAttribute("title", titleParts.join(" • "));
      if (text) {
        const denom = Math.max(0, Number(s.active_scan_enqueued || 0));
        const num = Math.max(0, Number(s.active_scan_processed || 0));
        const pct = denom > 0 ? Math.min(100, Math.round((num / denom) * 100)) : null;
        if (busy) {
          text.textContent = pct != null ? `Indexing ${pct}%` : "Indexing…";
        } else if (hasFailure) {
          text.textContent = "Failure (Diagnostics)";
        } else if (showSummary) {
          text.textContent = `Scan complete (${lastWaveFound} / ${lastWaveRoots} root${lastWaveRoots === 1 ? "" : "s"})`;
        } else {
          text.textContent = "";
        }
      }

      if (diagActivity) {
        const stateEl = qs("[data-diag-state]", diagActivity);
        const rootEl = qs("[data-diag-root]", diagActivity);
        const curEl = qs("[data-diag-current]", diagActivity);
        const photosTotalEl = qs("[data-diag-photos-total]", diagActivity);
        const photosIndexedEl = qs("[data-diag-photos-indexed]", diagActivity);

        const scanBlock = qs("[data-diag-scan]", diagActivity);
        const foundEl = qs("[data-diag-found]", diagActivity);

        const progress = qs("[data-diag-progress]", diagActivity);
        const enqEl = qs("[data-diag-enqueued]", diagActivity);
        const procEl = qs("[data-diag-processed]", diagActivity);
        const pctEl = qs("[data-diag-pct]", diagActivity);

        const failedPill = qs("[data-diag-failed]", diagActivity);
        const failedCountEl = qs("[data-diag-failed-count]", diagActivity);
        const lastFailed = qs("[data-diag-last-failed]", diagActivity);
        const lastFailedPath = qs("[data-diag-last-failed-path]", diagActivity);
        const lastFailedErr = qs("[data-diag-last-failed-error]", diagActivity);

        const scanning = !!s.active_scan_root_id;
        const indexingNow = !!s.active_ingest_path || (s.ingest_queue_size || 0) > 0;
        const state = scanning && indexingNow
          ? "Scanning + indexing"
          : scanning
            ? "Scanning"
            : indexingNow
              ? "Indexing"
              : showSummary
                ? `Idle • last scan ${lastWaveFound} file${lastWaveFound === 1 ? "" : "s"} across ${lastWaveRoots} root${lastWaveRoots === 1 ? "" : "s"}`
                : "Idle";
        if (stateEl) stateEl.textContent = state;

        if (rootEl) rootEl.textContent = s.active_scan_root_path || s.last_scan_root_path || "—";
        if (curEl) curEl.textContent = s.active_ingest_path || "—";
        if (photosTotalEl) photosTotalEl.textContent = String(Number(s.photos_total || 0));
        if (photosIndexedEl) photosIndexedEl.textContent = String(Number(s.photos_indexed || 0));

        if (scanBlock) scanBlock.hidden = !scanning;
        if (foundEl) foundEl.textContent = String(s.active_scan_found || 0);

        const denom = Math.max(0, Number(s.active_scan_enqueued || 0));
        const num = Math.max(0, Number(s.active_scan_processed || 0));
        if (enqEl) enqEl.textContent = String(denom);
        if (procEl) procEl.textContent = String(num);

        const pct = denom > 0 ? Math.min(100, Math.round((num / denom) * 100)) : null;
        if (pctEl) pctEl.textContent = pct != null ? `${pct}%` : "—";
        if (progress) {
          const max = Math.max(1, denom);
          progress.max = max;
          progress.value = Math.max(0, Math.min(num, max));
        }

        if (failedPill) failedPill.hidden = failedTotal <= 0;
        if (failedCountEl) failedCountEl.textContent = String(failedTotal);

        const lastPath = s.last_failed_path || "";
        const lastErr = s.last_failed_error || "";
        const showLast = !!(lastPath || lastErr);
        if (lastFailed) lastFailed.hidden = !showLast;
        if (lastFailedPath) lastFailedPath.textContent = lastPath || "—";
        if (lastFailedErr) lastFailedErr.textContent = lastErr || "—";
      }

      if (diagKpis) {
        const kPhotos = qs("[data-kpi-photos]", diagKpis);
        const kIndexed = qs("[data-kpi-indexed]", diagKpis);
        const kFailed = qs("[data-kpi-failed]", diagKpis);
        if (kPhotos) kPhotos.textContent = String(Number(s.photos_total || 0));
        if (kIndexed) kIndexed.textContent = String(Number(s.photos_indexed || 0));
        if (kFailed) kFailed.textContent = String(Number(s.failed_total || 0));
      }
      try {
        window.dispatchEvent(new CustomEvent("pb:status", { detail: { status: s } }));
      } catch (_) {
        // ignore
      }
    }

    async function pollFallback() {
      if (stopped) return;
      try {
        pollAbort?.abort?.();
        pollAbort = new AbortController();
        const r = await fetch("/api/status", { cache: "no-store", signal: pollAbort.signal });
        if (!r.ok) throw new Error("bad status");
        const s = await r.json();
        applyStatus(s);
      } catch (_) {
        dot?.classList.toggle("live", false);
        if (text) text.textContent = "Status unavailable";
        spinner?.classList.remove("on");
      }
    }

    if (typeof window.EventSource === "function") {
      es = new EventSource("/api/status/stream");
      es.onmessage = (ev) => {
        if (stopped) return;
        try {
          const s = JSON.parse(ev.data);
          applyStatus(s);
        } catch (_) {
          // ignore
        }
      };
      es.onerror = () => {
        if (stopped) return;
        // EventSource auto-reconnects; show degraded state briefly.
        dot?.classList.toggle("live", false);
        if (text) text.textContent = "Reconnecting…";
        // Keep UI alive even if SSE is flaky.
        pollTimeout = window.setTimeout(pollFallback, 800);
      };

      // Safety net: if SSE stalls, keep updating via polling.
      stallInterval = window.setInterval(() => {
        const age = Date.now() - lastStatusAt;
        if (age > 6500) pollFallback();
      }, 3000);
    } else {
      pollFallback();
      pollInterval = window.setInterval(pollFallback, 4000);
    }

    return { shutdown };
  }

  function initRootsLive() {
    const table = qs("[data-roots-table]");
    if (!table) return { shutdown: () => {} };

    let stopped = false;
    let fallbackTimer = null;
    let inFlight = null;
    let lastFetchAt = 0;
    let pendingTimer = null;
    let activeRootId = null;
    const transientUntilByRoot = new Map();
    let transientTimer = null;

    const setText = (row, sel, value) => {
      const el = qs(sel, row);
      if (!el) return;
      el.textContent = value == null ? "" : String(value);
    };

    const applyActiveState = () => {
      const now = Date.now();
      let nextWake = null;
      const rows = qsa("[data-root-row]", table);
      for (const row of rows) {
        const rid = Number(row.getAttribute("data-root-row"));
        const enumEl = qs("[data-root-scan-enum]", row);
        const finEl = qs("[data-root-scan-finished]", row);
        const until = Number(transientUntilByRoot.get(rid) || 0);
        const transient = until > now;
        if (transient) {
          if (nextWake == null || until < nextWake) nextWake = until;
        }
        if (rid === Number(activeRootId || 0) || transient) {
          if (enumEl) enumEl.textContent = "Scanning…";
          if (finEl) finEl.textContent = "In progress…";
          row.classList.add("scan-active");
          continue;
        }
        row.classList.remove("scan-active");
        if (enumEl) enumEl.textContent = row.getAttribute("data-last-scan-enum") || "";
        if (finEl) finEl.textContent = row.getAttribute("data-last-scan-finished") || "";
      }
      if (transientTimer) window.clearTimeout(transientTimer);
      transientTimer = null;
      if (nextWake != null) {
        transientTimer = window.setTimeout(() => {
          transientTimer = null;
          applyActiveState();
        }, Math.max(40, nextWake - Date.now() + 20));
      }
    };

    const applyRoots = (roots) => {
      if (!Array.isArray(roots)) return;
      for (const r of roots) {
        const row = qs(`[data-root-row="${Number(r.id)}"]`, table);
        if (!row) continue;
        setText(row, "[data-root-status]", r.status);
        setText(row, "[data-root-last-seen]", r.last_seen_at);
        const hadStartedAttr = row.hasAttribute("data-last-scan-started");
        const prevStarted = row.getAttribute("data-last-scan-started") || "";
        const nextStarted = r.last_scan_started_at == null ? "" : String(r.last_scan_started_at);
        if (hadStartedAttr && nextStarted && nextStarted !== prevStarted) {
          transientUntilByRoot.set(Number(r.id), Date.now() + 1400);
        }
        row.setAttribute("data-last-scan-started", nextStarted);
        row.setAttribute("data-last-scan-enum", r.last_scan_enumerated_at == null ? "" : String(r.last_scan_enumerated_at));
        row.setAttribute(
          "data-last-scan-finished",
          r.last_scan_finished_at == null ? "" : String(r.last_scan_finished_at),
        );
        setText(row, "[data-root-last-error]", r.last_error);
      }
      applyActiveState();
    };

    const poll = async () => {
      if (stopped) return;
      try {
        inFlight?.abort?.();
      } catch (_) {}
      inFlight = new AbortController();
      lastFetchAt = Date.now();
      try {
        const res = await fetch("/api/roots", { cache: "no-store", signal: inFlight.signal });
        if (!res.ok) throw new Error("bad roots status");
        const payload = await res.json();
        applyRoots(payload.roots || []);
      } catch (_) {
        // Keep silent; next poll will retry.
      }
    };

    const schedulePoll = (delayMs = 0) => {
      if (stopped) return;
      if (pendingTimer) return;
      pendingTimer = window.setTimeout(async () => {
        pendingTimer = null;
        await poll();
      }, Math.max(0, delayMs));
    };

    const onStatus = (ev) => {
      if (stopped) return;
      const s = ev?.detail?.status || {};
      const busy =
        (s.scan_queue_size || 0) > 0 ||
        (s.ingest_queue_size || 0) > 0 ||
        !!s.active_scan_root_id ||
        !!s.active_ingest_path;
      const nextActiveRoot = s.active_scan_root_id ? Number(s.active_scan_root_id) : null;
      const changedRoot = nextActiveRoot !== activeRootId;
      activeRootId = nextActiveRoot;
      applyActiveState();
      const age = Date.now() - lastFetchAt;
      if (busy || age > 6000 || changedRoot) {
        schedulePoll(0);
      }
    };

    poll();
    window.addEventListener("pb:status", onStatus);
    // Fallback in case status stream disconnects.
    fallbackTimer = window.setInterval(() => {
      const age = Date.now() - lastFetchAt;
      if (age > 10000) schedulePoll(0);
    }, 5000);

    return {
      shutdown: () => {
        if (stopped) return;
        stopped = true;
        window.removeEventListener("pb:status", onStatus);
        if (fallbackTimer) window.clearInterval(fallbackTimer);
        fallbackTimer = null;
        if (pendingTimer) window.clearTimeout(pendingTimer);
        pendingTimer = null;
        if (transientTimer) window.clearTimeout(transientTimer);
        transientTimer = null;
        try {
          inFlight?.abort?.();
        } catch (_) {}
        inFlight = null;
      },
    };
  }
})();
