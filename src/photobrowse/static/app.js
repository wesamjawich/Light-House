(() => {
  const root = document.documentElement;

  const qs = (sel, el = document) => el.querySelector(sel);
  const qsa = (sel, el = document) => Array.from(el.querySelectorAll(sel));

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  const thumbCfg = { min: 140, max: 360, step: 10, default: 200 };
  let currentThumbSize = null;

  function getThumbSize() {
    if (typeof currentThumbSize === "number") return currentThumbSize;
    let v = thumbCfg.default;
    try {
      const saved = Number(localStorage.getItem("pb_thumbSize"));
      if (saved && !Number.isNaN(saved)) v = saved;
    } catch (_) {}
    v = Math.round(v / thumbCfg.step) * thumbCfg.step;
    v = clamp(v, thumbCfg.min, thumbCfg.max);
    currentThumbSize = v;
    return v;
  }

  function setThumbSize(px) {
    const v = clamp(Math.round(px / thumbCfg.step) * thumbCfg.step, thumbCfg.min, thumbCfg.max);
    currentThumbSize = v;
    root.style.setProperty("--thumbSize", `${v}px`);
    const label = qs("[data-zoom-label]");
    if (label) label.textContent = `${v}px`;
    try {
      localStorage.setItem("pb_thumbSize", String(v));
    } catch (_) {}
  }

  function initZoomControls() {
    setThumbSize(getThumbSize());
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
      currentCaption = title;
      currentSrc = full;
      caption.textContent = title;
      openOriginal.setAttribute("href", full);
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
      if (photoEl) photoEl.remove();
      photoEl = null;
      photoImg = null;
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

      caption.textContent = title;
      openOriginal.setAttribute("href", full);

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
      const thumbSrc = thumb.currentSrc || thumb.src;
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

    stage?.addEventListener(
      "wheel",
      (e) => {
        if (!viewer.classList.contains("open")) return;
        e.preventDefault();
        const delta = e.deltaY > 0 ? 1 / 1.12 : 1.12;
        zoomAt(delta, e.clientX, e.clientY);
        if (s > 1) toast("Scroll to zoom, drag to pan, Esc to close");
      },
      { passive: false },
    );

    // Pan.
    stage?.addEventListener("pointerdown", (e) => {
      if (s <= 1) return;
      dragging = true;
      if (photoImg) photoImg.classList.add("dragging");
      stage.setPointerCapture(e.pointerId);
      dragStart = { x: e.clientX, y: e.clientY, tx, ty };
    });
    stage?.addEventListener("pointermove", (e) => {
      if (!dragging || !dragStart) return;
      tx = dragStart.tx + (e.clientX - dragStart.x);
      ty = dragStart.ty + (e.clientY - dragStart.y);
      applyTransform();
    });
    stage?.addEventListener("pointerup", () => {
      dragging = false;
      if (photoImg) photoImg.classList.remove("dragging");
      dragStart = null;
    });
    stage?.addEventListener("pointercancel", () => {
      dragging = false;
      if (photoImg) photoImg.classList.remove("dragging");
      dragStart = null;
    });

    // Double click to zoom.
    stage?.addEventListener("dblclick", (e) => {
      if (!viewer.classList.contains("open")) return;
      if (s <= 1) zoomAt(2.2, e.clientX, e.clientY);
      else resetTransform();
    });
  }

  window.addEventListener("DOMContentLoaded", () => {
    initZoomControls();
    initViewer();
    initStatus();
  });

  function initStatus() {
    const el = qs("[data-status]");
    if (!el) return;
    const text = qs("[data-status-text]", el);
    const spinner = qs("[data-status-spinner]", el);
    const dot = qs("[data-status-dot]", el);
    const diagActivity = qs("[data-diag-activity]");
    const diagKpis = qs("[data-diag-kpis]");

    let lastStatusAt = 0;

    function applyStatus(s) {
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

      const scan = s.active_scan_root_path ? `Scanning ${s.active_scan_root_path}` : `Scan queue ${s.scan_queue_size}`;
      const indexing = s.active_ingest_path
        ? `Indexing ${s.active_ingest_path.split("/").slice(-2).join("/")}`
        : `Index queue ${s.ingest_queue_size}`;
      const titleParts = [
        `${s.photos_total} photos`,
        `${s.photos_indexed || 0} indexed`,
        `${s.roots_online}/${s.roots_total} roots online`,
        busy ? scan : "Idle",
        busy ? indexing : "",
      ].filter(Boolean);
      el.setAttribute("title", titleParts.join(" • "));
      if (text) {
        const denom = Math.max(0, Number(s.active_scan_enqueued || 0));
        const num = Math.max(0, Number(s.active_scan_processed || 0));
        const pct = denom > 0 ? Math.min(100, Math.round((num / denom) * 100)) : null;
        text.textContent = busy ? (pct != null ? `Indexing ${pct}%` : "Indexing…") : "";
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
        const state = scanning && indexingNow ? "Scanning + indexing" : scanning ? "Scanning" : indexingNow ? "Indexing" : "Idle";
        if (stateEl) stateEl.textContent = state;

        if (rootEl) rootEl.textContent = s.active_scan_root_path || "—";
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

        const failedTotal = Number(s.failed_total || 0);
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
    }

    async function pollFallback() {
      try {
        const r = await fetch("/api/status", { cache: "no-store" });
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
      const es = new EventSource("/api/status/stream");
      es.onmessage = (ev) => {
        try {
          const s = JSON.parse(ev.data);
          applyStatus(s);
        } catch (_) {
          // ignore
        }
      };
      es.onerror = () => {
        // EventSource auto-reconnects; show degraded state briefly.
        dot?.classList.toggle("live", false);
        if (text) text.textContent = "Reconnecting…";
        // Keep UI alive even if SSE is flaky.
        window.setTimeout(pollFallback, 800);
      };

      // Safety net: if SSE stalls, keep updating via polling.
      window.setInterval(() => {
        const age = Date.now() - lastStatusAt;
        if (age > 6500) pollFallback();
      }, 3000);
    } else {
      pollFallback();
      window.setInterval(pollFallback, 4000);
    }
  }
})();
