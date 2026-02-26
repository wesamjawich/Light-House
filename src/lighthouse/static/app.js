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

  function getViewerTopInset() {
    const topbar = qs(".viewer-topbar");
    if (topbar) {
      const h = Math.ceil(topbar.getBoundingClientRect().height || 0);
      if (h > 0) return h;
    }
    return isCompactViewport() ? 92 : 64;
  }

  function getViewerImagePadding() {
    return isCompactViewport() ? 0 : 24;
  }

  function getViewerViewportHeight() {
    const innerH = Math.max(1, Math.round(window.innerHeight || 0));
    const vv = window.visualViewport;
    if (vv && Number.isFinite(vv.height) && vv.height > 0) {
      const offsetTop = Number.isFinite(vv.offsetTop) ? vv.offsetTop : 0;
      const visualH = Math.max(1, Math.round(vv.height + offsetTop));
      // iOS can report a visual viewport shorter than the composited page during
      // toolbar transitions; never shrink below innerHeight or you'll expose the page.
      return Math.max(innerH, visualH);
    }
    return innerH;
  }

  function syncViewerViewportMetrics() {
    const inset = getViewerTopInset();
    root.style.setProperty("--viewerTopInset", `${inset}px`);
    root.style.setProperty("--viewerVh", `${getViewerViewportHeight()}px`);
  }

  function containRectForAspect(aspect, padding = null) {
    const pad = padding == null ? getViewerImagePadding() : padding;
    const topInset = getViewerTopInset();
    const vw = Math.max(1, window.innerWidth - pad * 2);
    const vh = Math.max(1, getViewerViewportHeight() - pad * 2 - topInset);
    const a = aspect && Number.isFinite(aspect) ? clamp(aspect, 0.2, 5) : 1.5;
    let width = vw;
    let height = Math.round(width / a);
    // On compact/mobile viewports keep media full-width and center-crop vertically
    // when needed (matching native gallery behavior).
    if (!isCompactViewport() && height > vh) {
      height = vh;
      width = Math.round(height * a);
    }
    const left = Math.round((window.innerWidth - width) / 2);
    const top = Math.round(topInset + pad + (vh - height) / 2);
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

    let currentTile = null;
    let tiles = [];
    let currentIndex = -1;
    const knownAspectByFull = new Map();

    let photoEl = null;
    let photoImg = null;

    let s = 1;
    let tx = 0;
    let ty = 0;
    let dragging = false;
    let dragStart = null;
    const touchPoints = new Map();
    let pinching = false;
    let pinchBaseDistance = 0;
    let pinchBaseScale = 1;
    let swipeDrag = null;
    let swipePeerEl = null;
    let swipePeerImg = null;
    let swipePeerDir = 0;
    let suppressStageClickUntil = 0;
    let openToken = 0;
    let animDone = false;

    const swipeAxisLockPx = 12;
    const swipeCommitPx = 64;
    const swipeFlickPxPerMs = 0.45;

    function getPanLimits(forScale = s) {
      if (!photoEl || !stage) return { x: 0, y: 0 };
      const scale = Number.isFinite(forScale) ? Math.max(1, forScale) : 1;
      const baseW = photoEl.clientWidth || 0;
      const baseH = photoEl.clientHeight || 0;
      const stageRect = stage.getBoundingClientRect();
      if (!(baseW > 0 && baseH > 0 && stageRect.width > 0 && stageRect.height > 0)) {
        return { x: 0, y: 0 };
      }
      const scaledW = baseW * scale;
      const scaledH = baseH * scale;
      return {
        x: Math.max(0, (scaledW - stageRect.width) / 2),
        y: Math.max(0, (scaledH - stageRect.height) / 2),
      };
    }

    function clampPan(forScale = s) {
      const limits = getPanLimits(forScale);
      tx = clamp(tx, -limits.x, limits.x);
      ty = clamp(ty, -limits.y, limits.y);
    }

    function applyTransform() {
      if (!photoImg) return;
      if (s <= 1.001) {
        tx = 0;
        ty = 0;
      } else {
        clampPan(s);
      }
      photoImg.style.setProperty("--s", String(s));
      photoImg.style.setProperty("--tx", `${tx}px`);
      photoImg.style.setProperty("--ty", `${ty}px`);
      if (photoEl) photoEl.classList.toggle("zoomed", s > 1.001);
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
      photoEl.style.transition = "";
      photoEl.style.transform = "";
      photoEl.style.left = `${rect.left}px`;
      photoEl.style.top = `${rect.top}px`;
      photoEl.style.width = `${rect.width}px`;
      photoEl.style.height = `${rect.height}px`;
    }

    function clearSwipePeer() {
      if (swipePeerEl) swipePeerEl.remove();
      swipePeerEl = null;
      swipePeerImg = null;
      swipePeerDir = 0;
    }

    function clearSwipeVisualState() {
      swipeDrag = null;
      if (photoEl) {
        photoEl.style.transition = "";
        photoEl.style.transform = "";
      }
      if (swipePeerEl) {
        swipePeerEl.style.transition = "";
        swipePeerEl.style.transform = "";
      }
      clearSwipePeer();
    }

    function setCaptionAndLink(tile) {
      const title = tile.getAttribute("data-caption") || "";
      const full = tile.getAttribute("data-full") || "";
      const absPath = tile.getAttribute("data-path") || "";
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

    function getTileAspect(tile, thumb = null, thumbRect = null, fallbackAspect = 1.5) {
      const full = tile?.getAttribute("data-full") || "";
      const knownAspect = Number(knownAspectByFull.get(full));
      const tileAspectRaw = Number(tile?.getAttribute("data-aspect") || "");
      const tileAspect = Number.isFinite(tileAspectRaw) && tileAspectRaw > 0 ? tileAspectRaw : 0;
      const rect = thumbRect || (thumb ? thumb.getBoundingClientRect() : null);
      const thumbAspect =
        thumb && thumb.naturalWidth && thumb.naturalHeight
          ? thumb.naturalWidth / thumb.naturalHeight
          : rect && rect.width > 0 && rect.height > 0
            ? rect.width / rect.height
            : 0;
      const fallback = Number.isFinite(fallbackAspect) && fallbackAspect > 0 ? fallbackAspect : 1.5;
      return (Number.isFinite(knownAspect) && knownAspect > 0 && knownAspect) || tileAspect || thumbAspect || fallback;
    }

    function beginFullSwap(full, isCurrent) {
      if (!full) return;
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
        if (fullOk) {
          const fullAspect = fullLoader.naturalWidth / Math.max(1, fullLoader.naturalHeight);
          if (Number.isFinite(fullAspect) && fullAspect > 0) knownAspectByFull.set(full, fullAspect);
        }
        const maybeSwap = () => {
          if (!isCurrent()) return;
          if (!animDone) {
            window.setTimeout(maybeSwap, 30);
            return;
          }
          if (!fullReady || !fullOk) return;
          if (!isCurrent()) return;
          if (!photoEl || !photoImg) return;
          if (photoImg.dataset.wantFull !== full) return;

          // Re-fit using true full-res aspect before swapping.
          if (!swipeDrag && !pinching && s <= 1.001) {
            const fullAspect = Number(knownAspectByFull.get(full)) || (fullLoader.naturalWidth / Math.max(1, fullLoader.naturalHeight));
            const fullTarget = containRectForAspect(fullAspect);
            setPhotoRect(fullTarget);
            photoEl.style.borderRadius = isCompactViewport() ? "0px" : "12px";
          }

          const prevImg = photoImg;
          fullLoader.dataset.wantFull = full;
          fullLoader.style.objectFit = "contain";
          fullLoader.style.opacity = "1";
          fullLoader.classList.toggle("dragging", prevImg.classList.contains("dragging"));
          fullLoader.addEventListener("dragstart", (e) => e.preventDefault());
          if (!photoEl || prevImg.parentNode !== photoEl) return;
          photoEl.replaceChild(fullLoader, prevImg);
          photoImg = fullLoader;
          applyTransform();
        };
        maybeSwap();
      })();
    }

    function stepViewer(dir, { animate = false } = {}) {
      const nextIndex = currentIndex + dir;
      if (nextIndex < 0 || nextIndex >= tiles.length) return false;
      const tile = tiles[nextIndex];
      if (!tile) return false;
      currentIndex = nextIndex;
      setViewerToTile(tile, { animate });
      return true;
    }

    function closeViewer() {
      if (!viewer.classList.contains("open")) return;
      document.body.style.overflow = "";
      viewer.classList.remove("open");

      currentTile = null;
      tiles = [];
      currentIndex = -1;
      openToken += 1;
      animDone = false;
      dragging = false;
      dragStart = null;
      clearSwipeVisualState();
      touchPoints.clear();
      pinching = false;
      pinchBaseDistance = 0;
      pinchBaseScale = 1;
      suppressStageClickUntil = 0;
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
      const thumb = qs("img", tile);
      if (!thumb || !full) return;

      currentTile = tile;
      currentIndex = tiles.indexOf(tile);

      setCaptionAndLink(tile);
      clearSwipeVisualState();

      ensurePhotoEl();
      resetTransform();

      const thumbRect = thumb.getBoundingClientRect();
      const existingRect = photoEl ? photoEl.getBoundingClientRect() : null;
      const existingAspect =
        existingRect && existingRect.width > 0 && existingRect.height > 0
          ? existingRect.width / existingRect.height
          : 0;
      const aspect = getTileAspect(tile, thumb, thumbRect, existingAspect || 1.5);

      viewer.classList.add("open");
      document.body.style.overflow = "hidden";
      syncViewerViewportMetrics();

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
      const target = containRectForAspect(aspect);
      const targetRadius = isCompactViewport() ? "0px" : "12px";
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
              borderRadius: targetRadius,
            },
          ],
          { duration: 260, easing: "cubic-bezier(0.2, 0.0, 0.2, 1.0)" },
        );

        anim.onfinish = () => {
          if (!isCurrent()) return;
          // Commit final rect.
          setPhotoRect(target);
          photoEl.style.borderRadius = targetRadius;
          photoImg.style.objectFit = "contain";
          animDone = true;
        };
      } else {
        setPhotoRect(target);
        photoEl.style.borderRadius = targetRadius;
        photoImg.style.objectFit = "contain";
        animDone = true;
      }

      // Load full-res in the background and snap once ready.
      beginFullSwap(full, isCurrent);
    }

    function refreshViewerLayout() {
      syncViewerViewportMetrics();
      if (!viewer.classList.contains("open")) return;
      if (!currentTile || !photoEl) return;
      if (swipeDrag || pinching) return;
      const imgAspect =
        photoImg && photoImg.naturalWidth && photoImg.naturalHeight
          ? photoImg.naturalWidth / Math.max(1, photoImg.naturalHeight)
          : 0;
      const aspect = getTileAspect(currentTile, null, null, imgAspect || 1.5);
      const rect = containRectForAspect(aspect);
      setPhotoRect(rect);
      photoEl.style.borderRadius = isCompactViewport() ? "0px" : "12px";
      applyTransform();
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
        stepViewer(-1, { animate: false });
      }
      if (e.key === "ArrowRight") {
        e.preventDefault();
        stepViewer(1, { animate: false });
      }
    });
    stage?.addEventListener("click", (e) => {
      if (Date.now() < suppressStageClickUntil) return;
      // Clicking outside the image closes.
      if (e.target === stage) closeViewer();
    });

    // Zoom interactions in viewer.
    function zoomAt(delta, originX, originY) {
      const prev = s;
      const next = clamp(prev * delta, 1, 6);
      if (next === prev) return;
      if (next <= 1.001) {
        resetTransform();
        return;
      }

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

    const getPinchPair = () => {
      const vals = Array.from(touchPoints.values());
      if (vals.length < 2) return null;
      return [vals[0], vals[1]];
    };
    const pointerDistance = (a, b) => Math.hypot(b.x - a.x, b.y - a.y);
    const pointerMid = (a, b) => ({ x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 });
    const startPinch = () => {
      const pair = getPinchPair();
      if (!pair) return;
      const [a, b] = pair;
      const d = pointerDistance(a, b);
      if (!Number.isFinite(d) || d <= 0) return;
      pinching = true;
      pinchBaseDistance = d;
      pinchBaseScale = s;
      clearSwipeVisualState();
      dragging = false;
      dragStart = null;
      if (photoImg) photoImg.classList.remove("dragging");
    };
    const updatePinch = () => {
      if (!pinching) return;
      const pair = getPinchPair();
      if (!pair) return;
      const [a, b] = pair;
      const d = pointerDistance(a, b);
      if (!Number.isFinite(d) || d <= 0 || pinchBaseDistance <= 0) return;
      const mid = pointerMid(a, b);
      const next = clamp((d / pinchBaseDistance) * pinchBaseScale, 1, 6);
      if (!photoEl) return;
      if (!Number.isFinite(next) || next <= 0) return;
      const delta = next / s;
      if (!Number.isFinite(delta) || delta <= 0) return;
      zoomAt(delta, mid.x, mid.y);
    };
    const stopPinch = () => {
      pinching = false;
      pinchBaseDistance = 0;
      pinchBaseScale = s;
      if (s <= 1.02) resetTransform();
    };
    const updateTouchPoint = (e) => {
      if (e.pointerType !== "touch") return false;
      if (!touchPoints.has(e.pointerId)) return false;
      touchPoints.set(e.pointerId, { x: e.clientX, y: e.clientY });
      return true;
    };
    const onViewerPointerDown = (e) => {
      if (!viewer.classList.contains("open")) return;
      if (e.pointerType !== "touch") return;
      touchPoints.set(e.pointerId, { x: e.clientX, y: e.clientY });
      if (touchPoints.size >= 2) {
        e.preventDefault();
        startPinch();
      }
    };
    const onWindowPointerMove = (e) => {
      if (!viewer.classList.contains("open")) return;
      if (!updateTouchPoint(e)) return;
      if (!pinching && touchPoints.size >= 2) startPinch();
      if (!pinching) return;
      e.preventDefault();
      updatePinch();
    };
    const onWindowPointerUpOrCancel = (e) => {
      if (e.pointerType === "touch") {
        touchPoints.delete(e.pointerId);
        if (touchPoints.size < 2) stopPinch();
      }
    };
    viewer?.addEventListener("pointerdown", onViewerPointerDown);
    window.addEventListener("pointermove", onWindowPointerMove, { passive: false });
    window.addEventListener("pointerup", onWindowPointerUpOrCancel);
    window.addEventListener("pointercancel", onWindowPointerUpOrCancel);

    // Interactive swipe: current image follows finger while adjacent image slides in.
    const getTilePreviewSrc = (tile) => {
      if (!tile) return "";
      const thumb = qs("img", tile);
      if (!thumb) return tile.getAttribute("data-full") || "";
      const pending = thumb.getAttribute("data-src");
      const loaded = thumb.currentSrc || thumb.getAttribute("src") || "";
      if (pending && (!loaded || loaded.startsWith("data:"))) return pending;
      return loaded || pending || tile.getAttribute("data-full") || "";
    };
    const ensureSwipePeer = (tile, dir) => {
      if (!tile || !photoEl) return false;
      if (swipePeerEl && swipeDrag && swipeDrag.targetTile === tile && swipePeerDir === dir) return true;
      clearSwipePeer();
      const peer = document.createElement("div");
      peer.className = "viewer-photo swipe-peer";
      const peerAspect = getTileAspect(tile, null, null, 1.5);
      const peerRect = containRectForAspect(peerAspect);
      peer.style.left = `${peerRect.left}px`;
      peer.style.top = `${peerRect.top}px`;
      peer.style.width = `${peerRect.width}px`;
      peer.style.height = `${peerRect.height}px`;
      peer.style.borderRadius = isCompactViewport() ? "0px" : "12px";
      const img = document.createElement("img");
      const full = tile.getAttribute("data-full") || "";
      const preview = getTilePreviewSrc(tile);
      img.src = preview || full;
      img.dataset.wantFull = full;
      img.style.objectFit = "contain";
      img.style.opacity = "1";
      img.addEventListener("dragstart", (ev) => ev.preventDefault());
      peer.appendChild(img);
      viewer.appendChild(peer);
      swipePeerEl = peer;
      swipePeerImg = img;
      swipePeerDir = dir;
      if (full && full !== preview) {
        const loader = new Image();
        loader.src = full;
        (async () => {
          try {
            if (typeof loader.decode === "function") await loader.decode();
            else {
              await new Promise((resolve) => {
                loader.onload = () => resolve();
                loader.onerror = () => resolve();
              });
            }
          } catch (_) {
            // keep preview
          }
          if (swipePeerEl !== peer || swipePeerImg !== img) return;
          if (!(loader.naturalWidth && loader.naturalHeight)) return;
          loader.dataset.wantFull = full;
          loader.style.objectFit = "contain";
          loader.style.opacity = "1";
          loader.addEventListener("dragstart", (ev) => ev.preventDefault());
          peer.replaceChild(loader, img);
          swipePeerImg = loader;
        })();
      }
      return true;
    };
    const updateSwipePositions = (dxRaw) => {
      if (!swipeDrag || !photoEl) return;
      const vw = Math.max(1, window.innerWidth);
      const damp = swipeDrag.dir === 0 ? 0.22 : 1;
      const dx = clamp(dxRaw * damp, -vw, vw);
      swipeDrag.dx = dx;
      photoEl.style.transition = "none";
      photoEl.style.transform = `translate3d(${dx}px, 0, 0)`;
      if (!swipePeerEl || swipeDrag.dir === 0) return;
      swipePeerEl.style.transition = "none";
      swipePeerEl.style.transform = `translate3d(${dx + swipeDrag.dir * vw}px, 0, 0)`;
    };
    const settleSwipe = (drag, commit) => {
      if (!photoEl) {
        clearSwipeVisualState();
        return;
      }
      const vw = Math.max(1, window.innerWidth);
      const dir = drag.dir || 0;
      const duration = commit ? 220 : 180;
      const easing = "cubic-bezier(0.2, 0.0, 0.2, 1.0)";
      const currentTo = commit ? -dir * vw : 0;
      const peerTo = commit ? 0 : dir * vw;
      photoEl.style.transition = `transform ${duration}ms ${easing}`;
      photoEl.style.transform = `translate3d(${currentTo}px, 0, 0)`;
      if (swipePeerEl) {
        swipePeerEl.style.transition = `transform ${duration}ms ${easing}`;
        swipePeerEl.style.transform = `translate3d(${peerTo}px, 0, 0)`;
      }
      window.setTimeout(() => {
        if (commit && dir !== 0) {
          suppressStageClickUntil = Date.now() + 300;
          const nextTile = drag.targetTile || tiles[currentIndex + dir] || null;
          if (nextTile && swipePeerEl && swipePeerImg) {
            const oldEl = photoEl;
            photoEl = swipePeerEl;
            photoImg = swipePeerImg;
            swipePeerEl = null;
            swipePeerImg = null;
            swipePeerDir = 0;
            photoEl.classList.remove("swipe-peer");
            if (oldEl && oldEl !== photoEl) oldEl.remove();
            currentTile = nextTile;
            currentIndex = tiles.indexOf(nextTile);
            setCaptionAndLink(nextTile);
            const nextAspect = getTileAspect(nextTile, null, null, 1.5);
            const nextRect = containRectForAspect(nextAspect);
            setPhotoRect(nextRect);
            photoEl.style.borderRadius = isCompactViewport() ? "0px" : "12px";
            photoImg.style.objectFit = "contain";
            resetTransform();
            animDone = true;
            openToken += 1;
            const token = openToken;
            const isCurrent = () => token === openToken && currentTile === nextTile;
            beginFullSwap(nextTile.getAttribute("data-full") || "", isCurrent);
            swipeDrag = null;
            return;
          }
          clearSwipeVisualState();
          stepViewer(dir, { animate: false });
          return;
        }
        clearSwipeVisualState();
      }, duration + 24);
    };
    const beginSwipe = (e) => {
      if (!viewer.classList.contains("open")) return;
      if (e.pointerType !== "touch") return;
      if (pinching || touchPoints.size > 1) return;
      if (s > 1) return;
      if (!photoEl) return;
      if (!(e.target instanceof Node)) return;
      if (!photoEl.contains(e.target) && e.target !== stage) return;
      clearSwipeVisualState();
      swipeDrag = {
        pointerId: e.pointerId,
        startX: e.clientX,
        startY: e.clientY,
        at: Date.now(),
        active: false,
        dir: 0,
        dx: 0,
        targetTile: null,
      };
    };
    const moveSwipe = (e) => {
      if (!swipeDrag) return;
      if (e.pointerId !== swipeDrag.pointerId) return;
      if (!viewer.classList.contains("open")) return;
      if (pinching || touchPoints.size > 1 || s > 1) {
        clearSwipeVisualState();
        return;
      }
      const dx = e.clientX - swipeDrag.startX;
      const dy = e.clientY - swipeDrag.startY;
      if (!swipeDrag.active) {
        if (Math.abs(dx) < swipeAxisLockPx && Math.abs(dy) < swipeAxisLockPx) return;
        if (Math.abs(dx) <= Math.abs(dy) * 1.05) {
          clearSwipeVisualState();
          return;
        }
        swipeDrag.active = true;
        swipeDrag.dir = dx < 0 ? 1 : -1;
        const targetIndex = currentIndex + swipeDrag.dir;
        if (targetIndex < 0 || targetIndex >= tiles.length) swipeDrag.dir = 0;
        else swipeDrag.targetTile = tiles[targetIndex];
        if (swipeDrag.dir !== 0 && swipeDrag.targetTile) ensureSwipePeer(swipeDrag.targetTile, swipeDrag.dir);
      }
      e.preventDefault();
      updateSwipePositions(dx);
    };
    const endSwipe = (e) => {
      if (!swipeDrag) return;
      if (e.pointerId !== swipeDrag.pointerId) return;
      const drag = swipeDrag;
      swipeDrag = null;
      if (!drag.active) {
        clearSwipeVisualState();
        return;
      }
      const dx = Number.isFinite(drag.dx) ? drag.dx : e.clientX - drag.startX;
      const dt = Math.max(1, Date.now() - drag.at);
      if (drag.dir === 0 || !drag.targetTile) {
        settleSwipe(drag, false);
        return;
      }
      const expectedSign = -drag.dir;
      const inExpectedDirection = dx * expectedSign > 0;
      const movedEnough = Math.abs(dx) > Math.max(swipeCommitPx, Math.round(window.innerWidth * 0.14));
      const flickEnough = inExpectedDirection && Math.abs(dx / dt) > swipeFlickPxPerMs;
      settleSwipe(drag, inExpectedDirection && (movedEnough || flickEnough));
    };
    const cancelSwipe = (e) => {
      if (!swipeDrag) return;
      if (e.pointerId !== swipeDrag.pointerId) return;
      const drag = swipeDrag;
      swipeDrag = null;
      if (drag.active) settleSwipe(drag, false);
      else clearSwipeVisualState();
    };
    viewer?.addEventListener("pointerdown", beginSwipe);
    window.addEventListener("pointermove", moveSwipe, { passive: false });
    window.addEventListener("pointerup", endSwipe);
    window.addEventListener("pointercancel", cancelSwipe);
    let viewportSyncRaf = 0;
    const scheduleViewerLayoutRefresh = () => {
      if (viewportSyncRaf) return;
      viewportSyncRaf = window.requestAnimationFrame(() => {
        viewportSyncRaf = 0;
        refreshViewerLayout();
      });
    };
    window.addEventListener("resize", scheduleViewerLayoutRefresh, { passive: true });
    window.visualViewport?.addEventListener("resize", scheduleViewerLayoutRefresh, { passive: true });
    window.visualViewport?.addEventListener("scroll", scheduleViewerLayoutRefresh, { passive: true });

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
