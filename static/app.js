(() => {
  const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;

  function byId(id) {
    return document.getElementById(id);
  }

  function getInt(id) {
    const el = byId(id);
    return Number.parseInt(el ? el.value : "0", 10) || 0;
  }

  function setInt(id, value) {
    const el = byId(id);
    if (!el) return;
    el.value = String(Math.max(0, value));
  }

  function hideKeyboard() {
    try {
      if (tg && typeof tg.hideKeyboard === "function") {
        tg.hideKeyboard();
      }
    } catch (_) {}

    const active = document.activeElement;
    if (active && typeof active.blur === "function") {
      active.blur();
    }
  }

  function modemTotal() {
    return getInt("xb3") + getInt("xb6") + getInt("xb7") + getInt("xb8") + getInt("xb10");
  }

  function dvrTotal() {
    return getInt("xg1") + getInt("xg1_4k");
  }

  function showToast(message, tone = "info") {
    let toast = byId("app-toast");
    if (!toast) {
      toast = document.createElement("div");
      toast.id = "app-toast";
      toast.className = "toast";
      document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.dataset.tone = tone;
    toast.classList.add("show");
    window.clearTimeout(window.__toastTimer);
    window.__toastTimer = window.setTimeout(() => {
      toast.classList.remove("show");
    }, 2200);
  }

  function haptic(kind = "impact") {
    try {
      if (!tg || !tg.HapticFeedback) return;
      if (kind === "success") tg.HapticFeedback.notificationOccurred("success");
      else if (kind === "error") tg.HapticFeedback.notificationOccurred("error");
      else tg.HapticFeedback.impactOccurred("light");
    } catch (_) {}
  }

  function disableZoom() {
    document.addEventListener(
      "touchmove",
      function (event) {
        if (event.scale !== 1) {
          event.preventDefault();
        }
      },
      { passive: false }
    );
  }

  function applySafeArea() {
    const source = (tg && (tg.contentSafeAreaInset || tg.safeAreaInset)) || {};
    const root = document.documentElement;
    root.style.setProperty("--safe-top", `${source.top || 0}px`);
    root.style.setProperty("--safe-right", `${source.right || 0}px`);
    root.style.setProperty("--safe-bottom", `${source.bottom || 0}px`);
    root.style.setProperty("--safe-left", `${source.left || 0}px`);
  }

  function initTelegramChrome() {
    if (!tg) return;

    try {
      tg.ready();
      tg.expand();
      if (tg.disableVerticalSwipes) tg.disableVerticalSwipes();
      if (tg.requestFullscreen) tg.requestFullscreen();
      if (tg.setHeaderColor) tg.setHeaderColor("#f5f7fb");
      if (tg.setBackgroundColor) tg.setBackgroundColor("#eef3f8");
      if (tg.setBottomBarColor) tg.setBottomBarColor("#eef3f8");
    } catch (_) {}

    applySafeArea();

    try {
      tg.onEvent("safeAreaChanged", applySafeArea);
      tg.onEvent("contentSafeAreaChanged", applySafeArea);
      tg.onEvent("fullscreenChanged", applySafeArea);
      tg.onEvent("viewportChanged", applySafeArea);
    } catch (_) {}
  }

  function fillTelegramUserData() {
    if (!tg) return;
    const user = tg.initDataUnsafe && tg.initDataUnsafe.user ? tg.initDataUnsafe.user : null;
    if (!user) return;

    const telegramUserId = byId("telegram_user_id");
    const telegramUsername = byId("telegram_username");

    if (telegramUserId) telegramUserId.value = user.id || "";
    if (telegramUsername) telegramUsername.value = user.username || "";
  }

  function setBodyLocked(locked) {
    document.body.style.overflow = locked ? "hidden" : "";
  }

  window.acceptPrimaryNotice = function acceptPrimaryNotice() {
    hideKeyboard();
    const modal = byId("noticeModal");
    if (modal) modal.classList.add("hidden");

    if (window.HAS_ADMIN_MESSAGE) {
      const adminModal = byId("adminMessageModal");
      if (adminModal) {
        adminModal.classList.remove("hidden");
        setBodyLocked(true);
        return;
      }
    }

    setBodyLocked(false);
  };

  window.acceptAdminNotice = function acceptAdminNotice() {
    hideKeyboard();
    const adminModal = byId("adminMessageModal");
    if (adminModal) adminModal.classList.add("hidden");
    setBodyLocked(false);
  };

  function syncButtonsForInput(inputId, max, group = "") {
    const input = byId(inputId);
    if (!input) return;

    const parent = input.closest(".qty-controls");
    if (!parent) return;

    const minus = parent.querySelector('[data-role="minus"]');
    const plus = parent.querySelector('[data-role="plus"]');

    const value = getInt(inputId);

    if (minus) minus.disabled = value <= 0;

    let plusDisabled = false;
    if (group === "modems") {
      plusDisabled = modemTotal() >= window.APP_LIMITS.modems_total;
    } else if (group === "dvr") {
      plusDisabled = dvrTotal() >= window.APP_LIMITS.dvr_total;
    } else {
      plusDisabled = value >= max;
    }

    if (plus) plus.disabled = plusDisabled;
  }

  function refreshAllButtons() {
    ["xb3","xb6","xb7","xb8","xb10"].forEach(id => syncButtonsForInput(id, 999, "modems"));
    ["xg1","xg1_4k"].forEach(id => syncButtonsForInput(id, 999, "dvr"));

    syncButtonsForInput("xg2", window.APP_LIMITS.xg2);
    syncButtonsForInput("xid", window.APP_LIMITS.xid);
    syncButtonsForInput("xi6", window.APP_LIMITS.xi6);
    syncButtonsForInput("xer10", window.APP_LIMITS.xer10);
    syncButtonsForInput("onu", window.APP_LIMITS.onu);
    syncButtonsForInput("screen", window.APP_LIMITS.screen);
    syncButtonsForInput("battery", window.APP_LIMITS.battery);
    syncButtonsForInput("sensor", window.APP_LIMITS.sensor);
    syncButtonsForInput("camera", window.APP_LIMITS.camera);
    syncButtonsForInput("extra_item_qty", window.APP_LIMITS.extra_item);
  }

  function validateManualInput(inputId, max, group = "") {
    let value = getInt(inputId);

    if (group === "modems") {
      const current = getInt(inputId);
      const totalWithoutCurrent = modemTotal() - current;
      const allowed = Math.max(0, window.APP_LIMITS.modems_total - totalWithoutCurrent);
      if (value > allowed) {
        value = allowed;
        setInt(inputId, value);
        showToast(`Equipment limit reached. Maximum allowed in Modems is ${window.APP_LIMITS.modems_total}.`, "error");
        haptic("error");
      }
    } else if (group === "dvr") {
      const current = getInt(inputId);
      const totalWithoutCurrent = dvrTotal() - current;
      const allowed = Math.max(0, window.APP_LIMITS.dvr_total - totalWithoutCurrent);
      if (value > allowed) {
        value = allowed;
        setInt(inputId, value);
        showToast(`Equipment limit reached. Maximum allowed in DVR is ${window.APP_LIMITS.dvr_total}.`, "error");
        haptic("error");
      }
    } else if (value > max) {
      setInt(inputId, max);
      showToast(`Equipment limit reached. Maximum allowed is ${max}.`, "error");
      haptic("error");
    }

    refreshAllButtons();
  }

  window.adjustQty = function adjustQty(field, delta, max, group = "") {
    hideKeyboard();

    const current = getInt(field);
    const next = Math.max(0, current + delta);

    if (group === "modems") {
      const projected = modemTotal() - current + next;
      if (projected > window.APP_LIMITS.modems_total) {
        showToast(`Equipment limit reached. Maximum allowed in Modems is ${window.APP_LIMITS.modems_total}.`, "error");
        haptic("error");
        refreshAllButtons();
        return;
      }
    } else if (group === "dvr") {
      const projected = dvrTotal() - current + next;
      if (projected > window.APP_LIMITS.dvr_total) {
        showToast(`Equipment limit reached. Maximum allowed in DVR is ${window.APP_LIMITS.dvr_total}.`, "error");
        haptic("error");
        refreshAllButtons();
        return;
      }
    } else if (next > max) {
      showToast(`Equipment limit reached. Maximum allowed is ${max}.`, "error");
      haptic("error");
      refreshAllButtons();
      return;
    }

    setInt(field, next);
    haptic("impact");
    refreshAllButtons();
  };

  function initInputs() {
    const pairs = [
      ["xb3", 999, "modems"],
      ["xb6", 999, "modems"],
      ["xb7", 999, "modems"],
      ["xb8", 999, "modems"],
      ["xb10", 999, "modems"],
      ["xg1", 999, "dvr"],
      ["xg1_4k", 999, "dvr"],
      ["xg2", window.APP_LIMITS.xg2, ""],
      ["xid", window.APP_LIMITS.xid, ""],
      ["xi6", window.APP_LIMITS.xi6, ""],
      ["xer10", window.APP_LIMITS.xer10, ""],
      ["onu", window.APP_LIMITS.onu, ""],
      ["screen", window.APP_LIMITS.screen, ""],
      ["battery", window.APP_LIMITS.battery, ""],
      ["sensor", window.APP_LIMITS.sensor, ""],
      ["camera", window.APP_LIMITS.camera, ""],
      ["extra_item_qty", window.APP_LIMITS.extra_item, ""],
    ];

    pairs.forEach(([id, max, group]) => {
      const input = byId(id);
      if (!input) return;

      input.addEventListener("input", () => validateManualInput(id, max, group));
      input.addEventListener("focus", () => {});
    });

    const bpInput = byId("bp_number");
    if (bpInput) {
      bpInput.addEventListener("keydown", function (e) {
        if (e.key === "Enter") {
          e.preventDefault();
          hideKeyboard();
        }
      });
    }

    const techInput = byId("tech_id");
    if (techInput) {
      techInput.addEventListener("keydown", function (e) {
        if (e.key === "Enter") {
          e.preventDefault();
          hideKeyboard();
          byId("bp_number")?.focus();
        }
      });
    }
  }

  function initForm() {
    const form = byId("requestForm");
    if (!form) return;

    form.addEventListener("submit", (event) => {
      hideKeyboard();

      const techId = (byId("tech_id")?.value || "").trim();
      const bpNumber = (byId("bp_number")?.value || "").trim();
      const extraItemName = (byId("extra_item_name")?.value || "").trim();
      const extraItemQty = getInt("extra_item_qty");

      if (!techId) {
        event.preventDefault();
        showToast("Tech ID is required.", "error");
        haptic("error");
        return;
      }

      if (!bpNumber) {
        event.preventDefault();
        showToast("BP Number is required.", "error");
        haptic("error");
        return;
      }

      if (extraItemQty > 0 && !extraItemName) {
        event.preventDefault();
        showToast("Please enter the item name for Add Item.", "error");
        haptic("error");
        return;
      }

      const total =
        modemTotal() +
        dvrTotal() +
        getInt("xg2") +
        getInt("xid") +
        getInt("xi6") +
        getInt("xer10") +
        getInt("onu") +
        getInt("screen") +
        getInt("battery") +
        getInt("sensor") +
        getInt("camera") +
        extraItemQty;

      if (total <= 0) {
        event.preventDefault();
        showToast("Please add at least one equipment item.", "error");
        haptic("error");
        return;
      }

      const submitBtn = byId("submitBtn");
      if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = "Sending...";
      }

      haptic("success");
    });
  }

  function initSuccessState() {
    const body = document.body;
    const msg = body ? body.dataset.successMessage : "";
    if (msg) {
      showToast(msg, "success");
      try {
        if (tg && typeof tg.showAlert === "function") {
          tg.showAlert(msg);
        }
      } catch (_) {}
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    initTelegramChrome();
    fillTelegramUserData();
    disableZoom();
    initInputs();
    initForm();
    initSuccessState();
    refreshAllButtons();
    setBodyLocked(true);
  });
})();