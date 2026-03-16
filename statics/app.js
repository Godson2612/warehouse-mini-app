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
    el.value = String(value);
    el.dispatchEvent(new Event("change", { bubbles: true }));
  }

  function totalModems() {
    return getInt("xb3") + getInt("xb6") + getInt("xb7") + getInt("xb8") + getInt("xb10");
  }

  function haptic(kind = "impact") {
    try {
      if (!tg || !tg.HapticFeedback) return;
      if (kind === "success") tg.HapticFeedback.notificationOccurred("success");
      else if (kind === "error") tg.HapticFeedback.notificationOccurred("error");
      else tg.HapticFeedback.impactOccurred("light");
    } catch (_) {}
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
    }, 1800);
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

  window.adjustQty = function adjustQty(field, delta, max, group = "") {
    const current = getInt(field);
    const next = Math.max(0, current + delta);

    if (group === "modems") {
      const projected = totalModems() - current + next;
      const modemCap = (window.APP_LIMITS && window.APP_LIMITS.modems_total) || 12;
      if (projected > modemCap) {
        showToast(`Total modems cannot exceed ${modemCap}.`, "error");
        haptic("error");
        return;
      }
    }

    if (next > max) {
      showToast(`This item cannot exceed ${max}.`, "error");
      haptic("error");
      return;
    }

    setInt(field, next);
    haptic("impact");
  };

  function applySafeArea() {
    const source = (tg && (tg.contentSafeAreaInset || tg.safeAreaInset)) || {};
    const root = document.documentElement;
    root.style.setProperty("--safe-top", `${source.top || 0}px`);
    root.style.setProperty("--safe-right", `${source.right || 0}px`);
    root.style.setProperty("--safe-bottom", `${source.bottom || 0}px`);
    root.style.setProperty("--safe-left", `${source.left || 0}px`);
  }

  function fillTelegramUserData() {
    if (!tg) return;
    const user = tg.initDataUnsafe && tg.initDataUnsafe.user ? tg.initDataUnsafe.user : null;

    const raw = byId("raw_telegram_data");
    if (raw) raw.value = tg.initData || "";

    if (!user) return;

    const id = byId("telegram_user_id");
    const username = byId("telegram_username");
    const name = byId("telegram_name");

    if (id) id.value = user.id || "";
    if (username) username.value = user.username || "";
    if (name) {
      name.value = [user.first_name, user.last_name].filter(Boolean).join(" ");
    }
  }

  function initTelegramChrome() {
    if (!tg) return;

    try {
      tg.ready();
      tg.expand();
      if (tg.disableVerticalSwipes) tg.disableVerticalSwipes();
      if (tg.setHeaderColor) tg.setHeaderColor("#f5f7fb");
      if (tg.setBackgroundColor) tg.setBackgroundColor("#eef3f8");
      if (tg.setBottomBarColor) tg.setBottomBarColor("#eef3f8");
      if (tg.requestFullscreen) tg.requestFullscreen();
    } catch (_) {}

    applySafeArea();
    fillTelegramUserData();

    try {
      tg.onEvent("safeAreaChanged", applySafeArea);
      tg.onEvent("contentSafeAreaChanged", applySafeArea);
      tg.onEvent("fullscreenChanged", applySafeArea);
      tg.onEvent("viewportChanged", applySafeArea);
    } catch (_) {}
  }

  function initKeyboardBehavior() {
    document.addEventListener("keydown", (event) => {
      const target = event.target;
      if (!target) return;

      if (
        event.key === "Enter" &&
        target.matches('input[type="text"], input[type="search"], input[type="number"], textarea')
      ) {
        event.preventDefault();
        hideKeyboard();
      }
    });

    document.addEventListener("click", (event) => {
      const active = document.activeElement;
      const clickedInsideField = event.target.closest("input, textarea, .counter, button, label");
      if (
        active &&
        (active.tagName === "INPUT" || active.tagName === "TEXTAREA") &&
        !clickedInsideField
      ) {
        hideKeyboard();
      }
    });
  }

  function initForm() {
    const form = byId("requestForm");
    if (!form) return;

    form.addEventListener("submit", (event) => {
      hideKeyboard();

      const bpNumber = (byId("bp_number") ? byId("bp_number").value : "").trim();
      if (!bpNumber) {
        event.preventDefault();
        showToast("BP Number is required.", "error");
        haptic("error");
        return;
      }

      const total =
        totalModems() +
        getInt("xi6") +
        getInt("xid") +
        getInt("xg2") +
        getInt("dvr") +
        getInt("onu") +
        getInt("xer10") +
        getInt("camera") +
        getInt("battery") +
        getInt("sensor") +
        getInt("screen") +
        getInt("extra_qty");

      if (total <= 0) {
        event.preventDefault();
        showToast("Please select at least one item.", "error");
        haptic("error");
        return;
      }

      if (getInt("extra_qty") > 0) {
        const note = (byId("extra_note") ? byId("extra_note").value : "").trim();
        if (!note) {
          event.preventDefault();
          showToast("Please describe the additional equipment.", "error");
          haptic("error");
          return;
        }
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
    initKeyboardBehavior();
    initForm();
    initSuccessState();
  });
})();