function intVal(id) {
  return parseInt(document.getElementById(id).value || "0", 10);
}

function setVal(id, value) {
  document.getElementById(id).value = value;
}

function modemTotal() {
  return intVal("xb3") + intVal("xb6") + intVal("xb7") + intVal("xb8") + intVal("xb10");
}

function changeQty(field, delta) {
  const current = intVal(field);
  const next = Math.max(0, current + delta);

  const currentTotal = modemTotal();
  const currentFieldValue = intVal(field);
  const projectedTotal = currentTotal - currentFieldValue + next;

  if (projectedTotal > 12) {
    alert("Total modems cannot exceed 12.");
    return;
  }

  setVal(field, next);
}

function changeQtyLimited(field, delta, max) {
  const current = intVal(field);
  let next = current + delta;
  if (next < 0) next = 0;
  if (next > max) {
    alert(`This item cannot exceed ${max}.`);
    return;
  }
  setVal(field, next);
}

window.addEventListener("DOMContentLoaded", () => {
  if (window.Telegram && Telegram.WebApp) {
    Telegram.WebApp.ready();
    Telegram.WebApp.expand();

    const user = Telegram.WebApp.initDataUnsafe?.user;
    document.getElementById("raw_telegram_data").value = Telegram.WebApp.initData || "";

    if (user) {
      document.getElementById("telegram_user_id").value = user.id || "";
      document.getElementById("telegram_username").value = user.username || "";
      document.getElementById("telegram_name").value =
        [user.first_name, user.last_name].filter(Boolean).join(" ");
    }
  }
});