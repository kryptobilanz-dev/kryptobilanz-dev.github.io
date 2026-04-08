// Minimal static i18n toggle for the website (DE/EN).
// Persists choice in localStorage and swaps visible blocks.

(function () {
  const STORAGE_KEY = "kb_lang";
  const SUPPORTED = new Set(["de", "en"]);

  function getInitialLang() {
    const saved = (localStorage.getItem(STORAGE_KEY) || "").toLowerCase();
    if (SUPPORTED.has(saved)) return saved;
    const nav = (navigator.language || "de").toLowerCase();
    if (nav.startsWith("en")) return "en";
    return "de";
  }

  function applyLang(lang) {
    const l = SUPPORTED.has(lang) ? lang : "de";
    document.documentElement.setAttribute("lang", l);

    document.querySelectorAll("[data-lang]").forEach((el) => {
      el.style.display = el.getAttribute("data-lang") === l ? "" : "none";
    });

    document.querySelectorAll("[data-lang-btn]").forEach((btn) => {
      const isActive = (btn.getAttribute("data-lang-btn") || "").toLowerCase() === l;
      btn.classList.toggle("is-active", isActive);
      btn.setAttribute("aria-pressed", isActive ? "true" : "false");
    });

    const title = document.body && document.body.dataset ? document.body.dataset["title" + l.toUpperCase()] : "";
    if (title) document.title = title;

    try {
      localStorage.setItem(STORAGE_KEY, l);
    } catch (e) {
      // ignore
    }
  }

  function wire() {
    document.querySelectorAll("[data-lang-btn]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const l = (btn.getAttribute("data-lang-btn") || "").toLowerCase();
        applyLang(l);
      });
    });
  }

  const lang = getInitialLang();
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => {
      wire();
      applyLang(lang);
    });
  } else {
    wire();
    applyLang(lang);
  }
})();

