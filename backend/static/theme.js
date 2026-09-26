const THEME_STORAGE_KEY = "swissdgim-theme";

function readTheme() {
  try {
    const saved = window.localStorage.getItem(THEME_STORAGE_KEY);
    return saved === "dark" ? "dark" : "light";
  } catch {
    return "light";
  }
}

function writeTheme(theme) {
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, theme === "dark" ? "dark" : "light");
  } catch {
    // Ignore storage errors and keep the current session theme only.
  }
}

function applyTheme(theme) {
  const value = theme === "dark" ? "dark" : "light";
  document.body.dataset.theme = value;

  const buttons = document.querySelectorAll("#themeToggle");
  buttons.forEach((button) => {
    button.textContent = value === "dark" ? "Light mode" : "Dark mode";
    button.setAttribute("aria-pressed", value === "dark" ? "true" : "false");
  });
}

function toggleTheme() {
  const next = document.body.dataset.theme === "dark" ? "light" : "dark";
  writeTheme(next);
  applyTheme(next);
}

document.addEventListener("DOMContentLoaded", () => {
  applyTheme(readTheme());

  document.querySelectorAll("#themeToggle").forEach((button) => {
    button.addEventListener("click", toggleTheme);
  });
});