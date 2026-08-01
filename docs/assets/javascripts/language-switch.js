(() => {
  const bilingualRoutes = [
    "tutorial/",
    "journey/",
    "agent-guided/",
    "architecture/",
    "behavior-matrix/",
    "redis-mapping/",
    "labs-guide/",
  ];

  function siteBasePath() {
    const script = Array.from(document.scripts).find((item) =>
      item.src.includes("assets/javascripts/language-switch.js"),
    );
    if (!script) return "/";
    return new URL(script.src).pathname.replace(
      /assets\/javascripts\/language-switch\.js$/,
      "",
    );
  }

  function replaceLanguageLinks() {
    const base = siteBasePath();
    if (!window.location.pathname.startsWith(base)) return;

    const current = window.location.pathname.slice(base.length);
    const relative = current.startsWith("zh/") ? current.slice(3) : current;
    const isBilingual =
      relative === "" || bilingualRoutes.some((route) => relative.startsWith(route));
    if (!isBilingual) return;

    const targets = {
      English: base + relative,
      "简体中文": base + "zh/" + relative,
    };
    document
      .querySelectorAll(".md-tabs__link, nav.md-nav--primary a.md-nav__link")
      .forEach((link) => {
        const label = link.textContent.trim();
        if (targets[label]) link.href = targets[label];
      });
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(replaceLanguageLinks);
  } else if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", replaceLanguageLinks);
  } else {
    replaceLanguageLinks();
  }
})();
