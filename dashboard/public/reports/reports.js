// Shared bilingual report toggle
// Usage: include this script; add <div class="lang-toggle"><button data-lang="zh">中文</button><button data-lang="en">EN</button></div> anywhere
// Pages using dark header: add class "on-dark" to .lang-toggle

(function() {
  const STORAGE_KEY = 'gpo-lang-pref';
  const SUPPORTED = ['zh', 'en'];

  function getInitialLang() {
    // Priority: URL hash > localStorage > browser lang > default zh
    const hash = window.location.hash.replace('#', '').toLowerCase();
    if (SUPPORTED.includes(hash)) return hash;

    const stored = localStorage.getItem(STORAGE_KEY);
    if (SUPPORTED.includes(stored)) return stored;

    const browser = (navigator.language || 'zh').toLowerCase();
    if (browser.startsWith('zh')) return 'zh';
    if (browser.startsWith('en')) return 'en';

    return 'zh';
  }

  function setLang(lang) {
    if (!SUPPORTED.includes(lang)) return;
    document.body.setAttribute('data-lang', lang);
    document.documentElement.setAttribute('lang', lang === 'zh' ? 'zh-CN' : 'en');
    localStorage.setItem(STORAGE_KEY, lang);

    // Update toggle button state
    document.querySelectorAll('.lang-toggle button').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.lang === lang);
    });

    // Update URL hash without scroll jump
    if (window.location.hash !== '#' + lang) {
      history.replaceState(null, '', '#' + lang);
    }
  }

  function init() {
    setLang(getInitialLang());

    // Wire up toggle buttons
    document.querySelectorAll('.lang-toggle button[data-lang]').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        setLang(btn.dataset.lang);
      });
    });

    // Listen to hash changes (e.g. when user manually edits URL)
    window.addEventListener('hashchange', () => {
      const hash = window.location.hash.replace('#', '').toLowerCase();
      if (SUPPORTED.includes(hash)) setLang(hash);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
