/* Shared nav auth indicator + logout. Loaded by every dashboard page. */
(function () {
  function byId(id) { return document.getElementById(id); }

  function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  async function refreshAuthArea() {
    const area = byId('auth-area');
    if (!area) return;
    try {
      const r = await fetch('/api/auth/me', { cache: 'no-store' });
      const me = await r.json();
      if (me.authenticated) {
        area.innerHTML =
          '<span class="nav-user">' + escapeHtml(me.user || '') + '</span>' +
          '<a href="#" onclick="dashLogout();return false;">退出</a>';
      } else {
        const next = encodeURIComponent(location.pathname + location.search);
        area.innerHTML = '<a href="/login?next=' + next + '">登录</a>';
      }
    } catch (e) {
      /* nav is cosmetic — ignore network errors */
    }
  }

  window.dashLogout = async function () {
    try { await fetch('/api/auth/logout', { method: 'POST' }); } catch (e) { /* ignore */ }
    location.href = '/login';
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', refreshAuthArea);
  } else {
    refreshAuthArea();
  }
})();
