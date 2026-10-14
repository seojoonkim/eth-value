/*
 * ETHval desktop sidebar navigation (>= 1200px).
 *  - Replaces the inline top menu with a fixed left vertical menu.
 *  - Level 1: sections (01 Valuation ... 06 Research). Level 2: numbered subsections
 *    (01.1, 02.3 ...) or model cards in Ratings — always visible.
 *  - Level 3 (metric cards / panels under a subsection) is collapsible per subsection.
 *  - Search box filters every level (matches expand their parents); Enter jumps to the first hit.
 *  - Scroll-spy highlights the current item. Labels come from the live DOM, so they follow the
 *    current language; the tree is rebuilt when <html lang> changes or late sections appear.
 * Mobile/tablet (< 1200px) keeps the existing top/2nd-row nav untouched.
 */
(function () {
  'use strict';
  var BP = 1200;           // desktop web view breakpoint
  var W = 236;             // sidebar width (px)
  var mq = window.matchMedia('(min-width: ' + BP + 'px)');
  var root, headEl, listEl, inputEl, emptyEl, items = [], built = false, spyRaf = 0;
  var openState = {};      // level-2 key -> expanded (kept across rebuilds)
  try { openState = JSON.parse(sessionStorage.getItem('ethval-sb-open') || '{}') || {}; } catch (e) { openState = {}; }

  var L = {
    en: { search: 'Search menu', empty: 'No matches', toggle: 'Show or hide items', nav: 'Page menu' },
    ko: { search: '메뉴 검색', empty: '일치하는 항목 없음', toggle: '하위 항목 펼치기/접기', nav: '페이지 메뉴' },
    zh: { search: '搜索菜单', empty: '无匹配项', toggle: '展开或收起', nav: '页面菜单' },
    ja: { search: 'メニュー検索', empty: '該当なし', toggle: '展開/折りたたみ', nav: 'ページメニュー' }
  };
  function lang() { var l = (document.documentElement.lang || 'en').slice(0, 2); return L[l] ? l : 'en'; }
  function txt(el) { return el ? el.textContent.replace(/\s+/g, ' ').trim() : ''; }

  var CSS = [
    '@media (min-width:' + BP + 'px){',
    ' body.has-sb{padding-left:' + W + 'px}',
    ' body.has-sb .nav-bar-inline{display:none!important}',
    ' body.has-sb .header-wrapper{left:' + W + 'px!important;right:0;width:auto!important;box-sizing:border-box}',  /* header bar starts right of the rail so the rail logo is not covered */
    ' body.has-sb .recent-participants-avatars{min-width:0}',
    ' body.has-sb .summary-opportunity-wrapper{flex-wrap:wrap;justify-content:flex-end;row-gap:6px;overflow:visible}',  /* narrower body: "Undervalued" badge must not clip */
    ' body.has-sb .recent-participants-avatars img:not(:first-child){margin-left:-15px}',  /* body is ' + W + 'px narrower: 10 avatars + more must fit the community box */
    ' .sb{position:fixed;left:0;top:0;bottom:0;width:' + W + 'px;z-index:9000;display:flex;flex-direction:column;',
    '  background:#eceef2;border-right:1px solid #d5d9e0;box-sizing:border-box}',
    ' body.dark .sb{background:#050608;border-right-color:#22262d}',
    ' body.has-sb .header-wrapper .header>.logo{visibility:hidden}',   /* the logo moves to the top of the rail */
    '}',
    '@media (max-width:' + (BP - 1) + 'px){ .sb{display:none!important} }',
    '.sb-head{flex:0 0 auto;display:flex;align-items:center;padding:0 16px;box-sizing:border-box;border-bottom:1px solid #d5d9e0}',
    'body.dark .sb-head{border-bottom-color:#22262d}',
    '.sb-head .logo{margin:0}',
    '.sb-search{padding:12px 12px 8px;flex:0 0 auto}',
    '.sb-search input{width:100%;box-sizing:border-box;height:32px;padding:0 10px 0 30px;border-radius:8px;',
    ' border:1px solid var(--border,rgba(127,127,127,.25));background:var(--bg-card) url("data:image/svg+xml,%3Csvg xmlns=%27http://www.w3.org/2000/svg%27 width=%2714%27 height=%2714%27 viewBox=%270 0 24 24%27 fill=%27none%27 stroke=%27%23888%27 stroke-width=%272.2%27 stroke-linecap=%27round%27%3E%3Ccircle cx=%2711%27 cy=%2711%27 r=%277%27/%3E%3Cpath d=%27m20 20-4-4%27/%3E%3C/svg%3E") no-repeat 10px 50%;',
    ' color:var(--text-primary,inherit);font:inherit;font-size:13px;outline:none}',
    '.sb-search input:focus{border-color:var(--accent,#627eea)}',
    '.sb-list{flex:1 1 auto;overflow-y:auto;overscroll-behavior:contain;padding:2px 8px 24px;scrollbar-width:thin}',
    '.sb-empty{padding:10px 12px;font-size:12px;color:var(--text-muted,#888)}',
    '.sb ul{list-style:none;margin:0;padding:0}',
    '.sb-l1{margin-top:10px}',
    '.sb-l1:first-child{margin-top:2px}',
    '.sb-row{display:flex;align-items:center;border-radius:6px}',
    '.sb-row:hover{background:rgba(127,127,127,.09)}',
    '.sb a{display:flex;gap:6px;align-items:baseline;flex:1 1 auto;min-width:0;padding:5px 8px;color:var(--text-secondary,inherit);',
    ' text-decoration:none;font-size:13px;line-height:1.35;word-break:keep-all;overflow-wrap:anywhere;border-radius:6px}',
    '.sb a .n{flex:0 0 auto;font-variant-numeric:tabular-nums;color:var(--text-muted,#888);font-size:12px}',
    '.sb-l1>.sb-row>a{font-weight:700;color:var(--text-primary,inherit);font-size:16px;padding:7px 8px;letter-spacing:-.01em}',
    '.sb-l1>.sb-row>a .n{font-size:13px;font-weight:600}',
    '.sb-l2>.sb-row>a{padding-left:20px}',
    '.sb-l3>a{padding:4px 8px 4px 40px;font-size:12px;color:var(--text-muted,#888)}',
    '.sb a.on{color:var(--accent,#627eea);background:rgba(98,126,234,.11)}',
    '.sb-l1>.sb-row>a.in{color:var(--accent,#627eea)}',
    '.sb a:focus-visible,.sb-tg:focus-visible{outline:2px solid var(--accent,#627eea);outline-offset:-2px}',
    '.sb-tg{flex:0 0 auto;height:22px;margin-right:4px;padding:0 5px 0 7px;gap:2px;border:1px solid var(--border,rgba(127,127,127,.3));background:var(--bg-card);',
    ' color:var(--text-secondary,#888);cursor:pointer;border-radius:11px;display:flex;align-items:center;justify-content:center;font:inherit;font-size:12px;font-weight:600;font-variant-numeric:tabular-nums;line-height:1}',
    '.sb-tg:hover{border-color:var(--accent,#627eea);color:var(--accent,#627eea)}',
    '.sb-tg svg{transition:transform .15s ease;transform:rotate(90deg)}',
    '.sb-l2.open>.sb-row>.sb-tg{background:var(--accent,#627eea);border-color:var(--accent,#627eea);color:#fff}',
    '.sb-l2.open>.sb-row>.sb-tg svg{transform:rotate(-90deg)}',
    '.sb-l2.open>ul{border-left:1px solid var(--border,rgba(127,127,127,.3));margin:2px 0 6px 27px}',
    '.sb-l2.open>ul .sb-l3>a{padding-left:12px}',
    '.sb-l2>ul{display:none}',
    '.sb-l2.open>ul{display:block}',
    '.sb .hide{display:none!important}',
    '.sb mark{background:rgba(255,196,0,.35);color:inherit;border-radius:2px}'
  ].join('\n');

  function injectCss() {
    if (document.getElementById('sb-css')) return;
    var s = document.createElement('style'); s.id = 'sb-css'; s.textContent = CSS; document.head.appendChild(s);
  }

  function ensureId(el, id) { if (!el.id) el.id = id; return el.id; }

  // Build the tree model from the live DOM.
  function model() {
    var out = [];
    var secs = document.querySelectorAll('section.section[id]');
    var n1 = 0;
    secs.forEach(function (s) {
      var tEl = s.querySelector(':scope>.section-header .section-title, :scope>.section-title');
      var title = txt(tEl);
      var node;
      if (!title) {
        // Title-less wrapper (e.g. #section-valuation holding 01.2–01.4) belongs to the previous titled section.
        node = out[out.length - 1];
        if (!node) return;
      } else {
        n1++;
        node = { key: s.id, num: (n1 < 10 ? '0' : '') + n1, label: title, target: s, kids: [] };
        out.push(node);
      }
      var subs = [].filter.call(s.querySelectorAll('.metrics-subgroup'), function (g) {
        return g.closest('section.section') === s && !node.kids.some(function (k) { return k.target === g; });
      });
      if (subs.length) {
        subs.forEach(function (g) {
          var tt = g.querySelector('.metrics-subgroup-title');
          var sn = txt(tt && tt.querySelector('.sg-num'));
          var sl = txt(tt && tt.querySelector('.sg-txt')) || txt(tt);
          if (!sl) return;
          var gid = ensureId(g, 'sg-' + (sn || sl).replace(/[^\w]+/g, '-'));
          var kid = { key: gid, num: sn, label: sl, target: g, kids: [] };
          // Level 3: metric cards that belong to this subgroup (same parent, until next subgroup)
          var n = g.nextElementSibling;
          while (n && !n.classList.contains('metrics-subgroup')) {
            if (n.classList.contains('metric-card')) {
              var h = n.querySelector('h4');
              var ml = txt(h);
              if (ml) kid.kids.push({ key: ensureId(n, 'mc-' + (n.getAttribute('data-metric') || ml).replace(/[^\w]+/g, '-')), label: ml, target: n });
            }
            n = n.nextElementSibling;
          }
          node.kids.push(kid);
        });
      } else if (title) {
        // Ratings: model cards as level 2
        s.querySelectorAll('.method-card[data-model]').forEach(function (c) {
          var ml = txt(c.querySelector('.method-name'));
          if (ml) node.kids.push({ key: ensureId(c, 'mm-' + c.getAttribute('data-model')), label: ml, target: c, kids: [] });
        });
      }
    });
    return out;
  }

  var CHEV = '<svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true"><path d="M3.5 1.8 6.7 5 3.5 8.2" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>';

  function esc(s) { return s.replace(/[&<>"]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; }); }

  function link(node) {
    var a = document.createElement('a');
    a.href = '#' + node.key;
    a.innerHTML = (node.num ? '<span class="n">' + esc(node.num) + '</span>' : '') + '<span class="t">' + esc(node.label) + '</span>';
    a.addEventListener('click', function (e) { e.preventDefault(); go(node); });
    items.push({ a: a, node: node });
    return a;
  }

  function render() {
    var tree = model();
    if (!tree.length) return false;
    items = [];
    listEl.innerHTML = '';
    var ul1 = document.createElement('ul');
    tree.forEach(function (n1) {
      var li1 = document.createElement('li'); li1.className = 'sb-l1';
      var r1 = document.createElement('div'); r1.className = 'sb-row'; r1.appendChild(link(n1)); li1.appendChild(r1);
      if (n1.kids.length) {
        var ul2 = document.createElement('ul');
        n1.kids.forEach(function (n2) {
          var li2 = document.createElement('li'); li2.className = 'sb-l2';
          var r2 = document.createElement('div'); r2.className = 'sb-row';
          r2.appendChild(link(n2));
          if (n2.kids.length) {
            var b = document.createElement('button');
            b.type = 'button'; b.className = 'sb-tg'; b.innerHTML = '<span class="sb-c">' + n2.kids.length + '</span>' + CHEV;
            b.setAttribute('aria-label', L[lang()].toggle);
            var ul3 = document.createElement('ul'); ul3.id = 'sbk-' + n2.key;
            b.setAttribute('aria-controls', ul3.id);
            b.addEventListener('click', function () { setOpen(li2, n2.key, !li2.classList.contains('open')); });
            r2.appendChild(b);
            n2.kids.forEach(function (n3) {
              var li3 = document.createElement('li'); li3.className = 'sb-l3'; li3.appendChild(link(n3)); ul3.appendChild(li3);
            });
            li2.appendChild(r2); li2.appendChild(ul3);
            setOpen(li2, n2.key, !!openState[n2.key], true);
          } else li2.appendChild(r2);
          ul2.appendChild(li2);
        });
        li1.appendChild(ul2);
      }
      ul1.appendChild(li1);
    });
    listEl.appendChild(ul1);
    built = true;
    applyLabels();
    if (inputEl.value) filter(inputEl.value);
    spy();
    return true;
  }

  function setOpen(li, key, open, silent) {
    li.classList.toggle('open', open);
    var b = li.querySelector(':scope>.sb-row>.sb-tg'); if (b) b.setAttribute('aria-expanded', open ? 'true' : 'false');
    if (!silent) { openState[key] = open; try { sessionStorage.setItem('ethval-sb-open', JSON.stringify(openState)); } catch (e) {} }
  }

  function headerH() {
    var hw = document.querySelector('.header-wrapper');
    return hw ? Math.round(hw.getBoundingClientRect().height) : 60;
  }

  function go(node) {
    var t = node.target; if (!t || !t.isConnected) { var f = document.getElementById(node.key); if (!f) return; t = f; }
    var y = t.getBoundingClientRect().top + window.pageYOffset - headerH() - 12;
    window.scrollTo({ top: Math.max(0, y), behavior: 'smooth' });
    // Charts above the target can still grow while smooth-scrolling; once the scroll has settled, re-aim —
    // unless the user has taken over scrolling (wheel/touch/keys/scrollbar), which cancels the re-aim at once.
    clearInterval(go._t);
    var last = -1, still = 0, n = 0;
    function cancel() { clearInterval(go._t); USER_EV.forEach(function (e) { window.removeEventListener(e, cancel, true); }); }
    var USER_EV = ['wheel', 'touchstart', 'keydown', 'mousedown'];
    USER_EV.forEach(function (e) { window.addEventListener(e, cancel, { capture: true, passive: true }); });
    go._t = setInterval(function () {
      n++;
      var yNow = window.pageYOffset;
      still = Math.abs(yNow - last) < 1 ? still + 1 : 0; last = yNow;
      if (still >= 2 || n > 25) {
        cancel();
        var off = t.getBoundingClientRect().top - headerH() - 12;
        // Only a small correction for late layout growth; a big jump means the user went elsewhere.
        if (Math.abs(off) > 4 && Math.abs(off) < window.innerHeight * 0.6) window.scrollTo({ top: yNow + off, behavior: 'instant' });
      }
    }, 120);
    // URL hash is reserved for the language (#kr/#en/#zh) — do not touch it.
  }

  function norm(s) { return s.toLowerCase().replace(/\s+/g, ''); }

  function filter(q) {
    var qq = norm(q);
    var any = false;
    listEl.querySelectorAll('li').forEach(function (li) { li.classList.remove('hide'); });
    listEl.querySelectorAll('mark').forEach(function (m) { m.replaceWith(document.createTextNode(m.textContent)); });
    items.forEach(function (it) { var t = it.a.querySelector('.t'); t.textContent = it.node.label; });
    if (!qq) {
      listEl.querySelectorAll('.sb-l2').forEach(function (li) { var a = li.querySelector('a'); var k = a && a.getAttribute('href').slice(1); setOpen(li, k, !!openState[k], true); });
      emptyEl.classList.add('hide');
      return;
    }
    var hit = function (li) { var a = li.querySelector(':scope>a, :scope>.sb-row>a'); return a && norm(a.textContent).indexOf(qq) >= 0; };
    listEl.querySelectorAll('.sb-l1').forEach(function (l1) {
      var l1hit = hit(l1), l1keep = l1hit;
      l1.querySelectorAll(':scope>ul>.sb-l2').forEach(function (l2) {
        var l2hit = hit(l2), l3any = false;
        l2.querySelectorAll('.sb-l3').forEach(function (l3) { var h = hit(l3) || l2hit || l1hit; l3.classList.toggle('hide', !h); if (hit(l3)) l3any = true; });
        var keep = l1hit || l2hit || l3any;
        l2.classList.toggle('hide', !keep);
        l2.classList.toggle('open', l3any);
        if (keep) l1keep = true;
      });
      l1.classList.toggle('hide', !l1keep);
      if (l1keep) any = true;
    });
    items.forEach(function (it) {
      var t = it.a.querySelector('.t'); var lab = it.node.label; var i = lab.toLowerCase().indexOf(q.trim().toLowerCase());
      if (q.trim() && i >= 0) t.innerHTML = esc(lab.slice(0, i)) + '<mark>' + esc(lab.slice(i, i + q.trim().length)) + '</mark>' + esc(lab.slice(i + q.trim().length));
    });
    emptyEl.classList.toggle('hide', any);
  }

  function firstVisible() {
    for (var i = 0; i < items.length; i++) { var a = items[i].a; if (a.offsetParent !== null && a.closest('li') && !a.closest('.hide')) { if (!inputEl.value || a.querySelector('mark')) return items[i]; } }
    return null;
  }

  function spy() {
    if (!built || !mq.matches) return;
    // Same reading line as the site's top-tab rule (2026-10-14): middle of the visible area under the header.
    var hb = headerH(), line = hb + (window.innerHeight - hb) * 0.5, cur = null, best = -Infinity;
    for (var i = 0; i < items.length; i++) {
      var t = items[i].node.target; if (!t || !t.isConnected) continue;
      var r = t.getBoundingClientRect(); if (!r.height && !r.width) continue;
      // latest-starting item above the line wins; ties go to the deeper (later) item
      if (r.top <= line && r.top >= best) { best = r.top; cur = items[i]; }
    }
    items.forEach(function (it) { it.a.classList.toggle('on', it === cur); if (it === cur) it.a.setAttribute('aria-current', 'location'); else it.a.removeAttribute('aria-current'); });
    listEl.querySelectorAll('.sb-l1>.sb-row>a').forEach(function (a) { a.classList.toggle('in', !!(cur && a.closest('.sb-l1').contains(cur.a))); });
    if (cur) {
      var li3 = cur.a.closest('.sb-l3');
      var vis = li3 && !li3.closest('.sb-l2').classList.contains('open') ? li3.closest('.sb-l2').querySelector('a') : cur.a;
      if (vis !== cur.a) vis.classList.add('on');
      var lr = listEl.getBoundingClientRect(), ar = vis.getBoundingClientRect();
      if (ar.top < lr.top + 8 || ar.bottom > lr.bottom - 8) listEl.scrollTop += (ar.top - lr.top) - lr.height / 3;
    }
  }

  function place() {
    if (!root) return;
    if (headEl) headEl.style.height = headerH() + 'px';   /* logo row lines up with the header bar */
    document.body.classList.toggle('has-sb', mq.matches);
  }

  function applyLabels() {
    var l = L[lang()];
    inputEl.placeholder = l.search; inputEl.setAttribute('aria-label', l.search);
    emptyEl.textContent = l.empty; root.setAttribute('aria-label', l.nav);
    root.querySelectorAll('.sb-tg').forEach(function (b) { b.setAttribute('aria-label', l.toggle); });
  }

  var rebuildT = 0;
  function rebuildSoon() { clearTimeout(rebuildT); rebuildT = setTimeout(render, 250); }

  function init() {
    if (document.querySelector('.sb')) return;
    injectCss();
    root = document.createElement('nav'); root.className = 'sb'; root.setAttribute('data-ethval-sidebar', '1');
    var sw = document.createElement('div'); sw.className = 'sb-search';
    inputEl = document.createElement('input'); inputEl.type = 'search'; inputEl.autocomplete = 'off'; inputEl.spellcheck = false;
    sw.appendChild(inputEl);
    emptyEl = document.createElement('div'); emptyEl.className = 'sb-empty hide';
    listEl = document.createElement('div'); listEl.className = 'sb-list';
    headEl = document.createElement('div'); headEl.className = 'sb-head';
    var lg = document.querySelector('.header .logo');
    if (lg) {
      var cl = lg.cloneNode(true); cl.removeAttribute('onclick'); cl.removeAttribute('onkeydown');
      cl.querySelectorAll('[id]').forEach(function (e) { e.id = 'sb-' + e.id; });
      cl.querySelectorAll('[fill^="url(#"]').forEach(function (e) { e.setAttribute('fill', e.getAttribute('fill').replace('url(#', 'url(#sb-')); });
      cl.addEventListener('click', function () { lg.click(); });
      cl.addEventListener('keydown', function (e) { if (e.key === 'Enter') lg.click(); });
      headEl.appendChild(cl);
    }
    root.appendChild(headEl); root.appendChild(sw); root.appendChild(emptyEl); root.appendChild(listEl);
    document.body.appendChild(root);
    inputEl.addEventListener('input', function () { filter(inputEl.value); });
    inputEl.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') { var f = firstVisible(); if (f) go(f.node); }
      else if (e.key === 'Escape') { inputEl.value = ''; filter(''); inputEl.blur(); }
    });
    // "/" focuses the search (when not typing elsewhere)
    document.addEventListener('keydown', function (e) {
      if (e.key === '/' && mq.matches && !/INPUT|TEXTAREA|SELECT/.test((document.activeElement || {}).tagName || '') && !(document.activeElement || {}).isContentEditable) { e.preventDefault(); inputEl.focus(); }
    });
    place(); render();
    window.addEventListener('scroll', function () { if (!spyRaf) spyRaf = requestAnimationFrame(function () { spyRaf = 0; spy(); }); }, { passive: true });
    window.addEventListener('resize', function () { place(); spy(); });
    if (mq.addEventListener) mq.addEventListener('change', function () { place(); spy(); });
    // language switch -> labels in DOM change -> rebuild
    new MutationObserver(function () { rebuildSoon(); }).observe(document.documentElement, { attributes: true, attributeFilter: ['lang'] });
    // late sections (research library, community) and late i18n fills
    var mo = new MutationObserver(function (m) {
      for (var i = 0; i < m.length; i++) {
        var t = m[i].target;
        if (root.contains(t)) continue;
        for (var j = 0; j < m[i].addedNodes.length; j++) { var n = m[i].addedNodes[j]; if (n.nodeType === 1 && (n.matches('section.section,.metrics-subgroup,.metric-card,.method-card') || (n.classList && n.classList.contains('section-title')))) { rebuildSoon(); return; } }
      }
    });
    mo.observe(document.querySelector('.container') || document.body, { childList: true, subtree: true });
    setTimeout(render, 1500); setTimeout(render, 4000);   // i18n + extras settle
    window.EthvalSidebar = { render: render, filter: function (q) { inputEl.value = q; filter(q); }, items: function () { return items; } };
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
