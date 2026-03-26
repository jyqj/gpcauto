import { $, $$ } from './utils.js';
import { initDashboard, loadDashboard } from './dashboard.js';
import { initRegister } from './register.js';
import { initAccounts, loadAccounts } from './accounts.js';
import { initCards, loadCards } from './cards.js';
import { initProxies, loadProxies } from './proxies.js';
import { initSettings, loadSettings } from './settings.js';

const saved = localStorage.getItem('theme');
const theme = saved || (window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark');
document.documentElement.setAttribute('data-theme', theme);

$('#themeToggle')?.addEventListener('click', () => {
  const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
});

$$('.tab').forEach(btn => {
  btn.addEventListener('click', () => {
    $$('.tab').forEach(t => t.classList.remove('active'));
    $$('.tab-page').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    $(`#page-${btn.dataset.tab}`).classList.add('active');

    const tab = btn.dataset.tab;
    if (tab === 'dashboard') loadDashboard();
    if (tab === 'accounts') loadAccounts();
    if (tab === 'cards') loadCards();
    if (tab === 'proxies') loadProxies();
    if (tab === 'settings') loadSettings();
  });
});

initDashboard();
initRegister();
initAccounts();
initCards();
initProxies();
initSettings();

loadDashboard();

function getActiveTab() {
  const active = document.querySelector('.tab.active');
  return active ? active.dataset.tab : null;
}

document.addEventListener('keydown', (e) => {
  const tag = (e.target.tagName || '').toLowerCase();
  const isInput = tag === 'input' || tag === 'textarea' || tag === 'select' || e.target.isContentEditable;

  if (e.key === 'Escape') {
    const tab = getActiveTab();
    if (tab === 'accounts' && window.acctSelectNone) window.acctSelectNone();
    if (tab === 'cards' && window.cardSelectNone) window.cardSelectNone();
    return;
  }

  if ((e.ctrlKey || e.metaKey) && e.key === 'a' && !isInput) {
    const tab = getActiveTab();
    if (tab === 'accounts' && window.acctSelectAll) { e.preventDefault(); window.acctSelectAll(); }
    if (tab === 'cards' && window.cardSelectAll) { e.preventDefault(); window.cardSelectAll(); }
  }
});
