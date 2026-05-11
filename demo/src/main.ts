import { SimulatedServer } from './SimulatedServer.js';
import { Scanner } from './Scanner.js';

interface Guest {
  id: string;
  name: string;
}
const GUESTS: Guest[] = [
  { id: 'g1', name: 'Sarah Chen' },
  { id: 'g2', name: 'Marcus Williams' },
  { id: 'g3', name: 'Priya Patel' },
  { id: 'g4', name: 'Diego Ramos' },
  { id: 'g5', name: 'Hannah Kim' },
  { id: 'g6', name: 'Jamal Carter' },
  { id: 'g7', name: 'Sofia Reyes' },
  { id: 'g8', name: 'Wyatt Bauer' },
  { id: 'g9', name: 'Mia Andersson' },
  { id: 'g10', name: 'Theo Nakamura' },
];

const $ = <T extends HTMLElement = HTMLElement>(sel: string): T => {
  const el = document.querySelector(sel);
  if (!el) throw new Error(`Missing element: ${sel}`);
  return el as T;
};

let useLibrary = true;
let online = true;

const server = new SimulatedServer();
server.useLibrary = useLibrary;
server.wifiOnline = online;

const scanner = new Scanner(
  server,
  () => useLibrary,
  () => online
);

// --- Controls
const wifiBtn = $<HTMLButtonElement>('#wifi-btn');
function setOnline(next: boolean) {
  online = next;
  server.wifiOnline = next;
  wifiBtn.dataset.state = next ? 'online' : 'offline';
  wifiBtn.querySelector('.label')!.textContent = `WiFi: ${next ? 'ONLINE' : 'OFFLINE'}`;
  if (next) scanner.onNetworkResume();
  render();
}
wifiBtn.addEventListener('click', () => setOnline(!online));

const libraryToggle = $<HTMLInputElement>('#library-toggle');
libraryToggle.checked = useLibrary;
libraryToggle.addEventListener('change', () => {
  useLibrary = libraryToggle.checked;
  server.useLibrary = useLibrary;
  hardReset();
});

$<HTMLButtonElement>('#reset-btn').addEventListener('click', hardReset);

function hardReset() {
  server.reset();
  scanner.reset();
  render();
}

// --- Guest list (the scanner UI)
function renderGuests() {
  const container = $('#guests');
  container.innerHTML = '';
  for (const g of GUESTS) {
    const card = document.createElement('div');
    card.className = 'guest';
    card.innerHTML = `
      <div class="guest-main">
        <div class="guest-name">${g.name}</div>
        <div class="guest-status" data-status="${g.id}">—</div>
      </div>
      <button data-admit="${g.id}" class="btn-admit">Admit</button>
    `;
    container.appendChild(card);
  }
  container.addEventListener('click', (e) => {
    const target = e.target as HTMLElement;
    const id = target.getAttribute('data-admit');
    if (!id) return;
    const guest = GUESTS.find((g) => g.id === id);
    if (!guest) return;
    void scanner.admit({ guestId: guest.id, guestName: guest.name });
    render();
  });
}

// --- Ledger + counters
async function render() {
  const ledger = server.ledger();
  const ledgerEl = $('#ledger');
  ledgerEl.innerHTML = '';
  for (const r of ledger.slice(-15).reverse()) {
    const li = document.createElement('li');
    const time = new Date(r.serverTs).toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
    li.innerHTML = `
      <span class="seq">#${r.seqId}</span>
      <span class="who">${r.guestName}</span>
      <span class="when">${time}</span>
    `;
    ledgerEl.appendChild(li);
  }

  $('#server-count').textContent = `${server.totalRecorded()} admit${server.totalRecorded() === 1 ? '' : 's'}`;

  const dups = server.duplicateCount();
  const banner = $('#dup-banner');
  if (dups > 0) {
    banner.removeAttribute('hidden');
    $('#dup-count').textContent = String(dups);
    $('#dup-plural').textContent = dups === 1 ? '' : 's';
  } else {
    banner.setAttribute('hidden', '');
  }

  const pending = await scanner.pendingCount();
  $('#queue-pill').textContent = `${pending} pending`;
  $('#queue-pill').classList.toggle('queue-active', pending > 0);

  // Per-guest count under each name
  const countByGuest = new Map<string, number>();
  for (const r of ledger) {
    countByGuest.set(r.guestId, (countByGuest.get(r.guestId) ?? 0) + 1);
  }
  for (const g of GUESTS) {
    const el = document.querySelector(`[data-status="${g.id}"]`);
    if (!el) continue;
    const c = countByGuest.get(g.id) ?? 0;
    if (c === 0) {
      el.textContent = '—';
      el.className = 'guest-status';
    } else if (c === 1) {
      el.textContent = '✓ admitted';
      el.className = 'guest-status good';
    } else {
      el.textContent = `⚠ admitted ${c}× — ${c - 1} duplicate`;
      el.className = 'guest-status bad';
    }
  }
}

server.onChange(() => render());
scanner.onEvent(() => render());

// Boot
renderGuests();
render();

// Drive the pending-count UI smoothly (queue size is async).
setInterval(render, 400);
