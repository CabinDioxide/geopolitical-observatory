/**
 * Geopolitical Observatory — Conflict Dashboard v2
 * Preset views + Country vulnerability + Multi-layer analysis
 */

// ============================================================
// STATE
// ============================================================
const state = {
  data: null, filteredFeatures: [],
  sources: new Set(), eventTypes: new Set(),
  sourceVisibility: {}, typeVisibility: {},
  dateFrom: null, dateTo: null,
  markers: L.layerGroup(),
  basesData: null, basesLayer: L.layerGroup(),
  tradeData: null, tradeLayer: L.layerGroup(),
  maritimeData: null, maritimeLayer: L.layerGroup(),
  analysisLayer: L.layerGroup(),
  activePreset: 'overview',
  countryData: null,
};

// ============================================================
// COLORS
// ============================================================
const SOURCE_COLORS = { acled:'#e63946', gdelt:'#f4a261', ucdp:'#457b9d', bellingcat:'#52b788', default:'#6c757d' };
const COMMODITY_COLORS = {
  semiconductors:'#60a5fa', energy_oil:'#fbbf24', energy_gas:'#f97316', minerals:'#ef4444',
  rare_earths:'#c084fc', food:'#4ade80', manufacturing:'#a78bfa', defense:'#9ca3af',
  new_energy:'#34d399', ai_compute:'#818cf8', default:'#6b7280',
};
const OPERATOR_COLORS = { US:'#3b82f6', China:'#ef4444', Russia:'#a855f7', NATO:'#06b6d4', UK:'#f59e0b', France:'#8b5cf6', Japan:'#ec4899', India:'#f97316', Turkey:'#14b8a6', Iran:'#84cc16', default:'#9ca3af' };
const LANE_COLORS = { oil:'#fbbf24', container:'#38bdf8', bulk:'#f87171', lng:'#a78bfa', default:'#6b7280' };

// ============================================================
// MAP INIT
// ============================================================
const map = L.map('map', { center:[20,30], zoom:3, zoomControl:false, preferCanvas:true });
L.control.zoom({ position:'bottomright' }).addTo(map);
L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
  attribution:'&copy; CARTO &copy; OSM', subdomains:'abcd', maxZoom:19,
}).addTo(map);

state.maritimeLayer.addTo(map);
state.tradeLayer.addTo(map);
state.markers.addTo(map);
state.basesLayer.addTo(map);
state.analysisLayer.addTo(map);

// Leaflet tooltips used instead of popups

// ============================================================
// RENDERING: Conflict Events
// ============================================================
function renderEvents(features) {
  state.markers.clearLayers();
  features.forEach(f => {
    const coords = f.geometry?.coordinates;
    if (!coords || coords.length < 2) return;
    const [lng, lat] = coords;
    if (lat === 0 && lng === 0) return;
    const p = f.properties || {};
    const color = state.activePreset === 'conflicts' ? '#e63946' : (SOURCE_COLORS[p.source] || SOURCE_COLORS.default);
    const marker = L.circleMarker([lat, lng], { radius:5, fillColor:color, fillOpacity:0.8, color:'rgba(255,255,255,0.3)', weight:1 });
    marker.bindTooltip(`<strong>${esc(p.title||'Event')}</strong><br><span style="opacity:0.7">${p.date||''} · ${(p.source||'').toUpperCase()}</span>`, { className:'dark-tooltip', direction:'top', offset:[0,-8] });
    marker.on('click', () => showEventDetail(f));
    state.markers.addLayer(marker);
  });
}

function showEventDetail(feature) {
  const p = feature.properties || {};
  const coords = feature.geometry.coordinates;
  const actors = Array.isArray(p.actors) ? p.actors.join(', ') : (p.actors || '');
  const links = (Array.isArray(p.links) ? p.links : (p.links||'').split(' | ')).filter(Boolean)
    .map(u => `<a href="${esc(u)}" target="_blank" style="color:#2a9d8f">${esc(u.slice(0,50))}</a>`).join('<br>');
  document.getElementById('detail-content').innerHTML = `
    <h3>${esc(p.title||'Event')}</h3>
    <div class="detail-row"><span class="key">Source</span><span class="val"><span class="source-badge ${p.source||''}">${esc(p.source||'?')}</span></span></div>
    <div class="detail-row"><span class="key">Date</span><span class="val">${esc(p.date||'N/A')}</span></div>
    <div class="detail-row"><span class="key">Type</span><span class="val">${esc(p.event_type||'N/A')}</span></div>
    ${p.fatalities?`<div class="detail-row"><span class="key">Fatalities</span><span class="val">${p.fatalities}</span></div>`:''}
    ${actors?`<div class="detail-row"><span class="key">Actors</span><span class="val">${esc(actors)}</span></div>`:''}
    ${p.country?`<div class="detail-row"><span class="key">Country</span><span class="val">${esc(p.country)}</span></div>`:''}
    ${links?`<div class="detail-row"><span class="key">Links</span><span class="val">${links}</span></div>`:''}
    <div class="detail-row" style="opacity:0.5"><span class="key">Coords</span><span class="val">${coords[1].toFixed(4)}, ${coords[0].toFixed(4)}</span></div>`;
  document.getElementById('detail-panel').classList.remove('hidden');
}

// ============================================================
// RENDERING: Military Bases
// ============================================================
function renderBases(features, filterOps) {
  state.basesLayer.clearLayers();
  features.forEach(f => {
    const coords = f.geometry?.coordinates; if (!coords) return;
    const [lng, lat] = coords; const p = f.properties || {};
    if (filterOps && !filterOps.has(p.operator)) return;
    const color = OPERATOR_COLORS[p.operator] || OPERATOR_COLORS.default;
    const marker = L.circleMarker([lat, lng], { radius:4, fillColor:color, fillOpacity:0.9, color:'#fff', weight:1.5 });
    const icon = {naval:'⚓',air:'✈',army:'⬟',joint:'★',intelligence:'◉',nuclear:'☢'}[p.type]||'▲';
    marker.bindTooltip(`<strong>${icon} ${esc(p.name)}</strong><br><span style="opacity:0.7">${esc(p.operator)} · ${esc(p.type)}</span>${p.chokepoint?`<br><span style="color:#f4a261">⚠ ${esc(p.chokepoint)}</span>`:''}`, { className:'dark-tooltip', direction:'top', offset:[0,-6] });
    marker.on('click', () => {
      document.getElementById('detail-content').innerHTML = `
        <h3>${icon} ${esc(p.name)}</h3>
        <div class="detail-row"><span class="key">Operator</span><span class="val" style="color:${color};font-weight:700">${esc(p.operator)}</span></div>
        <div class="detail-row"><span class="key">Host</span><span class="val">${esc(p.host_country||p.country)}</span></div>
        <div class="detail-row"><span class="key">Type</span><span class="val">${esc(p.type)}</span></div>
        ${p.strategic_role?`<div class="detail-row"><span class="key">Role</span><span class="val">${esc(p.strategic_role)}</span></div>`:''}
        ${p.chokepoint?`<div class="detail-row"><span class="key">Chokepoint</span><span class="val" style="color:#f4a261">${esc(p.chokepoint)}</span></div>`:''}`;
      document.getElementById('detail-panel').classList.remove('hidden');
    });
    state.basesLayer.addLayer(marker);
  });
}

// ============================================================
// RENDERING: Maritime (Chokepoints + Lanes + Vessels)
// ============================================================
function renderMaritime(features) {
  state.maritimeLayer.clearLayers();
  features.forEach(f => {
    const p = f.properties || {};
    const geomType = f.geometry?.type;
    if (geomType === 'Point' && p.type === 'vessel') {
      const [lng, lat] = f.geometry.coordinates;
      const m = L.circleMarker([lat, lng], { radius:2.5, fillColor:'#22d3ee', fillOpacity:0.7, color:'#0e7490', weight:0.5 });
      m.bindTooltip(`<strong>🚢 ${esc(p.name)}</strong><br><span style="opacity:0.7">${p.speed||0}kn · ${esc(p.nav_status||'')}</span>`, { className:'dark-tooltip', direction:'top', offset:[0,-4] });
      state.maritimeLayer.addLayer(m);
    } else if (geomType === 'Point') {
      const [lng, lat] = f.geometry.coordinates;
      const vulnColor = {critical:'#ef4444',high:'#f97316',medium:'#fbbf24'}[p.vulnerability]||'#9ca3af';
      const m = L.circleMarker([lat, lng], { radius:7, fillColor:vulnColor, fillOpacity:0.7, color:'#fff', weight:2 });
      m.bindTooltip(`<strong>⚓ ${esc(p.name)}</strong><br><span style="color:${vulnColor}">${esc(p.vulnerability)} chokepoint</span>`, { className:'dark-tooltip', direction:'top', offset:[0,-8] });
      m.on('click', () => analyzeChokepoint(p.name));
      state.maritimeLayer.addLayer(m);
    } else if (geomType === 'LineString') {
      const coords = f.geometry.coordinates.map(c => [c[1], c[0]]);
      const color = LANE_COLORS[p.category] || LANE_COLORS.default;
      const line = L.polyline(coords, { color, weight:2, opacity:0.35, dashArray:'6 4' });
      line.bindTooltip(`<strong>${esc(p.name)}</strong><br><span style="opacity:0.7">${esc(p.category||'')} route</span>`, { className:'dark-tooltip', sticky:true });
      state.maritimeLayer.addLayer(line);
    }
  });
}

// ============================================================
// RENDERING: Trade Flows
// ============================================================
function tradeLineWeight(v) { return v && v > 0 ? Math.max(1, Math.min(6, Math.log10(v/1e9)+1)) : 1; }

function renderTradeFlows(features, filterGroups) {
  state.tradeLayer.clearLayers();
  features.forEach(f => {
    const coords = f.geometry?.coordinates; if (!coords || coords.length < 2) return;
    const p = f.properties || {};
    if (filterGroups && !filterGroups.has(p.commodity_group)) return;

    const routePoints = coords.map(c => [c[1], c[0]]);
    const isPipeline = p.route_type === 'pipeline';
    const pipeStatus = p.pipeline_status || '';
    const isDestroyed = pipeStatus === 'destroyed' || pipeStatus === 'halted';

    // Pipeline styling
    let color, weight, opacity, dashArray;
    if (isPipeline) {
      color = isDestroyed ? '#ef4444' : '#f97316'; // red=destroyed, orange=active
      weight = 3;
      opacity = isDestroyed ? 0.7 : 0.6;
      dashArray = isDestroyed ? '4 8' : '8 4'; // short dash=destroyed, long dash=active
      // White border for pipeline visibility
      const border = L.polyline(routePoints, { color:'#fff', weight:weight+2, opacity:0.15, smoothFactor:1 });
      state.tradeLayer.addLayer(border);
    } else {
      color = COMMODITY_COLORS[p.commodity_group] || COMMODITY_COLORS.default;
      weight = tradeLineWeight(p.trade_value_usd);
      opacity = 0.5;
      dashArray = p.vulnerability === 'critical' ? '8 4' : null;
    }

    const line = L.polyline(routePoints, { color, weight, opacity, smoothFactor:1, dashArray });

    // Tooltip
    const valueStr = p.trade_value_usd >= 1e9 ? `$${(p.trade_value_usd/1e9).toFixed(1)}B` : p.trade_value_usd > 0 ? `$${(p.trade_value_usd/1e6).toFixed(0)}M` : '$0';
    const statusLabel = isDestroyed ? `<br><span style="color:#ef4444;font-weight:700">❌ ${pipeStatus.toUpperCase()}</span>` : pipeStatus === 'reduced' ? `<br><span style="color:#f97316">⚠ REDUCED</span>` : '';
    const pipeIcon = isPipeline ? '🔵 ' : '';

    line.bindTooltip(`<strong>${pipeIcon}${esc(p.exporter)} → ${esc(p.importer)}</strong><br>${esc(p.commodity)}${isPipeline ? '' : ' · '+valueStr}${statusLabel}`, { className:'dark-tooltip', sticky:true });
    line.on('click', () => showTradeDetail(f));
    state.tradeLayer.addLayer(line);
  });
}

function showTradeDetail(feature) {
  const p = feature.properties || {};
  const color = COMMODITY_COLORS[p.commodity_group]||COMMODITY_COLORS.default;
  const v = p.trade_value_usd>=1e9?`$${(p.trade_value_usd/1e9).toFixed(1)}B`:`$${(p.trade_value_usd/1e6).toFixed(0)}M`;
  const vc = {critical:'#ef4444',high:'#f97316',medium:'#fbbf24'}[p.vulnerability]||'#9ca3af';
  document.getElementById('detail-content').innerHTML = `
    <h3>${esc(p.exporter)} → ${esc(p.importer)}</h3>
    <div class="detail-row"><span class="key">Commodity</span><span class="val" style="color:${color}">${esc(p.commodity)}</span></div>
    <div class="detail-row"><span class="key">Group</span><span class="val">${esc(p.commodity_group)}</span></div>
    <div class="detail-row"><span class="key">Value</span><span class="val">${v} (${p.year})</span></div>
    <div class="detail-row"><span class="key">Vulnerability</span><span class="val" style="color:${vc};font-weight:700">${esc(p.vulnerability)}</span></div>
    ${p.pipeline_status?`<div class="detail-row"><span class="key">Status</span><span class="val" style="color:${p.pipeline_status==='destroyed'||p.pipeline_status==='halted'?'#ef4444':p.pipeline_status==='reduced'?'#f97316':'#4ade80'};font-weight:700">${p.pipeline_status.toUpperCase()}</span></div>`:''}
    ${p.route_type==='pipeline'?`<div class="detail-row"><span class="key">Type</span><span class="val">🔵 Pipeline infrastructure</span></div>`:''}
    ${p.strategic_note?`<div class="detail-row"><span class="key">Analysis</span><span class="val">${esc(p.strategic_note)}</span></div>`:''}`;
  document.getElementById('detail-panel').classList.remove('hidden');
}

// ============================================================
// DATA LOADING
// ============================================================
async function loadConflicts() {
  try {
    const r = await fetch('/data/conflicts/merged_conflicts.geojson');
    if (r.ok) state.data = await r.json();
  } catch(e) {}
  if (!state.data) state.data = {type:'FeatureCollection', features:[]};
  state.data.features.forEach(f => {
    const p = f.properties||{};
    if (p.source) state.sources.add(p.source);
    if (p.event_type) state.eventTypes.add(p.event_type);
  });
  state.sources.forEach(s => { state.sourceVisibility[s] = true; });
  state.eventTypes.forEach(t => { state.typeVisibility[t] = true; });
  const now = new Date(); const ago = new Date(now); ago.setMonth(ago.getMonth()-3);
  state.dateFrom = ago.toISOString().slice(0,10);
  state.dateTo = now.toISOString().slice(0,10);
  document.getElementById('date-from').value = state.dateFrom;
  document.getElementById('date-to').value = state.dateTo;
}

async function loadMaritime() {
  const all = [];
  try { const r=await fetch('/data/maritime/chokepoints.geojson'); if(r.ok){const d=await r.json(); all.push(...(d.features||[]));} } catch(e){}
  try { const r=await fetch('/data/maritime/vessels_snapshot.geojson'); if(r.ok){const d=await r.json(); all.push(...(d.features||[]));} } catch(e){}
  state.maritimeData = {type:'FeatureCollection', features:all};
}

async function loadTrade() {
  try { const r=await fetch('/data/trade/strategic_flows.geojson'); if(r.ok) state.tradeData=await r.json(); } catch(e){}
}

async function loadBases() {
  try { const r=await fetch('/data/bases/military_bases.geojson'); if(r.ok) state.basesData=await r.json(); } catch(e){}
}

async function loadCountryCentroids() {
  try { const r=await fetch('/data/trade/country_centroids.json'); if(r.ok) state.countryData=await r.json(); } catch(e){}
}

// ============================================================
// FILTERING
// ============================================================
function applyFilters() {
  if (!state.data) return;
  state.filteredFeatures = state.data.features.filter(f => {
    const p = f.properties||{};
    if (p.source && !state.sourceVisibility[p.source]) return false;
    if (p.event_type && !state.typeVisibility[p.event_type]) return false;
    if (p.date && state.dateFrom && p.date < state.dateFrom) return false;
    if (p.date && state.dateTo && p.date > state.dateTo) return false;
    const c = f.geometry?.coordinates;
    return c && c.length >= 2 && !(c[0]===0 && c[1]===0);
  });
  renderEvents(state.filteredFeatures);
  updateStats();
}

function updateStats() {
  const f = state.filteredFeatures;
  const el = document.getElementById('stats');
  if (!el) return;
  el.innerHTML = `
    <div class="stat-card"><div class="value">${f.length.toLocaleString()}</div><div class="label">Events</div></div>
    <div class="stat-card"><div class="value">${f.reduce((s,x)=>s+(x.properties?.fatalities||0),0).toLocaleString()}</div><div class="label">Fatalities</div></div>
    <div class="stat-card"><div class="value">${new Set(f.map(x=>x.properties?.country).filter(Boolean)).size}</div><div class="label">Countries</div></div>
    <div class="stat-card"><div class="value">${state.tradeData?.features?.length||0}</div><div class="label">Trade Flows</div></div>`;
}

// ============================================================
// PRESET SYSTEM
// ============================================================
const PRESETS = {
  overview: {
    trade: null, conflicts: false, bases: true, maritime: true,
    zoom: [20, 30, 2.5],
  },
  energy: {
    trade: new Set(['energy_oil','energy_gas']),
    conflicts: false, bases: false, maritime: true,
    zoom: [28, 50, 4],
  },
  new_energy: {
    trade: new Set(['new_energy','minerals','rare_earths']),
    conflicts: false, bases: false, maritime: true,
    zoom: [10, 60, 2.5],
  },
  ai: {
    trade: new Set(['ai_compute','semiconductors']),
    conflicts: false, bases: false, maritime: false,
    zoom: [30, 120, 4],
  },
  semiconductor: {
    trade: new Set(['semiconductors']),
    conflicts: false, bases: false, maritime: false,
    zoom: [30, 120, 4],
  },
  balance: {
    trade: null, conflicts: false, bases: false, maritime: false,
    zoom: [30, 70, 2.5],
    custom: 'analyzeBalance',
  },
  conflicts: {
    trade: null, conflicts: true, bases: false, maritime: true,
    zoom: [30, 30, 3],
  },
  full: {
    trade: 'all', conflicts: true, bases: true, maritime: true,
    zoom: null,
  },
};

function applyPreset(name) {
  const preset = PRESETS[name];
  if (!preset) return;

  state.activePreset = name;
  document.querySelectorAll('.preset-btn:not(.country-btn)').forEach(b => b.classList.toggle('active', b.dataset.preset === name));
  document.querySelectorAll('.country-btn').forEach(b => b.classList.remove('active'));

  // Show/hide filters section
  document.getElementById('filters-section').classList.toggle('collapsed', name !== 'full');

  // Close detail panel
  document.getElementById('detail-panel').classList.add('hidden');
  state.analysisLayer.clearLayers();

  // Trade flows
  if (preset.trade === null) {
    map.removeLayer(state.tradeLayer);
  } else {
    state.tradeLayer.addTo(map);
    const groups = preset.trade === 'all' ? null : preset.trade;
    renderTradeFlows(state.tradeData?.features || [], groups);
  }

  // Conflicts
  if (preset.conflicts) {
    state.markers.addTo(map);
    applyFilters();
  } else {
    map.removeLayer(state.markers);
  }

  // Bases
  if (preset.bases) {
    state.basesLayer.addTo(map);
    renderBases(state.basesData?.features || []);
  } else {
    map.removeLayer(state.basesLayer);
  }

  // Maritime
  if (preset.maritime) {
    state.maritimeLayer.addTo(map);
  } else {
    map.removeLayer(state.maritimeLayer);
  }

  // Zoom
  if (preset.zoom) {
    map.setView([preset.zoom[0], preset.zoom[1]], preset.zoom[2]);
  }

  // Custom analysis
  if (preset.custom === 'analyzeBalance') {
    setTimeout(analyzeBalance, 500);
  }

  updateStats();
}

// ============================================================
// BALANCE OF POWER — Supply Chain Mutual Deterrence
// ============================================================
function analyzeBalance() {
  if (!state.tradeData) return;
  const trades = state.tradeData.features;

  // Define blocs
  const westBloc = new Set(['USA','GBR','FRA','DEU','ITA','CAN','JPN','KOR','AUS','NLD','TWN']);
  const chinaBloc = new Set(['CHN']);

  // West → China leverage (tech controls)
  const westToChina = trades.filter(t => {
    const exp = t.properties.exporter_code, imp = t.properties.importer_code;
    return westBloc.has(exp) && chinaBloc.has(imp) && ['critical','high'].includes(t.properties.vulnerability);
  });

  // China → West leverage (mineral processing, manufacturing)
  const chinaToWest = trades.filter(t => {
    const exp = t.properties.exporter_code, imp = t.properties.importer_code;
    return chinaBloc.has(exp) && westBloc.has(imp) && ['critical','high'].includes(t.properties.vulnerability);
  });

  const westVal = westToChina.reduce((s,t) => s + (t.properties.trade_value_usd||0), 0);
  const chinaVal = chinaToWest.reduce((s,t) => s + (t.properties.trade_value_usd||0), 0);

  // Categorize leverage types
  const westLeverage = {};
  westToChina.forEach(t => {
    const g = t.properties.commodity_group;
    westLeverage[g] = (westLeverage[g]||0) + (t.properties.trade_value_usd||0);
  });
  const chinaLeverage = {};
  chinaToWest.forEach(t => {
    const g = t.properties.commodity_group;
    chinaLeverage[g] = (chinaLeverage[g]||0) + (t.properties.trade_value_usd||0);
  });

  // Render on map: blue = West leverage, red/orange = China leverage
  state.analysisLayer.clearLayers();
  state.tradeLayer.addTo(map);
  renderTradeFlows([], new Set());

  westToChina.forEach(f => {
    const pts = f.geometry.coordinates.map(c => [c[1], c[0]]);
    state.analysisLayer.addLayer(L.polyline(pts, { color:'#3b82f6', weight:3, opacity:0.7 }));
  });
  chinaToWest.forEach(f => {
    const pts = f.geometry.coordinates.map(c => [c[1], c[0]]);
    state.analysisLayer.addLayer(L.polyline(pts, { color:'#ef4444', weight:3, opacity:0.7 }));
  });

  // Balance ratio
  const ratio = westVal > 0 ? chinaVal / westVal : 0;
  let balanceLabel, balanceColor;
  if (ratio > 1.3) { balanceLabel = 'CHINA ADVANTAGE'; balanceColor = '#ef4444'; }
  else if (ratio < 0.7) { balanceLabel = 'WESTERN ADVANTAGE'; balanceColor = '#3b82f6'; }
  else { balanceLabel = 'MUTUAL DETERRENCE'; balanceColor = '#fbbf24'; }

  const groupLabels = { semiconductors:'Semiconductors', energy_oil:'Oil', energy_gas:'Gas', minerals:'Minerals', rare_earths:'Rare Earths', food:'Food', manufacturing:'Manufacturing', defense:'Defense', new_energy:'New Energy', ai_compute:'AI Compute' };

  const westRows = Object.entries(westLeverage).sort((a,b)=>b[1]-a[1])
    .map(([g,v]) => `<div style="display:flex;justify-content:space-between;font-size:11px;margin:2px 0"><span style="color:#3b82f6">${groupLabels[g]||g}</span><span>$${(v/1e9).toFixed(0)}B</span></div>`).join('');
  const chinaRows = Object.entries(chinaLeverage).sort((a,b)=>b[1]-a[1])
    .map(([g,v]) => `<div style="display:flex;justify-content:space-between;font-size:11px;margin:2px 0"><span style="color:#ef4444">${groupLabels[g]||g}</span><span>$${(v/1e9).toFixed(0)}B</span></div>`).join('');

  // Balance bar
  const westPct = Math.round(westVal / (westVal + chinaVal) * 100) || 50;

  document.getElementById('detail-content').innerHTML = `
    <h3>⚖️ Supply Chain Balance of Power</h3>
    <div style="font-size:20px;font-weight:800;color:${balanceColor};margin:8px 0">${balanceLabel}</div>

    <div style="display:flex;height:24px;border-radius:4px;overflow:hidden;margin:8px 0">
      <div style="width:${westPct}%;background:#3b82f6;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#fff">${westPct}%</div>
      <div style="width:${100-westPct}%;background:#ef4444;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#fff">${100-westPct}%</div>
    </div>
    <div style="display:flex;justify-content:space-between;font-size:10px;color:#888;margin-bottom:12px">
      <span>Western bloc $${(westVal/1e9).toFixed(0)}B</span>
      <span>China $${(chinaVal/1e9).toFixed(0)}B</span>
    </div>

    <div style="border-top:1px solid rgba(255,255,255,0.1);padding-top:8px">
      <div style="font-size:9px;color:#3b82f6;text-transform:uppercase;font-weight:600;margin-bottom:4px">🔵 Western Leverage → China (${westToChina.length} flows)</div>
      ${westRows}
    </div>

    <div style="border-top:1px solid rgba(255,255,255,0.1);padding-top:8px;margin-top:8px">
      <div style="font-size:9px;color:#ef4444;text-transform:uppercase;font-weight:600;margin-bottom:4px">🔴 China Leverage → West (${chinaToWest.length} flows)</div>
      ${chinaRows}
    </div>

    <div style="border-top:1px solid rgba(255,255,255,0.1);padding-top:8px;margin-top:8px;font-size:11px;color:#aaa">
      <strong>Analysis:</strong> Western bloc controls upstream technology nodes (semiconductors, EDA, lithography). China controls midstream processing monopolies (rare earths, battery materials, solar wafers, graphite). Neither side can unilaterally decouple without 5-10 year restructuring. This mutual dependency creates a supply chain version of MAD — Mutually Assured Disruption.
    </div>

    <div style="margin-top:6px;font-size:10px;color:#666">Blue = West→China leverage · Red = China→West leverage</div>`;
  document.getElementById('detail-panel').classList.remove('hidden');
}

// ============================================================
// COUNTRY VULNERABILITY
// ============================================================
function analyzeCountryVulnerability(code) {
  if (!state.countryData?.[code] || !state.tradeData) return;
  const info = state.countryData[code];
  const trades = state.tradeData.features;
  const allBases = state.basesData?.features || [];

  // Set active button
  document.querySelectorAll('.preset-btn:not(.country-btn)').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.country-btn').forEach(b => b.classList.toggle('active', b.dataset.country === code));

  // Imports & Exports
  const imports = trades.filter(t => t.properties.importer_code === code);
  const exports = trades.filter(t => t.properties.exporter_code === code);
  const critImports = imports.filter(t => ['critical','high'].includes(t.properties.vulnerability));
  const critExports = exports.filter(t => ['critical','high'].includes(t.properties.vulnerability));
  const importVal = critImports.reduce((s,t) => s + (t.properties.trade_value_usd||0), 0);
  const exportVal = critExports.reduce((s,t) => s + (t.properties.trade_value_usd||0), 0);

  // Chokepoint exposure — check BOTH strategic notes AND geographic routing
  const cpSet = new Set();
  const cpWords = ['hormuz','malacca','suez','bab el-mandeb','taiwan','panama','gibraltar','korea','lombok'];
  // Check all imports (not just critical) for chokepoint transit
  imports.forEach(t => {
    const note = (t.properties.strategic_note||'').toLowerCase();
    const commodity = (t.properties.commodity||'').toLowerCase();
    cpWords.forEach(cp => { if(note.includes(cp) || commodity.includes(cp)) cpSet.add(cp); });
    // Geographic detection: if route passes through chokepoint regions
    const coords = t.geometry?.coordinates || [];
    coords.forEach(c => {
      if (c[0]>55 && c[0]<58 && c[1]>25 && c[1]<28) cpSet.add('hormuz');
      if (c[0]>100 && c[0]<105 && c[1]>0 && c[1]<4) cpSet.add('malacca');
      if (c[0]>31 && c[0]<34 && c[1]>29 && c[1]<32) cpSet.add('suez');
      if (c[0]>42 && c[0]<44 && c[1]>12 && c[1]<14) cpSet.add('bab el-mandeb');
    });
  });

  // Vulnerability score (0-100, higher = more vulnerable)
  // Factor in destroyed/halted pipelines as additional vulnerability
  const critImpCount = imports.filter(t=>t.properties.vulnerability==='critical').length;
  const highImpCount = imports.filter(t=>t.properties.vulnerability==='high').length;
  const critExpCount = exports.filter(t=>t.properties.vulnerability==='critical').length;
  const highExpCount = exports.filter(t=>t.properties.vulnerability==='high').length;
  const destroyedCount = imports.filter(t=>['destroyed','halted'].includes(t.properties.pipeline_status)).length;
  // Energy import dependency adds vulnerability (2026 Hormuz crisis affects all energy importers)
  const energyImports = imports.filter(t=>['energy_oil','energy_gas'].includes(t.properties.commodity_group)).length;
  let score = (critImpCount * 10 + highImpCount * 5) + cpSet.size * 8 + destroyedCount * 12 + energyImports * 3 - (critExpCount * 6 + highExpCount * 3);
  score = Math.max(0, Math.min(100, score));

  // Position — derived from score, not trade value ratio
  let position = 'BALANCED';
  if (score >= 60) position = 'NET VULNERABLE';
  else if (score <= 20 && exportVal > importVal) position = 'NET LEVERAGE HOLDER';
  const posColor = {'NET LEVERAGE HOLDER':'#4ade80','NET VULNERABLE':'#ef4444','BALANCED':'#fbbf24'}[position];
  const scoreColor = score > 60 ? '#ef4444' : score > 35 ? '#f97316' : '#4ade80';

  // Breakdown by commodity group
  const breakdown = {};
  critImports.forEach(t => {
    const g = t.properties.commodity_group;
    breakdown[g] = (breakdown[g]||0) + (t.properties.trade_value_usd||0);
  });

  // Own bases
  const ownBases = allBases.filter(b => {
    const op = (b.properties.operator||'').toLowerCase();
    return op === info.name.toLowerCase() || op === code.toLowerCase();
  });

  // Top 3 most vulnerable flows
  const top3 = critImports.sort((a,b) => (b.properties.trade_value_usd||0)-(a.properties.trade_value_usd||0)).slice(0, 5);

  // Render on map: highlight all flows for this country
  state.analysisLayer.clearLayers();
  const allFlows = trades.filter(t => t.properties.importer_code===code || t.properties.exporter_code===code);

  // Show relevant layers — keep conflicts visible
  state.markers.addTo(map);
  applyFilters(); // re-render conflicts
  state.tradeLayer.addTo(map);
  renderTradeFlows([], new Set()); // clear default trade (analysis layer shows country-specific)
  state.basesLayer.addTo(map);
  state.maritimeLayer.addTo(map);

  allFlows.forEach(f => {
    const coords = f.geometry.coordinates;
    const isImport = f.properties.importer_code === code;
    const color = isImport ? '#ef4444' : '#4ade80';
    const pts = coords.map(c => [c[1], c[0]]);
    const line = L.polyline(pts, { color, weight:2.5, opacity:0.7 });
    state.analysisLayer.addLayer(line);
  });

  // Zoom to country
  if (info.lat && info.lon) map.setView([info.lat, info.lon], 4);

  // Build breakdown bars
  const maxVal = Math.max(...Object.values(breakdown), 1);
  const groupLabels = { semiconductors:'Semiconductors', energy_oil:'Oil', energy_gas:'Gas', minerals:'Minerals', rare_earths:'Rare Earths', food:'Food', manufacturing:'Manufacturing', defense:'Defense', new_energy:'New Energy', ai_compute:'AI Compute' };
  const breakdownHtml = Object.entries(breakdown).sort((a,b)=>b[1]-a[1]).map(([g,v]) => {
    const pct = (v/maxVal*100).toFixed(0);
    const gc = COMMODITY_COLORS[g]||'#666';
    return `<div class="vuln-breakdown-row">
      <span class="vuln-breakdown-label">${groupLabels[g]||g}</span>
      <span class="vuln-breakdown-value">$${(v/1e9).toFixed(0)}B</span>
    </div>
    <div class="vuln-bar"><div class="vuln-bar-fill" style="width:${pct}%;background:${gc}"></div></div>`;
  }).join('');

  const topFlowsHtml = top3.map(t => {
    const p = t.properties;
    const vc = {critical:'#ef4444',high:'#f97316'}[p.vulnerability]||'#fbbf24';
    return `<div style="font-size:11px;margin:3px 0;display:flex;justify-content:space-between">
      <span>${esc(p.exporter)}: ${esc(p.commodity).substring(0,25)}</span>
      <span style="color:${vc}">$${(p.trade_value_usd/1e9).toFixed(0)}B</span></div>`;
  }).join('');

  document.getElementById('detail-content').innerHTML = `
    <h3>${esc(info.name)} Supply Chain Risk</h3>
    <div class="vuln-score" style="color:${scoreColor}">${score}<span style="font-size:14px;color:#888">/100</span></div>
    <div style="font-size:18px;font-weight:700;color:${posColor};margin-bottom:10px">${position}</div>

    <div class="detail-row"><span class="key">Import risk</span><span class="val" style="color:#ef4444">${critImports.length} flows · $${(importVal/1e9).toFixed(0)}B</span></div>
    <div class="detail-row"><span class="key">Export leverage</span><span class="val" style="color:#4ade80">${critExports.length} flows · $${(exportVal/1e9).toFixed(0)}B</span></div>
    <div class="detail-row"><span class="key">Chokepoints</span><span class="val">${cpSet.size>0?[...cpSet].join(', '):'low exposure'}</span></div>
    <div class="detail-row"><span class="key">Bases</span><span class="val">${ownBases.length} installations</span></div>

    ${breakdownHtml?`<div style="margin-top:10px;border-top:1px solid rgba(255,255,255,0.1);padding-top:8px">
      <div style="font-size:9px;color:#888;text-transform:uppercase;margin-bottom:4px">Import Risk by Sector</div>
      ${breakdownHtml}
    </div>`:''}

    ${topFlowsHtml?`<div style="margin-top:8px;border-top:1px solid rgba(255,255,255,0.1);padding-top:6px">
      <div style="font-size:9px;color:#888;text-transform:uppercase;margin-bottom:4px">Most Vulnerable Flows</div>
      ${topFlowsHtml}
    </div>`:''}

    <div style="margin-top:8px;font-size:10px;color:#666">Red = import dependencies · Green = export leverage</div>
    <a href="/reports/${code}.html" target="_blank" style="display:block;margin-top:10px;padding:8px;background:rgba(42,157,143,0.15);border:1px solid #2a9d8f;border-radius:5px;color:#2a9d8f;text-align:center;text-decoration:none;font-size:12px;font-weight:600">📄 Full Vulnerability Report →</a>`;
  document.getElementById('detail-panel').classList.remove('hidden');
}

// ============================================================
// CHOKEPOINT ANALYSIS (reused from v1)
// ============================================================
function analyzeChokepoint(name) {
  if (!state.tradeData) return;
  const trades = state.tradeData.features;
  const cpLower = name.toLowerCase().replace('strait of ','').replace(' strait','');
  const affected = trades.filter(t => (t.properties.strategic_note||'').toLowerCase().includes(cpLower));
  const totalVal = affected.reduce((s,t) => s + (t.properties.trade_value_usd||0), 0);
  const countries = new Set();
  affected.forEach(t => { countries.add(t.properties.importer); countries.add(t.properties.exporter); });

  state.analysisLayer.clearLayers();
  affected.forEach(f => {
    const coords = f.geometry.coordinates;
    const vuln = f.properties.vulnerability;
    const color = vuln==='critical'?'#ef4444':vuln==='high'?'#f97316':'#fbbf24';
    const pts = coords.map(c => [c[1], c[0]]);
    state.analysisLayer.addLayer(L.polyline(pts, { color, weight:3, opacity:0.85, dashArray:vuln==='critical'?'8 4':null }));
  });

  const flowList = affected.map(t => {
    const p = t.properties;
    const vc = {critical:'#ef4444',high:'#f97316',medium:'#fbbf24'}[p.vulnerability]||'#888';
    return `<div style="font-size:11px;margin:2px 0;display:flex;justify-content:space-between">
      <span>${esc(p.exporter)}→${esc(p.importer)}</span>
      <span><span style="color:${vc}">${esc(p.commodity).substring(0,20)}</span> $${(p.trade_value_usd/1e9).toFixed(0)}B</span></div>`;
  }).join('');

  document.getElementById('detail-content').innerHTML = `
    <h3>⚓ ${esc(name)} Impact</h3>
    <div style="font-size:24px;font-weight:800;color:#ef4444;margin:6px 0">$${(totalVal/1e9).toFixed(0)}B at risk</div>
    <div class="detail-row"><span class="key">Affected</span><span class="val">${affected.length} flows</span></div>
    <div class="detail-row"><span class="key">Countries</span><span class="val">${countries.size} (${[...countries].slice(0,5).join(', ')}${countries.size>5?'...':''})</span></div>
    <div style="margin-top:8px;border-top:1px solid rgba(255,255,255,0.1);padding-top:6px">
      <div style="font-size:9px;color:#888;text-transform:uppercase;margin-bottom:4px">Trade Flows at Risk</div>
      ${flowList}
    </div>`;
  document.getElementById('detail-panel').classList.remove('hidden');
}

// ============================================================
// UI CONTROLS
// ============================================================
function buildControls() {
  // Source toggles
  const srcEl = document.getElementById('source-toggles');
  srcEl.innerHTML = '';
  state.sources.forEach(src => {
    const color = SOURCE_COLORS[src]||SOURCE_COLORS.default;
    const lbl = document.createElement('label');
    lbl.innerHTML = `<input type="checkbox" checked data-source="${src}"><span class="color-dot" style="background:${color}"></span> ${src.toUpperCase()}`;
    lbl.querySelector('input').addEventListener('change', e => { state.sourceVisibility[src]=e.target.checked; applyFilters(); });
    srcEl.appendChild(lbl);
  });

  // Type toggles
  const typeEl = document.getElementById('type-toggles');
  typeEl.innerHTML = '';
  const counts = {};
  state.data.features.forEach(f => { const t=f.properties?.event_type; if(t) counts[t]=(counts[t]||0)+1; });
  Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,8).forEach(([type,count]) => {
    const lbl = document.createElement('label');
    lbl.innerHTML = `<input type="checkbox" checked data-type="${type}"><span class="color-dot" style="background:#f4a261"></span> ${type} <span style="opacity:0.5">(${count})</span>`;
    lbl.querySelector('input').addEventListener('change', e => { state.typeVisibility[type]=e.target.checked; applyFilters(); });
    typeEl.appendChild(lbl);
  });

  // Date controls
  document.getElementById('date-from').addEventListener('change', e => { state.dateFrom=e.target.value; applyFilters(); });
  document.getElementById('date-to').addEventListener('change', e => { state.dateTo=e.target.value; applyFilters(); });

  // Collapse headers
  document.querySelectorAll('.collapse-header').forEach(h => {
    h.addEventListener('click', () => {
      const target = document.getElementById(h.dataset.target);
      if (target) {
        target.classList.toggle('hidden');
        h.textContent = h.textContent.replace(/[▸▾]/, target.classList.contains('hidden') ? '▸' : '▾');
      }
    });
  });
}

function setupPresetButtons() {
  // Preset buttons
  document.querySelectorAll('.preset-btn:not(.country-btn)').forEach(btn => {
    btn.addEventListener('click', () => applyPreset(btn.dataset.preset));
  });

  // Country buttons
  document.querySelectorAll('.country-btn').forEach(btn => {
    btn.addEventListener('click', () => analyzeCountryVulnerability(btn.dataset.country));
  });
}

function updateMeta() {
  const el = document.getElementById('last-updated');
  const meta = state.data?.metadata;
  if (meta?.merged_at) {
    el.textContent = `${new Date(meta.merged_at).toLocaleString()} · ${meta.total_features||0} events · ${state.tradeData?.features?.length||0} flows`;
  } else {
    el.textContent = `${state.data?.features?.length||0} events · ${state.tradeData?.features?.length||0} flows`;
  }
}

// ============================================================
// UTILS
// ============================================================
function esc(s) { return s ? String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;') : ''; }
function closeDetail() { document.getElementById('detail-panel').classList.add('hidden'); state.analysisLayer.clearLayers(); }

// ============================================================
// INIT
// ============================================================
async function init() {
  await Promise.all([loadConflicts(), loadMaritime(), loadTrade(), loadBases(), loadCountryCentroids()]);

  // Render maritime (always visible in most presets)
  renderMaritime(state.maritimeData?.features || []);

  // Render bases
  renderBases(state.basesData?.features || []);

  // Build UI controls
  buildControls();
  setupPresetButtons();
  updateMeta();

  // Apply default preset
  applyPreset('overview');
}

init();
